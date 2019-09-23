module Snappy
  private module Decompressor
    extend self

    protected def uncompressed_length(compressed : Slice)
      read_uncompressed_length(compressed)[0]
    end

    protected def uncompress(compressed : Slice) : Slice
      # Read the uncompressed length from the front of the compressed input
      expected_len, read = read_uncompressed_length(compressed)
      uncompressed = Bytes.new(expected_len)

      # Process the entire input
      uncompressed_size = decompress_all_tags(
        compressed[read..],
        uncompressed
      )
      if !(expected_len == uncompressed_size)
        raise SnappyError.new("Recorded length is #{expected_len} bytes but actual length after decompression is #{uncompressed_size}")
      end
      uncompressed
    end

    protected def uncompress(compressed : Slice, uncompressed : Slice)
      # Read the uncompressed length from the front of the compressed input
      expected_len, read = read_uncompressed_length(compressed)

      raise SnappyError.new("Uncompressed size #{expected_len} must be less than #{uncompressed.size}") unless expected_len <= uncompressed.size

      # Process the entire input
      uncompressed_size = decompress_all_tags(
        compressed[read..],
        uncompressed
      )
      if !(expected_len == uncompressed_size)
        raise SnappyError.new("Recorded length is #{expected_len} bytes but actual length after decompression is #{uncompressed_size}")
      end
      expected_len
    end

    private def decompress_all_tags(input : Slice, output : Slice)
      outpu_limit = output.size
      ip_limit = input.size
      op_index = 0
      ip_index = 0

      while ip_index < ip_limit - 5
        opcode = Utils.load_byte(input[ip_index..])
        ip_index += 1
        entry = (OP_LOOKUP_TABLE[opcode] & 0xFFFF).to_i
        trailer_bytes = entry.to_u32 >> 11
        trailer = read_trailer(input[ip_index..], trailer_bytes)

        # advance the ip_index past the op codes
        ip_index += entry.to_u32 >> 11
        length = entry & 0xff
        if (opcode & 0x3) == Snappy::ElemType::Literal.value
          literal_len = length + trailer
          copy_literal(input[ip_index..], output[op_index..], literal_len)
          ip_index += literal_len
          op_index += literal_len
        else
          # copy_offset/256 is encoded in bits 8..10. By just fetching
          # those bits, we get copy_offset (since the fit-field starts at bit 8).
          copy_offset = entry & 0x700
          copy_offset += trailer
          # Equivalent to incremental_copy (below) except that it can write up to ten extra
          # bytes after the end of the copy, and that it is faster.
          #
          # The main part of this loop is a simple copy of eight bytes at a time until
          # we've copied (at least) the requested amount of bytes.  However, if op and
          # src are less than eight bytes apart (indicating a repeating pattern of
          # length < 8), we first need to expand the pattern in order to get the correct
          # results. For instance, if the buffer looks like this, with the eight-byte
          # <src> and <op> patterns marked as intervals:
          #
          #    abxxxxxxxxxxxx
          #    [------]           src
          #      [------]         op
          #
          # a single eight-byte copy from <src> to <op> will repeat the pattern once,
          # after which we can move <op> two bytes without moving <src>:
          #
          #    ababxxxxxxxxxx
          #    [------]           src
          #        [------]       op
          #
          # and repeat the exercise until the two no longer overlap.
          #
          # This allows us to do very well in the special case of one single byte
          # repeated many times, without taking a big hit for more general cases.
          #
          # The worst case of extra writing past the end of the match occurs when
          # op - src == 1 and len == 1; the last copy will read from byte positions
          # [0..7] and write to [4..11], whereas it was only supposed to write to
          # position 1. Thus, ten excess bytes.
          space_left = outpu_limit - op_index
          src_index = op_index - copy_offset

          raise SnappyError.new("Invalid copy offset for opcode starting at #{ip_index - trailer_bytes - 1}") if src_index < 0

          if length <= 16 && copy_offset >= 8 && space_left >= 16
            # fast path, used for the majority (70-80%) of dynamic invocations.
            Utils.copy_long(output[src_index..], output[op_index..])
            Utils.copy_long(output[src_index + 8..], output[op_index + 8..])
          elsif space_left >= length + MAX_INCREMENT_COPY_OVERFLOW
            incremental_copy_fastpath(output, src_index, op_index, length)
          else
            incremental_copy(output, src_index, output, op_index, length)
          end
          op_index += length
        end
      end

      while ip_index < ip_limit
        ip, op_index = decompress_tag_slow(input[ip_index..], output, outpu_limit, op_index)
        ip_index += ip
      end
      op_index
    end

    # This is a second copy of the inner loop of decompress_tags used when near the end
    # of the input. The key difference is the reading of the trailer bytes.  The fast
    # code does a blind read of the next 4 bytes as an int, and this code assembles
    # the int byte-by-byte to assure that the array is not over run.  The reason this
    # code path is separate is the if condition to choose between these two seemingly
    # small differences costs like 10-20% of the throughput.
    private def decompress_tag_slow(input : Slice, output : Slice, outpu_limit, op_index)
      # read the op code
      opcode = Utils.load_byte(input)
      ip_index = 1
      entry = (OP_LOOKUP_TABLE[opcode] & 0xFFFF).to_i
      trailer_bytes = entry.to_u32 >> 11

      # Key difference here
      trailer = get_trailer(input[ip_index..], trailer_bytes)

      # advance the ip_index past the op codes
      ip_index += trailer_bytes
      length = entry & 0xff

      if (opcode & 0x3) == Snappy::ElemType::Literal.value
        literal_len = length + trailer
        copy_literal(input[ip_index..], output[op_index..], literal_len)
        ip_index += literal_len
        op_index += literal_len
      else
        # copy_offset/256 is encoded in bits 8..10. By just fetching
        # those bits, we get copy_offset (since the fit-field starts at bit 8).
        copy_offset = entry & 0x700
        copy_offset += trailer

        space_left = outpu_limit - op_index
        src_index = op_index - copy_offset

        raise SnappyError.new("Invalid copy offset for opcode starting at #{ip_index - trailer_bytes - 1}") if src_index < 0

        if length <= 16 && copy_offset >= 8 && space_left >= 16
          # fast path, used for the majority (70-80%) of dynamic invocations.
          Utils.copy_long(output[src_index..], output[op_index..])
          Utils.copy_long(output[src_index + 8..], output[op_index + 8..])
        elsif space_left >= length + MAX_INCREMENT_COPY_OVERFLOW
          incremental_copy_fastpath(output, src_index, op_index, length)
        else
          incremental_copy(output, src_index, output, op_index, length)
        end
        op_index += length
      end
      {ip_index, op_index}
    end

    private def get_trailer(input, bytes)
      trailer = 0
      case bytes
      when 4
        trailer = (input[3].to_i & 0xff) << 24
        trailer |= (input[2].to_i & 0xff) << 16
        trailer |= (input[1].to_i & 0xff) << 8
        trailer |= input[0].to_i & 0xff
      when 3
        trailer = (input[2].to_i & 0xff) << 16
        trailer |= (input[1].to_i & 0xff) << 8
        trailer |= input[0].to_i & 0xff
      when 2
        trailer = (input[1].to_i & 0xff) << 8
        trailer |= input[0].to_i & 0xff
      when 1
        trailer = input[0].to_i & 0xff
      end
      trailer
    end

    private def read_trailer(data : Slice, bytes)
      Utils.load_int(data) & WORD_MASK[bytes]
    end

    private def copy_literal(input : Slice, output : Slice, length)
      raise SnappyError.new("Corrupted input") unless length > 0

      space_left = output.size
      readable_bytes = input.size

      raise SnappyError.new("Corrupt literal length") if (readable_bytes < length) || (space_left < length)

      if length <= 16 && space_left >= 16 && readable_bytes >= 16
        Utils.copy_long(input, output)
        Utils.copy_long(input[8..], output[8..])
      else
        fast_len = length & 0xFFFFFFF8
        if fast_len <= 64
          # copy long-by-long
          i = 0
          while i < fast_len
            Utils.copy_long(input[i..], output[i..])
            i += 8
          end

          # copy byte-by-byte
          slow_len = length & 0x7
          # NOTE: This is not a manual array copy.  We are copying an overlapping region
          # and we want input data to repeat as it is recopied. see incremental_copy below.
          0.upto(slow_len - 1) do |j|
            output[fast_len + j] = input[fast_len + j]
          end
        else
          output.copy_from(input.to_unsafe, length)
        end
      end
    end

    # Copy "len" bytes from "src" to "op", one byte at a time.  Used for
    # handling COPY operations where the input and output regions may
    # overlap.  For example, suppose:
    # src    == "ab"
    # op     == src + 2
    # len    == 20
    # <p/>
    # After incrementalCopy, the result will have
    # eleven copies of "ab"
    # ababababababababababab
    # Note that this does not match the semantics of either memcpy() or memmove().
    private def incremental_copy(src : Slice, src_index : Int32, op : Slice, op_index : Int32, length : Int32)
      while length > 0
        op[op_index] = src[src_index]
        op_index += 1
        src_index += 1
        length -= 1
      end
    end

    private def incremental_copy_fastpath(output : Slice, src_index, op_index, length)
      copied_len = 0
      while (op_index + copied_len) - src_index < 8
        Utils.copy_long(output[src_index..], output[op_index + copied_len..])
        copied_len += (op_index + copied_len) - src_index
      end
      i = 0
      while i < (length - copied_len)
        Utils.copy_long(output[src_index + i..], output[op_index + copied_len + i..])
        i += 8
      end
    end

    # Reads the variable length integer encoded a the specified offset, and
    # returns this length with the number of bytes read.
    private def read_uncompressed_length(compressed : Slice)
      v, n = read_uint(compressed)
      raise SnappyError.new "corrupt snappy input" if n <= 0 || v > 0xffffffff
      t = (0_u32 >> 32 & 1)
      t ^= t
      word_size = 32 << t
      raise SnappyError.new "decoded block is too large" if word_size == 32 && v > 0x7fffffff
      {v.to_i, n}
    end

    private def read_uint(buf : Slice)
      x = 0_u64
      s = 0_u32

      buf.each_with_index do |b, i|
        if b < 0x80
          raise SnappyError.new "overflow in reading snappy buffer" if i > 9 || i == 9 && b > 1
          return {(x | b.to_u64 << s).to_u64, i + 1}
        end
        x |= (b & 0x7f).to_u64 << s
        s += 7
      end
      {0_u64, 0}
    end

    private MAX_INCREMENT_COPY_OVERFLOW = 20

    # Mapping from i in range [0,4] to a mask to extract the bottom 8*i bits
    private WORD_MASK = StaticArray[0, 0xff, 0xffff, 0xffffff, 0xffffffff]

    # Data stored per entry in lookup table:
    #      Range   Bits-used       Description
    #      ------------------------------------
    #      1..64   0..7            Literal/copy length encoded in opcode byte
    #      0..7    8..10           Copy offset encoded in opcode byte / 256
    #      0..4    11..13          Extra bytes after opcode
    #
    # We use eight bits for the length even though 7 would have sufficed
    # because of efficiency reasons:
    #      (1) Extracting a byte is faster than a bit-field
    #      (2) It properly aligns copy offset so we do not need a <<8
    private OP_LOOKUP_TABLE = StaticArray[
      0x0001, 0x0804, 0x1001, 0x2001, 0x0002, 0x0805, 0x1002, 0x2002,
      0x0003, 0x0806, 0x1003, 0x2003, 0x0004, 0x0807, 0x1004, 0x2004,
      0x0005, 0x0808, 0x1005, 0x2005, 0x0006, 0x0809, 0x1006, 0x2006,
      0x0007, 0x080a, 0x1007, 0x2007, 0x0008, 0x080b, 0x1008, 0x2008,
      0x0009, 0x0904, 0x1009, 0x2009, 0x000a, 0x0905, 0x100a, 0x200a,
      0x000b, 0x0906, 0x100b, 0x200b, 0x000c, 0x0907, 0x100c, 0x200c,
      0x000d, 0x0908, 0x100d, 0x200d, 0x000e, 0x0909, 0x100e, 0x200e,
      0x000f, 0x090a, 0x100f, 0x200f, 0x0010, 0x090b, 0x1010, 0x2010,
      0x0011, 0x0a04, 0x1011, 0x2011, 0x0012, 0x0a05, 0x1012, 0x2012,
      0x0013, 0x0a06, 0x1013, 0x2013, 0x0014, 0x0a07, 0x1014, 0x2014,
      0x0015, 0x0a08, 0x1015, 0x2015, 0x0016, 0x0a09, 0x1016, 0x2016,
      0x0017, 0x0a0a, 0x1017, 0x2017, 0x0018, 0x0a0b, 0x1018, 0x2018,
      0x0019, 0x0b04, 0x1019, 0x2019, 0x001a, 0x0b05, 0x101a, 0x201a,
      0x001b, 0x0b06, 0x101b, 0x201b, 0x001c, 0x0b07, 0x101c, 0x201c,
      0x001d, 0x0b08, 0x101d, 0x201d, 0x001e, 0x0b09, 0x101e, 0x201e,
      0x001f, 0x0b0a, 0x101f, 0x201f, 0x0020, 0x0b0b, 0x1020, 0x2020,
      0x0021, 0x0c04, 0x1021, 0x2021, 0x0022, 0x0c05, 0x1022, 0x2022,
      0x0023, 0x0c06, 0x1023, 0x2023, 0x0024, 0x0c07, 0x1024, 0x2024,
      0x0025, 0x0c08, 0x1025, 0x2025, 0x0026, 0x0c09, 0x1026, 0x2026,
      0x0027, 0x0c0a, 0x1027, 0x2027, 0x0028, 0x0c0b, 0x1028, 0x2028,
      0x0029, 0x0d04, 0x1029, 0x2029, 0x002a, 0x0d05, 0x102a, 0x202a,
      0x002b, 0x0d06, 0x102b, 0x202b, 0x002c, 0x0d07, 0x102c, 0x202c,
      0x002d, 0x0d08, 0x102d, 0x202d, 0x002e, 0x0d09, 0x102e, 0x202e,
      0x002f, 0x0d0a, 0x102f, 0x202f, 0x0030, 0x0d0b, 0x1030, 0x2030,
      0x0031, 0x0e04, 0x1031, 0x2031, 0x0032, 0x0e05, 0x1032, 0x2032,
      0x0033, 0x0e06, 0x1033, 0x2033, 0x0034, 0x0e07, 0x1034, 0x2034,
      0x0035, 0x0e08, 0x1035, 0x2035, 0x0036, 0x0e09, 0x1036, 0x2036,
      0x0037, 0x0e0a, 0x1037, 0x2037, 0x0038, 0x0e0b, 0x1038, 0x2038,
      0x0039, 0x0f04, 0x1039, 0x2039, 0x003a, 0x0f05, 0x103a, 0x203a,
      0x003b, 0x0f06, 0x103b, 0x203b, 0x003c, 0x0f07, 0x103c, 0x203c,
      0x0801, 0x0f08, 0x103d, 0x203d, 0x1001, 0x0f09, 0x103e, 0x203e,
      0x1801, 0x0f0a, 0x103f, 0x203f, 0x2001, 0x0f0b, 0x1040, 0x2040,
    ]
  end
end
