module Snappy
  private module Compressor
    extend self
    # *** DO NOT CHANGE THE VALUE OF BLOCK_SIZE ***
    #
    # New Compression code chops up the input into blocks of at most
    # the following size.  This ensures that back-references in the
    # output never cross BLOCK_SIZE block boundaries.  This can be
    # helpful in implementing blocked decompression.  However the
    # decompression code should not rely on this guarantee since older
    # compression code may not obey it.
    private BLOCK_LOG  = 15
    private BLOCK_SIZE = 1 << BLOCK_LOG

    private INPUT_MARGIN_BYTES = 15

    private MAX_HASH_TABLE_BITS = 14
    private MAX_HASH_TABLE_SIZE = 1 << MAX_HASH_TABLE_BITS

    protected def max_compressed_length(source_len : Int32)
      # Compressed data can be defined as:
      #    compressed := item* literal*
      #    item       := literal* copy
      #
      # The trailing literal sequence has a space blowup of at most 62/60
      # since a literal of length 60 needs one tag byte + one extra byte
      # for length information.
      #
      # Item blowup is trickier to measure.  Suppose the "copy" op copies
      # 4 bytes of data.  Because of a special check in the encoding code,
      # we produce a 4-byte copy only if the offset is < 65536.  Therefore
      # the copy op takes 3 bytes to encode, and this type of item leads
      # to at most the 62/60 blowup for representing literals.
      #
      # Suppose the "copy" op copies 5 bytes of data.  If the offset is big
      # enough, it will take 5 bytes to encode the copy op.  Therefore the
      # worst case here is a one-byte literal followed by a five-byte copy.
      # I.e., 6 bytes of input turn into 7 bytes of "compressed" data.
      #
      # This last factor dominates the blowup, so the final estimate is:
      32 + source_len + source_len / 6
    end

    private def compress(src : Slice, dst : Slice)
      compressed_out = Bytes.new(max_compressed_length(src.size))
      compressed_size = compress(src, 0, src.size, compressed_out, 0)
      dst[0..].copy_from(compressed_out[0...compressed_size].to_unsafe, compressed_size)
    end

    protected def compress(uncompressed : Slice, uncompressed_len : Int32, compressed : Slice, compressed_offset : Int32)
      # First write the uncompressed size to the output as a variable length int
      compressed_index = write_uncompressed_length(compressed[compressed_offset, uncompressed_len])

      hash_table_size = get_hash_table_size(uncompressed_len)
      table = Array(UInt16).new(hash_table_size, 0_u16)
      read = 0
      while read < uncompressed_len
        # get encoding table for compression
        table.fill(0_u16)
        compressed_index += compress_fragment(uncompressed[read..],
          Math.min(uncompressed_len - read, BLOCK_SIZE),
          compressed[compressed_index..], table)
        read += BLOCK_SIZE
      end

      table.clear
      compressed_index - compressed_offset
    end

    private def compress_fragment(input : Slice, input_size : Int32, output : Slice, table : Array(UInt16))
      input_offset = 0
      output_index = 0
      ip_index = input_offset
      raise "input_size(#{input_size}) > BLOCK_SIZE" unless input_size <= BLOCK_SIZE
      ip_end_index = input_offset + input_size

      hash_table_size = get_hash_table_size(input_size)
      shift = 32 - log2_floor(hash_table_size)
      raise "table must be a power of two" unless (hash_table_size & (hash_table_size - 1)) == 0
      raise "invalid table size calculated" unless (0xFFFFFFFF >> shift.to_u32) == hash_table_size - 1

      # bytes in [next_emit_index, ip_index) will be emitted as literal bytes. Or
      # [next_emit_index, ip_end_index) after the main loop.
      next_emit_index = ip_index

      if input_size >= INPUT_MARGIN_BYTES
        ip_limit = input_offset + input_size - INPUT_MARGIN_BYTES
        while ip_index <= ip_limit
          raise "next_emit_index > ip_index" unless next_emit_index >= ip_index

          # The body of this loop calls emit_literal once and then emit_copy one or
          # more times.  (The exception is that when we're close to exhausting
          # the input we exit and emit a literal.)
          #
          # In the first iteration of this loop we're just starting, so
          # there's nothing to copy, so calling emit_literal once is
          # necessary.  And we only start a new iteration when the
          # current iteration has determined that a call to emit_literal will
          # precede the next call to emit_copy (if any).
          #
          # Step 1: Scan forward in the input looking for a 4-byte-long match.
          # If we get close to exhausting the input exit and emit a final literal.
          #
          # Heuristic match skipping: If 32 bytes are scanned with no matches
          # found, start looking only at every other byte. If 32 more bytes are
          # scanned, look at every third byte, etc.. When a match is found,
          # immediately go back to looking at every byte. This is a small loss
          # (~5% performance, ~0.1% density) for compressible data due to more
          # bookkeeping, but for non-compressible data (such as JPEG) it's a huge
          # win since the compressor quickly "realizes" the data is incompressible
          # and doesn't bother looking for matches everywhere.
          #
          # The "skip" variable keeps track of how many bytes there are since the
          # last match; dividing it by 32 (ie. right-shifting by five) gives the
          # number of bytes to move ahead for each iteration.

          skip = 32

          candidate_result = find_candidate(input, ip_index, ip_limit, input_offset, shift, table, skip)
          ip_index = candidate_result[0]
          candidate_index = candidate_result[1]
          skip = candidate_result[2]
          break if (ip_index + bytes_between_hash_lookups(skip) > ip_limit)

          # Step 2: A 4-byte match has been found.  We'll later see if more
          # than 4 bytes match.  But, prior to the match, input
          # bytes [next_emit, ip) are unmatched.  Emit them as "literal bytes."
          raise "bytes [next_emit, ip) are unmatched" unless next_emit_index + 16 <= ip_end_index

          output_index += emit_literal(output[output_index..], input[next_emit_index..], ip_index - next_emit_index, true)

          # Step 3: Call emit_copies, and then see if another emit_copies could
          # be our next move.  Repeat until we find no match for the
          # input immediately after what was consumed by the last emit_copies call.
          #
          # If we exit this loop normally then we need to call emit_literal next,
          # though we don't yet know how big the literal will be.  We handle that
          # by proceeding to the next iteration of the main loop.  We also can exit
          # this loop via goto if we get close to exhausting the input.

          indexes = emit_copies(input, input_offset, input_size, ip_index, output, output_index, table, shift, candidate_index)
          ip_index = indexes[0]
          output_index = indexes[1]
          next_emit_index = ip_index
        end
      end

      # goto emit_remainder hack
      if (next_emit_index < ip_end_index)
        # emit the remaining bytes as a literal
        output_index += emit_literal(output[output_index..], input[next_emit_index..], ip_end_index - next_emit_index)
      end

      output_index
    end

    private def find_candidate(input : Slice, ip_index : Int32, ip_limit : Int32, input_offset : Int32,
                               shift : Int32, table : Array(UInt16), skip : Int32)
      candidate_index = 0
      ip_index += 1
      while (ip_index + bytes_between_hash_lookups(skip)) <= ip_limit
        # hash the 4 bytes starting at the input pointer
        current_int = Utils.load_int(input[ip_index..])
        hash = hash_bytes(current_int, shift)
        # get the position of a 4 bytes sequence with the same hash
        candidate_index = input_offset + table[hash]
        raise "hash not found" unless candidate_index >= 0 || candidate_index < ip_index

        # update the hash to point to the current position
        table[hash] = (ip_index - input_offset).to_u16

        # if the 4 byte sequence a the candidate index matches the sequence at the
        # current position, proceed to the next phase
        break if current_int == Utils.load_int(input[candidate_index..])

        ip_index += bytes_between_hash_lookups(skip)
        skip += 1
      end
      {ip_index, candidate_index, skip}
    end

    private def bytes_between_hash_lookups(skip : Int32)
      skip.to_u32 >> 5
    end

    private def emit_copies(input : Slice,
                            input_offset : Int32,
                            input_size : Int32,
                            ip_index : Int32,
                            output : Slice,
                            output_index : Int32,
                            table : Array(UInt16),
                            shift : Int32,
                            candidate_index : Int32)
      # Step 3: Call emit_copy, and then see if another emit_copy could
      # be our next move.  Repeat until we find no match for the
      # input immediately after what was consumed by the last emit_copy call.
      #
      # If we exit this loop normally then we need to call emit_literal next,
      # though we don't yet know how big the literal will be.  We handle that
      # by proceeding to the next iteration of the main loop.  We also can exit
      # this loop via goto if we get close to exhausting the input.

      input_bytes = 0
      loop do
        # We have a 4-byte match at ip, and no need to emity any
        # "literal bytes" prior to ip.
        matched = 4 + find_match_length(input[candidate_index + 4..], input[ip_index + 4...], input_offset + input_size - (ip_index + 4))

        offset = ip_index - candidate_index
        raise "does not match" unless Utils.equals(input, ip_index, input, candidate_index, matched)
        ip_index += matched

        # emit the copy operation for this chunk
        output_index += emit_copy(output[output_index..], offset, matched)

        # are we done?
        return {ip_index, output_index} if (ip_index >= input_offset + input_size - INPUT_MARGIN_BYTES)

        # we could immediately start working at ip now, but to improve compression we first
        # update table[hash_bytes(ip -1, ...)].
        foo = Utils.load_int64(input[ip_index - 1..])
        prev_int = foo.to_i
        input_bytes = (foo >> 8).to_i

        # add hash starting with previous byte
        prev_hash = hash_bytes(prev_int, shift)
        table[prev_hash] = (ip_index - input_offset - 1).to_u16

        # update hash of the current byte
        curr_hash = hash_bytes(input_bytes, shift)

        candidate_index = input_offset + table[curr_hash]
        table[curr_hash] = (ip_index - input_offset).to_u16
        break unless input_bytes == Utils.load_int(input[candidate_index..])
      end
      {ip_index, output_index}
    end

    private def emit_literal(output : Slice, literal : Slice, length : Int32, allow_fast_path = false)
      output_index = 0
      literal_index = 0
      n = length - 1 # Zero-length literals are disallowed
      if n < 60
        # size fits in a tag byte
        output[output_index] = (Snappy::ElemType::Literal.value | (n << 2)).to_u8
        output_index += 1
        # The vast majority of copies are below 16 bytes, for which a
        # call to memcpy is overkill. This fast path can sometimes
        # copy up to 15 bytes too much, but that is okay in the
        # main loop, since we have a bit to go on for both sides:
        #
        #   - The input will always have kInputMarginBytes = 15 extra
        #     available bytes, as long as we're in the main loop, and
        #     if not, allow_fast_path = false.
        #   - The output will always have 32 spare bytes (see
        #     max_compressed_length).
        if allow_fast_path && length <= 16
          Utils.copy_long(literal[literal_index..], output[output_index..])
          Utils.copy_long(literal[literal_index + 8..], output[output_index + 8..])
          output_index += length
          return output_index
        end
      elsif n < (1 << 8)
        output[output_index] = (Snappy::ElemType::Literal.value | (59 + 1 << 2)).to_u8
        output_index += 1
        output[output_index] = n.to_u8
        output_index += 1
      elsif n < (1 << 16)
        output[output_index] = (Snappy::ElemType::Literal.value | (59 + 2 << 2)).to_u8
        output_index += 1
        output[output_index] = n.to_u8
        output_index += 1
        output[output_index] = (n.to_u32 >> 8).to_u8
        output_index += 1
      elsif n < (1 << 24)
        output[output_index] = (Snappy::ElemType::Literal.value | (59 + 3 << 2)).to_u8
        output_index += 1
        output[output_index] = n.to_u8
        output_index += 1
        output[output_index] = (n.to_u32 >> 8).to_u8
        output_index += 1
        output[output_index] = (n.to_u32 >> 16).to_u8
        output_index += 1
      else
        output[output_index] = (Snappy::ElemType::Literal.value | (59 + 4 << 2)).to_u8
        output_index += 1
        output[output_index] = n.to_u8
        output_index += 1
        output[output_index] = (n.to_u32 >> 8).to_u8
        output_index += 1
        output[output_index] = (n.to_u32 >> 16).to_u8
        output_index += 1
        output[output_index] = (n.to_u32 >> 24).to_u8
        output_index += 1
      end
      Utils.check_position_indexes(literal_index, literal_index + length, literal.size)
      output[output_index..].copy_from(literal[literal_index..].to_unsafe, length)
      output_index += length
      output_index
    end

    private def emit_copy_less_than_64(output : Slice, offset : Int32, length : Int32)
      raise ArgumentError.new("Invalid argument") if offset <= 0 || length > 64 || length < 4 || offset > 65536
      output_index = 0
      if (length < 12) && (offset < 2048)
        len_min_4 = length - 4
        raise "must fit in 3 bits" unless len_min_4 < 8
        output[output_index] = (Snappy::ElemType::Copy1.value | ((len_min_4 << 2) | ((offset.to_u32 >> 8) << 5))).to_u8
        output_index += 1
        output[output_index] = offset.to_u8
        output_index += 1
      else
        output[output_index] = (Snappy::ElemType::Copy2.value | ((length - 1) << 2)).to_u8
        output_index += 1
        output[output_index] = offset.to_u8
        output_index += 1
        output[output_index] = (offset.to_u32 >> 8).to_u8
        output_index += 1
      end
      output_index
    end

    private def emit_copy(output : Slice, offset : Int32, length : Int32)
      # Emit 64 byte copies but make sure to keep at least four bytes reserved
      output_index = 0
      while length >= 68
        output_index += emit_copy_less_than_64(output[output_index..], offset, 64)
        length -= 64
      end

      # Emit an extra 60 byte copy if have too much data to fit in one copy
      if length > 64
        output_index += emit_copy_less_than_64(output[output_index..], offset, 60)
        length -= 60
      end

      # Emit remainder
      output_index += emit_copy_less_than_64(output[output_index..], offset, length)
      output_index
    end

    private def find_match_length(s1 : Slice, s2 : Slice, s2_limit : Int32)
      matched = 0
      while matched < s2_limit
        return matched if s1[matched] != s2[matched]
        matched += 1
      end
      s2_limit
    end

    private def get_hash_table_size(input_size : Int32)
      # Use smaller hash table when input_size is smaller, since we
      # fill the table, incurring O(hash table size) overhead for
      # compression, and if the input is short, we won't need that
      # many hash table entries anyway.

      hash_table_size = 256
      while hash_table_size < MAX_HASH_TABLE_SIZE && hash_table_size < input_size
        hash_table_size <<= 1
      end
      raise "hash must be power of two" unless (hash_table_size & (hash_table_size - 1)) == 0
      raise "hash table too large" unless hash_table_size <= MAX_HASH_TABLE_SIZE
      hash_table_size
    end

    # Any hash_bytes function will produce a valid compressed bitstream, but a good
    # hash function reduces the number of collisions and thus yields better
    # compression for compressible input, and more speed for incompressible
    # input. Of course, it doesn't hurt if the hash function is reasonably fast
    # either, as it gets called a lot.
    private def hash_bytes(bytes, shift)
      (bytes * 0x1e35a7bd).to_u32 >> shift
    end

    private def log2_floor(n : Int32)
      n == 0 ? -1 : 31 ^ n.leading_zeros_count
    end

    # Writes the uncompressed length as variable length integer.
    private def write_uncompressed_length(compressed : Slice)
      high_bit_mask = 0x80
      compressed_offset = 0
      uncompressed_len = compressed.size

      if uncompressed_len < (1 << 7) && uncompressed_len >= 0
        compressed[compressed_offset] = uncompressed_len.to_u8
        compressed_offset += 1
      elsif uncompressed_len < (1 << 14) && uncompressed_len > 0
        compressed[compressed_offset] = (uncompressed_len | high_bit_mask).to_u8
        compressed_offset += 1
        compressed[compressed_offset] = (uncompressed_len.to_u32 >> 7).to_u8
        compressed_offset += 1
      elsif uncompressed_len < (1 << 21) && uncompressed_len > 0
        compressed[compressed_offset] = (uncompressed_len | high_bit_mask).to_u8
        compressed_offset += 1
        compressed[compressed_offset] = ((uncompressed_len.to_u32 >> 7) | high_bit_mask).to_u8
        compressed_offset += 1
        compressed[compressed_offset] = (uncompressed_len.to_u32 >> 14).to_u8
        compressed_offset += 1
      elsif uncompressed_len < (1 << 28) && uncompressed_len > 0
        compressed[compressed_offset] = (uncompressed_len | high_bit_mask).to_u8
        compressed_offset += 1
        compressed[compressed_offset] = ((uncompressed_len.to_u32 >> 7) | high_bit_mask).to_u8
        compressed_offset += 1
        compressed[compressed_offset] = ((uncompressed_len.to_u32 >> 14) | high_bit_mask).to_u8
        compressed_offset += 1
        compressed[compressed_offset] = (uncompressed_len.to_u32 >> 21).to_u8
        compressed_offset += 1
      else
        compressed[compressed_offset] = (uncompressed_len | high_bit_mask).to_u8
        compressed_offset += 1
        compressed[compressed_offset] = ((uncompressed_len.to_u32 >> 7) | high_bit_mask).to_u8
        compressed_offset += 1
        compressed[compressed_offset] = ((uncompressed_len.to_u32 >> 14) | high_bit_mask).to_u8
        compressed_offset += 1
        compressed[compressed_offset] = ((uncompressed_len.to_u32 >> 21) | high_bit_mask).to_u8
        compressed_offset += 1
        compressed[compressed_offset] = (uncompressed_len.to_u32 >> 28).to_u8
        compressed_offset += 1
      end
      compressed_offset
    end
  end
end
