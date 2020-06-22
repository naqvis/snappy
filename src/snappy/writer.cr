# A write-only `IO` object to compress data in the snappy format.
#
# Instances of this class wrap another `IO` object. When you write to this
# instance, it compresses the data and writes it to the underlying `IO`.
#
# NOTE: unless created with a block, `close` must be invoked after all
# data has been written to a `Snappy::Writer` instance.
# Note: Writer handles the Snappy stream format, not the Snappy block format.
#
# ### Example: compress a file
#
# ```
# require "snappy"
#
# File.write("file.txt", "abc")
#
# File.open("./file.txt", "r") do |input_file|
#   Snappy::Writer.open("./file.sz") do |sz|
#     IO.copy(input_file, sz)
#   end
# end
# ```
require "./crc32c"

class Compress::Snappy::Writer < IO
  # Whether to close the enclosed `IO` when closing this writer.
  property? sync_close = false

  # Returns `true` if this writer is closed.
  getter? closed = false

  # Creates a new writer to the given *io*.
  def initialize(@io : IO, @compression_ratio = Snappy::Consts::MIN_COMPRESSION_RATIO, @sync_close = false)
    raise ArgumentError.new "compression_ratio #{@compression_ratio} must be between (0,1.0]." unless @compression_ratio > 0 && @compression_ratio <= 1.0
    @in_buf = Bytes.new(Snappy::Consts::MAX_BLOCK_SIZE)
    @out_buf = Bytes.new(Snappy::Consts::BUF_LEN)
    @position = 0
    write_header
  end

  # Creates a new writer to the given *filename*.
  def self.new(filename : String)
    new(::File.new(filename, "w"), sync_close: true)
  end

  # Creates a new writer to the given *io*, yields it to the given block,
  # and closes it at the end.
  def self.open(io : IO, sync_close = false)
    writer = new(io, sync_close: sync_close)
    yield writer ensure writer.close
  end

  # Creates a new writer to the given *filename*, yields it to the given block,
  # and closes it at the end.
  def self.open(filename : String)
    writer = new(filename)
    yield writer ensure writer.close
  end

  # Always raises `IO::Error` because this is a write-only `IO`.
  def read(slice : Bytes)
    raise IO::Error.new("Can't read from Snappy::Writer")
  end

  # See `IO#write`.
  def write(slice : Bytes) : Nil
    check_open
    return if slice.empty?
    free = Snappy::Consts::MAX_BLOCK_SIZE - @position
    offset = 0
    length = slice.size
    # enough free space in buffer for entire input
    if free >= length
      copy_to_buffer(slice[offset..], slice.size)
      return
    end

    # fill partial buffer as much as possible and flush
    unless @position <= 0
      copy_to_buffer(slice[offset..], free)
      flush
      offset += free
      length -= free
    end

    # write remaining full blocks directly from input array
    while length >= Snappy::Consts::MAX_BLOCK_SIZE
      write_compressed(slice[offset..], Snappy::Consts::MAX_BLOCK_SIZE)
      offset += Snappy::Consts::MAX_BLOCK_SIZE
      length -= Snappy::Consts::MAX_BLOCK_SIZE
    end

    # copy remaining partial block into now-empty buffer
    copy_to_buffer(slice[offset..], length)
  end

  # Compresses and write out any unbuffered data. This does nothing if there is no
  # currently buffered data.
  # See `IO#flush`.
  def flush
    check_open
    if (@position > 0)
      write_compressed(@in_buf, @position)
      @position = 0
    end
  end

  # Closes this writer. Must be invoked after all data has been written.
  def close
    return if @closed
    flush
    @closed = true
    @io.close if @sync_close
  end

  private def copy_to_buffer(slice : Bytes, length)
    @in_buf[@position..].copy_from(slice.to_unsafe, length)
    @position += length
  end

  # calculate the crc, compresses the data, determines if the compression ratio
  # is acceptable and call write_block to actual write the frame.
  private def write_compressed(input : Bytes, length : Int32)
    # crc is based on the user supplied input data
    crc = CRC32C.masked_crc32c(input, length)
    compressed = Compressor.compress(input, length, @out_buf, 0)

    # only use the compressed data if compression ratio is <= MIN_COMPRESSION_RATIO
    if (compressed.to_f64 / length) <= Consts::MIN_COMPRESSION_RATIO
      write_block(@out_buf, compressed, true, crc)
    else
      # otherwise use the uncompressed data
      write_block(input, length, false, crc)
    end
  end

  # Each chunk consists first a single byte of chunk identifier, then a
  # three-byte little-endian length of the chunk in bytes (from 0 to
  # 16777215, inclusive), and then the data if any. The four bytes of chunk
  # header is not counted in the data length.

  private def write_block(data : Slice, length : Int32, compressed : Bool,
                          crc32c)
    flag = compressed ? ChunkType::Compressed : ChunkType::Uncompressed
    # the length written out to the header is both the checksum and the frame
    header_len = length + 4

    header = Bytes.new(4)
    header[0] = flag.value.to_u8
    header[1] = (header_len >> 0).to_u8!
    header[2] = (header_len >> 8).to_u8!
    header[3] = (header_len >> 16).to_u8!

    @io.write header

    # write crc
    @io.write_bytes crc32c, IO::ByteFormat::LittleEndian
    # write data
    @io.write data[...length]
  end

  private def write_header
    @io.write Consts::MAGIC_CHUNK.to_slice
  end
end
