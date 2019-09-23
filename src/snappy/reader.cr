# A read-only `IO` object to decompress data in the Snappy compression format.
#
# Instances of this class wrap another IO object. When you read from this instance
# instance, it reads data from the underlying IO, decompresses it, and returns
# it to the caller.
#
# Reads Snappy compressed stream, using the framing format
# described at https://github.com/google/snappy/blob/master/framing_format.txt
#
# ### Example: decompress a snappy file
#
# ```
# require "snappy"
#
# File.write("file.sz", Bytes[255, 6, 0, 0, 115, 78, 97, 80, 112, 89, 1, 8, 0, 0, 104, 16, 130, 162, 97, 98, 99, 100])
#
# string = File.open("file.sz") do |file|
#   Snappy::Reader.open(file) do |sz|
#     sz.gets_to_end
#   end
# end
# string # => "abcd"
# ```

require "./crc32c"

class Snappy::Reader < IO
  include IO::Buffered
  # Whether to close the enclosed `IO` when closing this reader.
  property? sync_close = false

  # Returns `true` if this reader is closed.
  getter? closed = false

  # Creates a new reader from the given *io*.
  def initialize(@io : IO, @verify_checksums = true, @sync_close = false)
    # stream must begin with stream header
    header = Bytes.new(Consts::MAGIC_CHUNK.size)
    read = @io.read(header)
    raise SnappyError.new("encountered EOF while reading stream header") if read < header.size
    raise SnappyError.new("Invalid Snappy stream header") unless Consts::MAGIC_CHUNK.to_slice == header

    @frame_header = Bytes.new(Snappy::Consts::CHUNK_HEADER_SIZE)
    # A single frame read from underlying io
    @input = Bytes.new(Snappy::Consts::MAX_BLOCK_SIZE + 5)
    # decompressed data from input
    @uncompressed = Bytes.new(Snappy::Consts::MAX_BLOCK_SIZE + 5)
    # Indicates if we have reached the EOF on @io
    @eof = false
    # The position in @input to read to
    @valid = 0
    # The next position to read from @buffer
    @position = 0
    # buffer is a reference to the real buffer of uncompressed data for the
    # current block: uncompressed if the block is compressed, or input if it is not.
    @buffer = Bytes.empty
  end

  # Creates a new reader from the given *filename*.
  def self.new(filename : String, verify_checksums = true)
    new(::File.new(filename), verify_checksums, sync_close: true)
  end

  # Creates a new reader from the given *io*, yields it to the given block,
  # and closes it at the end.
  def self.open(io : IO, verify_checksums = true, sync_close = false)
    reader = new(io, verify_checksums, sync_close: sync_close)
    yield reader ensure reader.close
  end

  # Creates a new reader from the given *filename*, yields it to the given block,
  # and closes it at the end.
  def self.open(filename : String, verify_checksums = true)
    reader = new(filename, verify_checksums)
    yield reader ensure reader.close
  end

  def unbuffered_read(slice : Bytes)
    check_open
    return 0 if slice.empty?
    return 0 unless ensure_buffer

    size = Math.min(slice.size, available())
    slice.copy_from(@buffer[@position..].to_unsafe, size)
    @position += size
    size
  end

  # Always raises `IO::Error` because this is a read-only `IO`.
  def unbuffered_write(slice : Bytes) : Nil
    raise IO::Error.new("Can't write to Snappy::Reader")
  end

  def unbuffered_flush
    raise IO::Error.new "Can't flush Snappy::Reader"
  end

  # Closes this reader.
  def unbuffered_close
    return if @closed
    @closed = true
    @io.close if @sync_close
  end

  def unbuffered_rewind
    check_open
    @io.rewind
    initialize(@io, @sync_close)
  end

  def reset(io)
    initialize(io, @sync_close)
  end

  private def available
    return 0 if @closed
    @valid - @position
  end

  private def ensure_buffer
    return true if available > 0
    return false if @eof

    unless read_block_header
      @eof = true
      return false
    end

    # get action based on header
    meta = frame_metadata

    if meta.action == FrameAction::SKIP
      @io.skip(meta.length)
      return ensure_buffer
    end
    if meta.length > @input.size
      @input = Bytes.new(meta.length)
    end

    actual_read = @io.read(@input[0...meta.length])
    raise SnappyError.new "unexpected EOF when reading frame" unless actual_read == meta.length

    frame = frame_data(@input)

    if meta.action == FrameAction::UNCOMPRESS
      uncompressed_len = Decompressor.uncompressed_length(@input[frame.offset..])
      @uncompressed = Bytes.new(uncompressed_len) if uncompressed_len > @uncompressed.size
      @valid = Decompressor.uncompress(@input[frame.offset, actual_read - frame.offset], @uncompressed)

      @buffer = @uncompressed
      @position = 0
    else
      # we need to start reading at the offset
      @position = frame.offset
      @buffer = @input
      # valid is until the end of the read data, regardless of offset
      # indicating where we start
      @valid = actual_read
    end

    if @verify_checksums
      actual_crc = Snappy::CRC32C.masked_crc32c(@buffer[@position..], @valid - @position)
      raise SnappyError.new("Corrupt input: invalid checksum") if actual_crc != frame.checksum
    end
    true
  end

  private def read_block_header
    read = @io.read(@frame_header)
    return false if read == 0
    raise SnappyError.new "encountered EOF while reading block header" if read < @frame_header.size
    true
  end

  private def frame_metadata
    length = @frame_header[1].to_i | (@frame_header[2].to_i << 8) | (@frame_header[3].to_i << 16)
    chunk_type = Snappy::ChunkType.new(@frame_header[0].to_i)
    case chunk_type
    when Snappy::ChunkType::Compressed
      action = FrameAction::UNCOMPRESS
      min_len = 5
    when Snappy::ChunkType::Uncompressed
      action = FrameAction::RAW
      min_len = 5
    when Snappy::ChunkType::StreamIdentifer
      raise SnappyError.new "stream identifier chunk with invalid length: " if length != Snappy::Consts::MAGIC_BODY.size
      action = FrameAction::SKIP
      min_len = 6
    else
      # Section 4.5. Reserved unskippable chunks (chunk types 0x02-0x7f).
      raise SnappyError.new "unsupported unskippable chunk #{chunk_type.to_s}" if chunk_type.value <= 0x7f
      #  Section 4.4 Padding (chunk type 0xfe).
      # Section 4.6. Reserved skippable chunks (chunk types 0x80-0xfd).
      action = FrameAction::SKIP
      min_len = 0
    end

    raise SnappyError.new("invalid length: #{length} for chunk flag: #{chunk_type.to_s}") if length < min_len

    FrameMetaData.new(action, length)
  end

  private def frame_data(buf : Slice)
    # crc is contained in the frame content
    crc = buf[0].to_u32 | (buf[1].to_u32 << 8) | (buf[2].to_u32 << 16) | (buf[3].to_u32 << 24)
    FrameData.new(crc, 4)
  end

  private enum FrameAction
    RAW
    SKIP
    UNCOMPRESS
  end

  private record FrameMetaData, action : FrameAction, length : Int32
  private record FrameData, checksum : UInt32, offset : Int32
end
