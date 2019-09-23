# The Snappy module contains readers and writers of Snappy format compressed
# data, as specified in [Snappy](https://github.com/google/snappy).

module Snappy
  class SnappyError < Exception
  end

  # Each encoded block begins with the varint-encoded length of the decoded data,
  # followed by a sequence of chunks. Chunks begin and end on byte boundaries. The
  # first byte of each chunk is broken into its 2 least and 6 most significant bits
  # called l and m: l ranges in [0, 4) and m ranges in [0, 64). l is the chunk tag.
  # Zero means a literal tag. All other values mean a copy tag.
  #
  # For literal tags:
  #  - If m < 60, the next 1 + m bytes are literal bytes.
  #  - Otherwise, let n be the little-endian unsigned integer denoted by the next
  #    m - 59 bytes. The next 1 + n bytes after that are literal bytes.
  #
  # For copy tags, length bytes are copied from offset bytes ago, in the style of
  # Lempel-Ziv compression algorithms. In particular:
  #  - For l == 1, the offset ranges in [0, 1<<11) and the length in [4, 12).
  #    The length is 4 + the low 3 bits of m. The high 3 bits of m form bits 8-10
  #    of the offset. The next byte is bits 0-7 of the offset.
  #  - For l == 2, the offset ranges in [0, 1<<16) and the length in [1, 65).
  #    The length is 1 + m. The offset is the little-endian unsigned integer
  #    denoted by the next 2 bytes.
  #  - For l == 3, this tag is a legacy format that is no longer issued by most
  #    encoders. Nonetheless, the offset ranges in [0, 1<<32) and the length in
  #    [1, 65). The length is 1 + m. The offset is the little-endian unsigned
  #    integer denoted by the next 4 bytes.

  enum ElemType
    Literal = 0x00
    Copy1   = 0x01
    Copy2   = 0x02
    Copy4   = 0x03
  end

  module Consts
    CHECK_SUM_SIZE    = 4
    CHUNK_HEADER_SIZE = 4
    MAGIC_BODY        = "sNaPpY"
    # header consists of the stream identifier flag, 3 bytes indicating a
    # length of 6, and "sNaPpY" in ASCII
    MAGIC_CHUNK = "\xff\x06\x00\x00" + MAGIC_BODY

    # MAX_BLOCK_SIZE is the maximum size that a chunk of  uncompressed data must be no
    # longer than this size. It is not part of the wire format per se,
    # but some parts of the encoder assume that an offset fits into a UInt16.
    #
    # Also, for the framing format (Writer type instead of encode function),
    # https://github.com/google/snappy/blob/master/framing_format.txt says
    # that "the uncompressed data in a chunk must be no longer than 65536
    # bytes".
    MAX_BLOCK_SIZE = 65536

    # MAX_ENCODED_LEN_OF_MAX_BLOCK_SIZE equals decode_length(max_block_size), but is
    # hard coded to be a const instead of a variable, so that obufLen can also
    # be a const.
    MAX_ENCODED_LEN_OF_MAX_BLOCK_SIZE = 76490

    BUF_HEADER_LEN = MAGIC_CHUNK.size + CHECK_SUM_SIZE + CHUNK_HEADER_SIZE
    BUF_LEN        = BUF_HEADER_LEN + MAX_ENCODED_LEN_OF_MAX_BLOCK_SIZE

    # Minimum Compression ratio that must be achieved to write the compressed data.
    # This must in (0,1.0]
    MIN_COMPRESSION_RATIO = 0.85
  end

  # The chunk types are specified at
  # https://github.com/google/snappy/blob/master/framing_format.txt
  enum ChunkType
    Compressed      = 0x00
    Uncompressed    = 0x01
    Padding         = 0xfe
    StreamIdentifer = 0xff
  end

  def self.decode_length(compressed : Slice)
    Decompressor.uncompressed_length(compressed)
  end

  def self.decode(src : Slice, dest : Slice)
    Decompressor.uncompress(src, dest)
  end

  def self.decode(compressed : Slice)
    Decompressor.uncompress(compressed)
  end

  def self.encode_length(source_length : Int32)
    Compressor.max_compressed_length(source_length)
  end

  def self.encode(data : Slice)
    res = Bytes.new(encode_length(data.size))
    compressed_size = Compressor.compress(data, data.size, res, 0)
    res[0...compressed_size]
  end

  def self.encode(data : Slice, src_offset, length, dest : Slice, dest_offset = 0)
    Compressor.compress(data[src_offset..], length, dest, dest_offset)
  end

  protected def self.decode_uint(b : Slice)
    Decompressor.read_uint(b)
  end

  protected def self.encode_uint(b : Slice, val : UInt64)
    Compressor.write_uncompressed_length(b, 0, val.to_i)
  end
end
