require "spec"
require "../src/snappy"

module Snappy
  extend self
  TEST_DATA_DIR = "./spec/testdata"
  GENERATOR     = RandomGenerator.new(0.5)

  def random_data(compression_ratio, length)
    gen = RandomGenerator.new(compression_ratio)
    gen.next_position(length)
    random = gen.data[...length].dup
    random.size.should eq(length)
    random
  end

  def test_files
    files = Dir.children(TEST_DATA_DIR)
    files.map! { |f| File.join(TEST_DATA_DIR, f) }
    files
  end

  def verify_compression(size : Int32)
    input = GENERATOR.data
    position = GENERATOR.next_position(size)
    verify_compression(input, position, size)
  end

  def verify_compression(input : Slice, position, size)
    compressed = Bytes.new(Snappy.encode_length(size))
    compressed_size = Snappy.encode(input, position, size,
      compressed)

    # verify the contents can be uncompressed
    uncompressed = Bytes.new(size)
    Snappy.decode(compressed[0, compressed_size], uncompressed)
    if !Utils.equals(uncompressed, 0, input, position, size)
      fail "Invalid uncompressed output for input size #{size} at offset #{position}"
    end
  end

  def compress(input : Slice)
    io = IO::Memory.new
    Snappy::Writer.open(io) do |sw|
      sw.write input
    end
    io.rewind
    io.gets_to_end.to_slice
  end

  def uncompress(input : Slice)
    io = IO::Memory.new(String.new(input))
    val = Snappy::Reader.open(io) do |sr|
      sr.gets_to_end
    end
    val.to_slice
  end

  class RandomGenerator
    getter data : Bytes

    def initialize(@compression_ratio : Float64)
      # We use a limited amount of data over and over again and ensure
      # that it is larger than the compression window (32KB), and also
      # large enough to serve all typical value sizes we want to write.
      @data = Bytes.new(1048576 + 100)
      @position = 0
      @rnd = Random.new(301)

      i = 0
      while i < 1048576
        # Add a short fragment that is as compressible as specified ratio
        @data[i..].copy_from(compressible_data(100).to_unsafe, 100)
        i += 100
      end
    end

    def next_position(length)
      if (@position + length > @data.size)
        @position = 0
        fail "length < @data.size" unless length < @data.size
      end
      result = @position
      @position += length
      result
    end

    private def compressible_data(length)
      raw = (length * @compression_ratio).to_i
      raw = 1 if raw < 1
      raw_data = @rnd.random_bytes(raw)

      # Duplicate the random data until we have filled "length" bytes
      dest = Bytes.new(length)
      i = 0
      while i < length
        chunk_len = Math.min(raw_data.size, length - i)
        dest[i..].copy_from(raw_data.to_unsafe, chunk_len)
        i += chunk_len
      end
      dest
    end
  end
end
