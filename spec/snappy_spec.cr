require "./spec_helper"

module Compress::Snappy
  it "Test Byte for Byte Test Files" do
    files = test_files
    files.each do |file|
      puts "Testing file - #{file}"
      content = File.read(file).to_slice
      verify_compression(content, 0, content.size)
    end
  end

  it "Test Simple Writer & Reader" do
    input = "aaaaaaaaaaaabbbbbbbaaaaaa".to_slice
    compressed = compress(input)
    uncompressed = uncompress(compressed)

    uncompressed.should eq(input)
    #  10 byte stream header, 4 byte block header, 4 byte crc, 19 bytes
    compressed.size.should eq(37)

    # Stream header
    compressed[...10].should eq(Consts::MAGIC_CHUNK.to_slice)

    # flag: compressed
    compressed[10].should eq(ChunkType::Compressed.value)

    # length: 23 = 0x000017
    compressed[11..13].should eq(Bytes[0x17, 0x00, 0x00])

    # crc32c: 0x11911124
    compressed[14..17].dup.reverse!.should eq(Bytes[0x11, 0x91, 0x11, 0x24])
  end

  it "Test Empty Compression" do
    compressed = compress(Bytes.empty)
    compressed.should eq(Consts::MAGIC_CHUNK.to_slice)
    uncompress(Consts::MAGIC_CHUNK.to_slice).should eq(Bytes.empty)
  end

  it "Test Uncompressible Data" do
    random = random_data(1.0, 5000)
    compressed = compress(random)
    uncompressed = uncompress(compressed)

    uncompressed.should eq(random)
    compressed.size.should eq(random.size + 10 + 4 + 4)

    # flag: uncompressed
    compressed[10].should eq(ChunkType::Uncompressed.value)

    # length: 5004 = 0x138c
    compressed[11..13].should eq(Bytes[0x8c, 0x13, 0x00])
  end

  it "Test Short block header" do
    input = (Consts::MAGIC_CHUNK + String.new(Bytes[0])).to_slice
    expect_raises(SnappyError, "encountered EOF while reading block header") do
      uncompress(input)
    end
  end

  it "Test Short block data" do
    # flag = 0, size = 8, crc32c = 0, block data= [x, x]
    input = (Consts::MAGIC_CHUNK + String.new(Bytes[1, 8, 0, 0, 0, 0, 0, 0, 120, 120])).to_slice
    expect_raises(SnappyError, "unexpected EOF when reading frame") do
      uncompress(input)
    end
  end

  it "Test Unskippable Chunk Flags" do
    2.upto(0x7f - 1) do |i|
      input = (Consts::MAGIC_CHUNK + String.new(Bytes[i.to_u8, 5, 0, 0, 0, 0, 0, 0, 0])).to_slice
      expect_raises(SnappyError, "unsupported unskippable chunk #{i}") do
        uncompress(input)
      end
    end
  end

  it "Test Skippable Chunk Flags" do
    0x80.upto(0xfe - 1) do |i|
      input = (Consts::MAGIC_CHUNK + String.new(Bytes[i.to_u8, 5, 0, 0, 0, 0, 0, 0, 0])).to_slice
      uncompress(input)
    end
  end

  it "Test Invalid Block Size Zero" do
    # flag = 0, block size = 4, crc32c = 0
    input = (Consts::MAGIC_CHUNK + String.new(Bytes[1, 4, 0, 0, 0, 0, 0, 0])).to_slice
    expect_raises(SnappyError, "invalid length: 4 for chunk flag: Uncompressed") do
      uncompress(input)
    end
  end

  it "Test Invalid checksum" do
    # flag = 0, size = 5, crc32c = 0, block data = [a]
    input = (Consts::MAGIC_CHUNK + String.new(Bytes[1, 5, 0, 0, 0, 0, 0, 0, 97])).to_slice
    expect_raises(SnappyError, "Corrupt input: invalid checksum") do
      uncompress(input)
    end
  end

  it "Test Invalid checksum ignored when verification disabled" do
    # flag = 0, size = 5, crc32c = 0, block data = [a]
    input = (Consts::MAGIC_CHUNK + String.new(Bytes[1, 5, 0, 0, 0, 0, 0, 0, 97])).to_slice
    io = IO::Memory.new(String.new(input))
    val = Snappy::Reader.open(io, verify_checksums: false) do |sr|
      sr.gets_to_end
    end
    val.should eq("a")
  end

  it "Test Larger frames raw" do
    random = random_data(0.5, 100000)
    input = Bytes.new(Consts::MAGIC_CHUNK.size + 8 + random.size)
    input.copy_from(Consts::MAGIC_CHUNK.to_unsafe, Consts::MAGIC_CHUNK.size)
    input[10] = ChunkType::Uncompressed.value.to_u8
    length = random.size.to_u32 + 4
    input[11] = length.to_u8!
    input[12] = (length >> 8).to_u8!
    input[13] = (length >> 16).to_u8!

    crc32 = CRC32C.masked_crc32c(random)
    input[14] = crc32.to_u8!
    input[15] = (crc32 >> 8).to_u8!
    input[16] = (crc32 >> 16).to_u8!
    input[17] = (crc32 >> 24).to_u8!

    input[18..].copy_from(random.to_unsafe, random.size)

    uncompressed = uncompress(input)

    random.should eq(uncompressed)
  end

  it "Test Larger frames compressed" do
    random = random_data(0.5, 500000)
    compressed = Snappy.encode(random)

    input = Bytes.new(Consts::MAGIC_CHUNK.size + 8 + compressed.size)
    input.copy_from(Consts::MAGIC_CHUNK.to_unsafe, Consts::MAGIC_CHUNK.size)
    input[10] = ChunkType::Compressed.value.to_u8
    length = compressed.size.to_u32 + 4
    input[11] = length.to_u8!
    input[12] = (length >> 8).to_u8!
    input[13] = (length >> 16).to_u8!

    crc32 = CRC32C.masked_crc32c(random)
    input[14] = crc32.to_u8!
    input[15] = (crc32 >> 8).to_u8!
    input[16] = (crc32 >> 16).to_u8!
    input[17] = (crc32 >> 24).to_u8!

    input[18..].copy_from(compressed.to_unsafe, compressed.size)

    uncompressed = uncompress(input)

    random.should eq(uncompressed)
  end

  it "Test Larger frames compressed with smaller raw larger" do
    random = random_data(0.5, 100000)
    compressed = Snappy.encode(random)

    input = Bytes.new(Consts::MAGIC_CHUNK.size + 8 + compressed.size)
    input.copy_from(Consts::MAGIC_CHUNK.to_unsafe, Consts::MAGIC_CHUNK.size)
    input[10] = ChunkType::Compressed.value.to_u8
    length = compressed.size.to_u32! + 4
    input[11] = length.to_u8!
    input[12] = (length >> 8).to_u8!
    input[13] = (length >> 16).to_u8!

    crc32 = CRC32C.masked_crc32c(random)
    input[14] = crc32.to_u8!
    input[15] = (crc32 >> 8).to_u8!
    input[16] = (crc32 >> 16).to_u8!
    input[17] = (crc32 >> 24).to_u8!

    input[18..].copy_from(compressed.to_unsafe, compressed.size)

    uncompressed = uncompress(input)

    random.should eq(uncompressed)
  end

  it "Test Large Writes" do
    random = random_data(0.5, 500000)
    io = IO::Memory.new

    Snappy::Writer.open(io) do |sw|
      # partially fill buffer
      small = 1000
      sw.write random[...small]

      # write more than the buffer size
      sw.write random[small...]
    end
    io.rewind
    compressed = io.gets_to_end.to_slice

    compressed.size.should be < random.size

    # decompress
    uncompressed = uncompress(compressed)

    uncompressed.should eq(random)

    # decompress byte at a time
    io.rewind
    uncompressed = Bytes.new(uncompressed.size)
    i = 0
    Snappy::Reader.open(io) do |sr|
      sr.each_byte do |b|
        uncompressed[i] = b
        i += 1
      end
    end

    i.should eq(random.size)
    uncompressed.should eq(random)
  end

  it "Test Single Byte Writes" do
    random = random_data(0.5, 500000)
    io = IO::Memory.new

    Snappy::Writer.open(io) do |sw|
      random.each do |b|
        sw.write_byte b
      end
    end
    io.rewind
    compressed = io.gets_to_end.to_slice

    compressed.size.should be < random.size

    # decompress
    uncompressed = uncompress(compressed)

    uncompressed.should eq(random)
  end

  it "Test Extra flushes" do
    random = random_data(0.5, 500000)
    io = IO::Memory.new

    Snappy::Writer.open(io) do |sw|
      sw.write random
      0.upto(9) { |_| sw.flush }
    end
    io.rewind
    compressed = io.gets_to_end.to_slice

    compressed.size.should be < random.size

    # decompress
    uncompressed = uncompress(compressed)

    uncompressed.should eq(random)
  end

  it "Test Uncompressible Range" do
    max = 128 * 1024
    random = random_data(1.0, max)
    i = 1
    while i <= max
      orignal = random[0, i]
      compressed = compress(orignal)
      uncompressed = uncompress(compressed)
      uncompressed.should eq(orignal)
      i += 102
    end
  end

  it "Test empty stream" do
    expect_raises(SnappyError, "encountered EOF while reading stream header") do
      uncompress(Bytes.empty)
    end
  end

  it "Test Invalid Stream Header" do
    expect_raises(SnappyError, "Invalid Snappy stream header") do
      uncompress("b000gus000".to_slice)
    end
  end

  it "Tests that the presence of the marker bytes can appear as a valid frame anywhere in stream" do
    size = 500000
    random = random_data(0.5, size)
    io = IO::Memory.new
    marker_frame = Consts::MAGIC_CHUNK.to_slice
    Snappy::Writer.open(io) do |sw|
      i = 0
      while i < size
        to_write = Math.max((size - i) // 4, 512)
        # write some data to be compressed
        sw.write random[i, Math.min(size - i, to_write)]
        # force teh write of a frame
        sw.flush

        # write the marker frame to the io
        io.write marker_frame

        # this is not accurate for the final write, but at that point it
        # does not matter
        # as we wil be exiting the loop now
        i += to_write
      end
    end
    io.rewind
    compressed = io.gets_to_end.to_slice
    # decompress
    uncompressed = uncompress(compressed)
    uncompressed.should eq(random)
  end

  it "Test Byte for Byte Test Data Files" do
    files = test_files
    files.each do |file|
      content = File.read(file).to_slice
      compressed = compress(content)
      uncompressed = uncompress(compressed)
      uncompressed.should eq(content)
    end
  end

  it "Test Byte for Byte output synthetic random data" do
    1.upto((65 * 1024) - 1) do |i|
      verify_compression(i)
    end
  end
end
