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

    # masked crc32c: 0x9274cda8 (little-endian in the frame)
    compressed[14..17].dup.reverse!.should eq(Bytes[0x92, 0x74, 0xcd, 0xa8])
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
    random = random_data(0.5, 60000)
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
    random = random_data(0.5, 60000)
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
      uncompress(Bytes.new(0))
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

  it "CRC32C matches reference vectors" do
    {
      ""                          => 0xa282ead8_u32,
      "abc"                       => 0x21f1576e_u32,
      "123456789"                 => 0xc78ab0e5_u32,
      "aaaaaaaaaaaabbbbbbbaaaaaa" => 0x9274cda8_u32,
    }.each do |input, expected|
      CRC32C.masked_crc32c(input.to_slice).should eq(expected)
    end
  end

  it "CRC32C unmask is the inverse of mask" do
    [0_u32, 0x12345678_u32, 0xE3069283_u32, 0xA282EAD8_u32, 0xFFFFFFFF_u32].each do |crc|
      CRC32C.unmask(CRC32C.mask(crc)).should eq(crc)
    end
  end

  it "honors compression_ratio" do
    rnd = Random.new(42)
    data = Bytes.new(20_000)
    i = 0
    while i < data.size
      if i > 8 && rnd.rand < 0.55
        back = 1 + rnd.rand(8).to_i
        ln = [rnd.rand(8).to_i + 4, data.size - i].min
        data[i, ln].copy_from(data[i - back, ln])
        i += ln
      else
        data[i] = rnd.rand(256).to_u8
        i += 1
      end
    end

    # the data compresses to ~45%, so a 0.3 threshold must force a raw chunk
    io = IO::Memory.new
    Compress::Snappy::Writer.open(io, compression_ratio: 0.3) { |w| w.write data }
    io.rewind
    io.to_s.byte_at(10).should eq(ChunkType::Uncompressed.value)

    # the default threshold (0.85) stores the same data compressed
    io = IO::Memory.new
    Compress::Snappy::Writer.open(io, compression_ratio: 0.85) { |w| w.write data }
    io.rewind
    io.to_s.byte_at(10).should eq(ChunkType::Compressed.value)
  end

  it "rewind does not disable checksum verification" do
    corrupt = (Consts::MAGIC_CHUNK + String.new(Bytes[1, 5, 0, 0, 0, 0, 0, 0, 97])).to_slice
    path = File.tempname("snappy_rewind_verify")
    File.write(path, String.new(corrupt))
    begin
      reader = Compress::Snappy::Reader.new(path, verify_checksums: true)
      expect_raises(SnappyError, "Corrupt input: invalid checksum") { reader.gets_to_end }
      reader.rewind
      expect_raises(SnappyError, "Corrupt input: invalid checksum") { reader.gets_to_end }
    ensure
      File.delete(path)
    end
  end

  it "rewind keeps sync_close" do
    path = File.tempname("snappy_rewind_sync")
    File.write(path, String.new(compress("hello".to_slice)))
    begin
      reader = Compress::Snappy::Reader.new(path)
      reader.gets_to_end
      reader.rewind
      reader.gets_to_end
      reader.close
      reader.@io.closed?.should be_true
    ensure
      File.delete(path)
    end
  end

  it "raises SnappyError when a copy exceeds the available output" do
    # varint len 4, 4-byte literal, then a 64-byte copy at offset 1 with no room left
    block = Bytes[4, 0x0c, 97, 97, 97, 97, 0xfe, 1, 0, 0, 0, 0, 0]
    expect_raises(SnappyError, "Corrupt input: copy length exceeds available output") do
      Compress::Snappy.decode(block)
    end
  end

  it "raises SnappyError when a trailer overruns the input" do
    # varint len 1, copy4 opcode claiming 4 trailer bytes with only 1 byte present
    block = Bytes[1, 0xff, 0]
    expect_raises(SnappyError, "Corrupt input: truncated trailer") do
      Compress::Snappy.decode(block)
    end
  end

  it "rejects compressed chunks that claim more than the max block size" do
    # a compressed chunk whose varint claims 100_000 uncompressed bytes (> 65536)
    random = random_data(0.5, 100_000)
    compressed = Compress::Snappy.encode(random)
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

    expect_raises(SnappyError, "exceeds the max block size") do
      uncompress(input)
    end
  end
end
