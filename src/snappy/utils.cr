module Compress::Snappy
  private module Utils
    extend self

    def check_position_indexes(s, e, len)
      raise bad_position_indexes(s, e, len) if (s < 0 || e < s || e > len)
    end

    def bad_position_indexes(s, e, len)
      return bad_position_index(s, len, "start index") if s < 0 || s > len
      return bad_position_index(e, len, "end index") if e < 0 || e > len
      "end index (#{e}) must not be less than start index (#{s})"
    end

    def bad_position_index(index, size, desc)
      return "#{desc} (#{index}) must not be negative" if index < 0
      raise ArgumentError.new("negative size: #{size}") if size < 0
      "#{desc} (#{index}) must not be greater than size (#{size})"
    end

    def equals(left : Slice, left_index : Int32, right : Slice, right_index : Int32, length : Int32)
      check_position_indexes(left_index, left_index + length, left.size)
      check_position_indexes(right_index, right_index + length, right.size)

      0.upto(length - 1) do |i|
        return false if (left[left_index + i] != right[right_index + i])
      end
      true
    end

    def lookup_short(b : Array(UInt16), index)
      b[index] & 0xffff
    end

    def load_byte(b : Slice)
      b[0] & 0xff
    end

    def load_int(b : Slice) : UInt32
      b[0].to_u32 & 0xff | ((b[1].to_u32 & 0xff) << 8) | ((b[2].to_u32 & 0xff) << 16) |
        ((b[3].to_u32 & 0xff) << 24)
    end

    def load_int64(b : Slice) : UInt64
      b[0].to_u64 & 0xff | ((b[1].to_u64 & 0xff) << 8) | ((b[2].to_u64 & 0xff) << 16) |
        ((b[3].to_u64 & 0xff) << 24) | ((b[4].to_u64 & 0xff) << 32) | ((b[5].to_u64 & 0xff) << 40) |
        ((b[6].to_u64 & 0xff) << 48) | ((b[7].to_u64 & 0xff) << 56)
    end

    def copy_long(src : Slice, dest : Slice)
      dest.copy_from(src.to_unsafe, 8)
    end
  end
end
