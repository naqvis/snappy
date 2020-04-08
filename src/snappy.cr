# `Snappy` shard implements the Snappy compression format. It aims for very
# high speeds and reasonable compression.
#
# There are actually two Snappy formats: block and stream. They are related,
# but different: trying to decompress block-compressed data as a Snappy stream
# will fail, and vice versa. The block format is the `Snappy.decode` and `Snappy.encode`
# functions and the stream format is the Reader and Writer types.
#
# The block format, the more common case, is used when the complete size (the
# number of bytes) of the original data is known upfront, at the time
# compression starts. The stream format, also known as the framing format, is
# for when that isn't always true.
#
# The canonical, C++ implementation is at https://github.com/google/snappy and
# it only implements the block format.

module Snappy
  VERSION = "0.1.3"
end

require "./snappy/*"
