# Crystal Snappy
![CI](https://github.com/naqvis/snappy/workflows/CI/badge.svg)
[![GitHub release](https://img.shields.io/github/release/naqvis/snappy.svg)](https://github.com/naqvis/snappy/releases)
[![Docs](https://img.shields.io/badge/docs-available-brightgreen.svg)](https://naqvis.github.io/snappy/)

**Pure Crystal** implementation of [Snappy](https://github.com/google/snappy) compression format. This implementation supports more common case [Snappy Framing Format](https://github.com/google/snappy/blob/master/framing_format.txt).

## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     snappy:
       github: naqvis/snappy
   ```

2. Run `shards install`

## Usage

```crystal
require "snappy"
```

Module provides both `Compress::Snappy::Reader` and `Compress::Snappy::Writer` for use with streams like `IO` and/or files. It also provides `Compress::Snappy#decode` and `Compress::Snappy#encode` for uncompressing and compressing block formats.

## Example: decompress a snappy file
#
```crystal
require "snappy"

File.write("file.sz", Bytes[255, 6, 0, 0, 115, 78, 97, 80, 112, 89, 1, 8, 0, 0, 104, 16, 130, 162, 97, 98, 99, 100])

string = File.open("file.sz") do |file|
   Compress::Snappy::Reader.open(file) do |sz|
     sz.gets_to_end
   end
end
string # => "abcd"
```
## Example: compress a file to snappy format
#
```crystal
require "snappy"

File.write("file.txt", "abcd")

File.open("./file.txt", "r") do |input_file|
  File.open("./file.sz", "w") do |output_file|
    Compress::Snappy::Writer.open(output_file) do |sz|
      IO.copy(input_file, sz)
    end
  end
end
```

Refer to **specs** for usage examples.

## Development

To run all tests:

```
crystal spec
```
## Contributing

1. Fork it (<https://github.com/naqvis/snappy/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Ali Naqvi](https://github.com/naqvis) - creator and maintainer
