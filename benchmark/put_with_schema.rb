#!/usr/bin/env ruby

if ARGV.length != 3
  puts "usage: #{$0} <version> <dist> <hbase.zookeeper.quorum>"
  exit 1
end

require 'benchmark'
gem 'hbase-jruby', ARGV[0]
require 'hbase-jruby'
require 'parallelize'

HBase.log4j = { 'log4j.threshold' => 'ERROR' }
HBase.resolve_dependency! ARGV[1]
hbase = HBase.new 'hbase.zookeeper.quorum' => ARGV[2],
                  'hbase.hconnection.threads.core' => 256
puts "- HTablePool (deprecated): #{hbase.use_table_pool? rescue true}"

test = hbase[:test]
test.drop! if test.exists?
test.create! :f, splits: (0..10 ** 10).step(10 ** 8).to_a[1...-1]

schema_supported = HBase::JRuby::VERSION >= '0.3'

if schema_supported
  hbase.schema = {
    test: {
      f: {
        str: :string,
        num: :fixnum,
        sym: :symbol,
        bool: :boolean,
        float: :float
      }
    }
  }

  data = {
    str:   "Hello world",
    num:   1000,
    sym:   :foobar,
    bool:  true,
    float: 3.14
  }
end

data_old = {
  'f:str'   => "Hello world",
  'f:num'   => 1000,
  'f:sym'   => :foobar,
  'f:bool'  => true,
  'f:float' => 3.14
}

threads = 256
Benchmark.bmbm(30) do |x|
  x.report(:put) {
    parallelize(threads) do
      1000.times do
        test.put rand(10 ** 10), data
      end
    end
  } if schema_supported

  x.report(:put_old) {
    parallelize(threads) do
      1000.times do
        test.put rand(10 ** 10), data_old
      end
    end
  }
end
