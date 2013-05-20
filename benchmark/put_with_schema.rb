#!/usr/bin/env ruby

require 'benchmark'
gem 'hbase-jruby', ARGV[0] || '0.3.0'
require 'hbase-jruby'
require 'parallelize'

HBase.resolve_dependency! 'cdh4.2'
hbase = HBase.new 'hbase.zookeeper.quorum' =>
          ENV.fetch('HBASE_JRUBY_TEST_ZK', '127.0.0.1')
test = hbase[:test]
test.drop! if test.exists?
test.create! :f, splits: (0..10 ** 10).step(10 ** 8).to_a[1...-1]

new_version = HBase::JRuby::VERSION >= '0.3'

if new_version
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
  } if new_version

  x.report(:put_old) {
    parallelize(threads) do
      1000.times do
        test.put rand(10 ** 10), data_old
      end
    end
  }
end
