$VERBOSE = true

require 'simplecov'
SimpleCov.start
require 'minitest/autorun'
RECREATE = false

unless defined? Enumerator
  Enumerator = Enumerable::Enumerator
end

$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
$rowkey = Time.now.to_i

require "hbase-jruby"
if jar = ENV['HBASE_JRUBY_TEST_JAR']
  $CLASSPATH << jar
end
HBase.log4j = { 'log4j.threshold' => 'ERROR' }

class TestHBaseJRubyBase < Minitest::Test
  TABLE = 'test_hbase_jruby'
  ZK    = ENV.fetch('HBASE_JRUBY_TEST_ZK', 'localhost')

  # Initialize
  hbase = HBase.new 'hbase.zookeeper.quorum' => ZK
  hbase.table(TABLE) do |table|
    table.drop! if table.exists?
  end
  hbase.close

  def connect
    HBase.new('hbase.zookeeper.quorum' => ZK,
              'hbase.client.retries.number' => 5,
              'hbase.client.scanner.caching' => 100)
  end

  def setup
    @hbase = connect
    @table = @hbase.table(TABLE)
    begin
      org.apache.hadoop.hbase.client.coprocessor.AggregationClient
      @aggregation = true
    rescue NameError
      @aggregation = false
    end

    # Drop & Create
    @table.drop! if RECREATE && @table.exists?
    @table.create!(
      :cf1 => { :compression => :none, :bloomfilter => :row, :versions => 3 },
      :cf2 => { :bloomfilter => :rowcol, :versions => 3 },
      :cf3 => { :versions => 1, :bloomfilter => :rowcol }
    ) unless @table.exists?
    @table.enable! if @table.disabled?

    unless RECREATE
      @table.delete(*@table.map { |row| [row.rowkey(:raw)] })
      assert_equal 0, @table.count
    end
  end

  def next_rowkey
    $rowkey += 1
  end

  def teardown
    if RECREATE
      @table.drop! if @table && @table.exists?
    end
    @hbase.close
  end
end
