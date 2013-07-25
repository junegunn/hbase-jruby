$VERBOSE = true

require 'rubygems'
require "test-unit"
require 'simplecov'
SimpleCov.start

RECREATE = false

unless defined? Enumerator
  Enumerator = Enumerable::Enumerator
end

$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
$rowkey = Time.now.to_i

require "hbase-jruby"
if dist = ENV['HBASE_JRUBY_TEST_DIST']
  HBase.resolve_dependency!(dist, :verbose => true)
end
HBase.log4j = { 'log4j.threshold' => 'ERROR' }

class TestHBaseJRubyBase < Test::Unit::TestCase
  TABLE = 'test_hbase_jruby'
  ZK    = ENV.fetch 'HBASE_JRUBY_TEST_ZK'

  # Initialize
  hbase = HBase.new 'hbase.zookeeper.quorum' => ZK
  hbase.table(TABLE) do |table|
    table.drop! if table.exists?
  end
  hbase.close

  def connect
    HBase.new('hbase.zookeeper.quorum' => ZK,
              'hbase.client.retries.number' => 10,
              'hbase.client.scanner.caching' => 100)
  end

  def setup
    @hbase = connect
    @table = @hbase.table(TABLE)

    # Drop & Create
    @table.drop! if RECREATE && @table.exists?
    @table.create!(
      :cf1 => { :compression => :none, :bloomfilter => :row },
      :cf2 => { :bloomfilter => :rowcol },
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
