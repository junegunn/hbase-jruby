$VERBOSE = true

require 'rubygems'
require "test-unit"
require 'simplecov'
SimpleCov.start

RECREATE = false

$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
require "hbase-jruby"

# Required
HBase.resolve_dependency! 'cdh4.1.2'

class TestHBaseJRubyBase < Test::Unit::TestCase
  TABLE = 'test_hbase_jruby'
  ZK    = ENV.fetch 'HBASE_JRUBY_TEST_ZK'

  # Initialize
  hbase = HBase.new 'hbase.zookeeper.quorum' => ZK
  hbase.table(TABLE) do |table|
    table.drop! if table.exists?
  end
  hbase.close

  def setup
    @hbase ||= HBase.new 'hbase.zookeeper.quorum' => ZK
    @table = @hbase.table(TABLE)

    # Drop & Create
    @table.drop! if RECREATE && @table.exists?
    @table.create!(
      :cf1 => { :compression => :none, :bloomfilter => :row },
      :cf2 => { :bloomfilter => :rowcol },
      :cf3 => { :versions    => 1, :bloomfilter => org.apache.hadoop.hbase.regionserver.StoreFile::BloomType::ROWCOL }
    ) unless @table.exists?
    @table.enable! if @table.disabled?

    unless RECREATE
      @table.each do |row|
        @table.delete row.rowkey :raw
      end
      assert_equal 0, @table.count
    end
  end

  def teardown
    if RECREATE
      @table.drop! if @table && @table.exists?
    end
  end
end
