#!/usr/bin/env ruby

require "test-unit"
$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
require "hbase-jruby"

class TestHBaseJruby < Test::Unit::TestCase
  TABLE = 'test_hbase_jruby'
  ZK    = ENV['HBASE_JRUBY_TEST_ZK'] || '127.0.0.1'

  def setup
    HBase.resolve_dependency! :cdh4
    @hbase = HBase.new 'hbase.zookeeper.quorum' => ZK
    @table = @hbase.table(TABLE)

    # Drop & Create
    @table.drop! if @table.exists?
    assert_false @table.exists?
    @table.create!(
      :cf1 => { :compression => :none, :bloomfilter => :row },
      :cf2 => { :bloomfilter => :rowcol },
      :cf3 => { :versions    => 1 })
    assert @table.exists?
  end

  def teardown
    @table.drop! if @table.exists?
    assert_false @table.exists?
    @table.close
  end

  def test_create_table_symbol_string
    t = @hbase.table(:test_hbase_jruby_create_table)
    t.drop! if t.exists?
    [ :cf, 'cf', :cf => {} ].each do |cf|
      assert_false t.exists?
      t.create! cf
      assert t.exists?
      t.drop!
    end
  end

  def test_tables
    assert @hbase.table_names.include?(TABLE)
    assert @hbase.tables.map(&:name).include?(TABLE)
  end

  def test_put_then_get
    # Single record put
    assert_equal 1, @table.put('row1', 'cf1:a' => 1, 'cf1:b' => 'a', 'cf1:c' => 3.14)

    # Batch put
    assert_equal 2, @table.put(
      'row2' => { 'cf1:a' => 2, 'cf1:b' => 'b', 'cf1:c' => 6.28 },
      'row3' => { 'cf1:a' => 4, 'cf1:b' => 'c', 'cf1:c' => 6.28 })

    # single-get
    assert_equal 'row1', @table.get('row1').rowkey
    assert_equal 1,      @table.get('row1').integer('cf1:a')
    assert_equal 'a',    @table.get('row1').string('cf1:b')
    assert_equal 3.14,   @table.get('row1').float('cf1:c')
    assert_equal nil,    @table.get('xxx')

    # multi-get
    assert_equal %w[row1 row2 row3], @table.get('row1', 'row2', 'row3').map { |r| r.rowkey }
    assert_equal [1, 2, 4         ], @table.get('row1', 'row2', 'row3').map { |r| r.integer('cf1:a') }
    assert_equal [3.14, 6.28, 6.28], @table.get('row1', 'row2', 'row3').map { |r| r.float('cf1:c') }
    assert_equal [nil, nil        ], @table.get('xxx', 'yyy')
  end

  def test_to_hash
    data = {
      'cf1:a' => 'Hello',
      'cf1:b' => 200,
      'cf1:c' => 3.14,
      'cf2:d' => { 'a' => 'b' }.to_json,
      'cf2:e' => false,
      'cf3:f' => 1234567890123456789012345678901234567890,
      'cf3'   => true
    }
    schema = {
      'cf1:a' => :string,
      'cf1:b' => :integer,
      'cf1:c' => :float,
      'cf2:d' => :json,
      'cf2:e' => :boolean,
      'cf3:f' => :biginteger,
      'cf3'   => :boolean
    }
    ret = data.dup.tap { |d| d['cf2:d'] = { 'a' => 'b' } }

    @table.put('row1', data)
    assert_equal ret, @table.get('row1').to_hash(schema)
    assert_equal 1,   @table.get('row1').to_hash_with_versions(schema)['cf1:a'].length

    # TODO Better testing for versioned values
    @table.put('row1', data)
    assert_equal ret, @table.get('row1').to_hash(schema)

    assert_equal 2, @table.get('row1').to_hash_with_versions(schema)['cf1:a'].length
    assert_equal 1, @table.get('row1').to_hash_with_versions(schema)['cf3:f'].length

    # get option: :versions
    assert_equal 1, @table.get('row1', :versions => 1).to_hash_with_versions(schema)['cf1:a'].length
  end

  def test_increment
    @table.put('row1', 'cf1:counter' => 1, 'cf1:counter2' => 100)
    assert_equal 1, @table.get('row1').fixnum('cf1:counter')

    @table.increment('row1', 'cf1:counter', 1)
    assert_equal 2, @table.get('row1').int('cf1:counter')

    @table.increment('row1', 'cf1:counter', 2)
    assert_equal 4, @table.get('row1').integer('cf1:counter')

    # Multi-column increment
    @table.increment('row1', 'cf1:counter' => 4, 'cf1:counter2' => 100)
    assert_equal 8,   @table.get('row1').integer('cf1:counter')
    assert_equal 200, @table.get('row1').integer('cf1:counter2')
  end

  def test_delete
    @table.put('row1', 'cf1:a' => 1, 'cf1:b' => 2, 'cf2:c' => 3, 'cf2:d' => 4)
    @table.put('row1', 'cf2:d' => 5)
    versions = @table.get('row1').to_hash_with_versions['cf2:d'].keys
    assert versions[0] > versions[1]

    # Deletes a version
    @table.delete('row1', 'cf2:d', versions[0])
    new_versions = @table.get('row1').to_hash_with_versions['cf2:d'].keys
    assert_equal new_versions, versions[1, 1]

    # Deletes a column
    assert_equal 3, @table.get('row1').integer('cf2:c')
    @table.delete('row1', 'cf2:c')
    assert_nil @table.get('row1').to_hash['cf2:c']

    # Deletes a column family
    assert_equal 1, @table.get('row1').integer('cf1:a')
    assert_equal 2, @table.get('row1').integer('cf1:b')
    @table.delete('row1', 'cf1') # FIXME What about column w/o qualifier?
    assert_nil @table.get('row1').to_hash['cf1:a']
    assert_nil @table.get('row1').to_hash['cf1:b']

    # Deletes a row
    @table.delete('row1')
    @table.get('row1') # TODO

    # Batch delete
    @table.put('row2', 'cf1:a' => 1)
    @table.put('row3', 'cf1:a' => 1, 'cf1:b' => 2)

    @table.delete ['row2'], ['row3', 'cf1:a']
    @table.get('row2') # TODO
    assert_nil @table.get('row3').to_hash['cf1:a']
    assert_equal 2, @table.get('row3').integer('cf1:b')
  end

  def test_table_non_string_rowkey
    @hbase.table(TABLE, :string_rowkey => false) do |table|
      rk = 'row1'.to_java_bytes
      table.put(rk, 'cf1:a' => 1)
      assert_equal Bytes.to_string(rk), Bytes.to_string(table.get(rk).rowkey)
      assert_equal 1,  table.get(rk).integer('cf1:a')
    end
  end
  
  def test_count
    (101..150).each do |i|
      @table.put(i, 'cf1:a' => i, 'cf2:b' => i, 'cf3:c' => i * 3)
    end

    assert_equal 50, @table.count
    assert_equal 50, @table.each.count
    assert_equal 50, @table.scan.count

    # Start key
    assert_equal 40,  @table.range(111).count

    # Stop key (exclusive)
    assert_equal 19,  @table.range(nil, 120).count

    # Start key ~ Stop key (exclusive)
    assert_equal  9,  @table.range(111, 120).count

    # Start key ~ Stop key (exclusive)
    assert_equal  9,  @table.range(111...120).count

    # Start key ~ Stop key (inclusive)
    assert_equal 10,  @table.range(111..120).count

    # Start key ~ Stop key (inclusive) + limit
    begin
      assert_equal 5, @table.range(111..120).limit(5).count
    rescue NotImplementedError
    end

    # Start key ~ Stop key (inclusive) + filters
    assert_equal 10,  @table.range(111..150).filter('cf1:a' => 131..140).count
    assert_equal 9,   @table.range(111..150).filter('cf1:a' => 131...140).count
    assert_equal 2,   @table.range(111..150).filter('cf1:a' => 131...140, 'cf2:b' => 132..133).count
  end

  def test_scan
    insert = lambda do
      (40..70).each do |i|
        @table.put(i, 'cf1:a' => i, 'cf2:b' => i * 2, 'cf3:c' => i * 3, 'cf3:d' => 'dummy', 'cf3:e' => 3.14)
      end
    end
    insert.call

    assert_instance_of HBase::Scoped, @table.each
    assert_instance_of HBase::Scoped, @table.scan

    # Test both for HBase::Table and HBase::Scoped
    [@table, @table.each].each do |table|
      # project
      project_cols = ['cf1:a', 'cf3:c']
      assert table.project(*project_cols).all? { |result|
        result.to_hash.keys == project_cols
      }

      # project: additive
      assert_equal project_cols + ['cf3:d'], table.project(*project_cols).project('cf3:d').first.to_hash.keys

      # filter: Hash
      #   to_a.length instead of count :)
      assert_equal 1, table.filter('cf1:a' => 50).to_a.length
      assert_equal 0, table.filter('cf1:a' => 50, 'cf2:b' => 60).to_a.length
      assert_equal 1, table.filter('cf1:a' => 50, 'cf2:b' => 100).to_a.length
      assert_equal 1, table.filter('cf1:a' => 50, 'cf2:b' => 90..100).to_a.length
      assert_equal 0, table.filter('cf1:a' => 50, 'cf2:b' => 90...100).to_a.length
      assert_equal 6, table.filter('cf1:a' => 50..60, 'cf2:b' => 100..110).to_a.length

      # filter: Hash + additive
      assert_equal 6, table.filter('cf1:a' => 50..60).filter('cf2:b' => 100..110).to_a.length

      # filter: Java filter
      # Bug: https://issues.apache.org/jira/browse/HBASE-6954
      import org.apache.hadoop.hbase.filter.ColumnPaginationFilter
      assert_equal 3, table.filter(ColumnPaginationFilter.new(3, 1)).first.to_hash.keys.length

      # filter: Java filter list TODO
      import org.apache.hadoop.hbase.filter.FilterList 
      import org.apache.hadoop.hbase.filter.ColumnRangeFilter
      assert_equal %w[cf2:b cf3:c],
          table.filter(FilterList.new [
             ColumnRangeFilter.new('a'.to_java_bytes, true, 'd'.to_java_bytes, true),
             ColumnPaginationFilter.new(2, 1),
          ]).first.to_hash.keys

      # limit with filter
      begin
        assert_equal 4, table.filter('cf1:a' => 50..60).filter('cf2:b' => 100..110).limit(4).to_a.length
      rescue NotImplementedError
      end

      # caching: How do we know if it's working? TODO
      assert_equal 6, table.filter('cf1:a' => 50..60).filter('cf2:b' => 100..110).caching(10).to_a.length
    end

    insert.call
    [@table, @table.each].each do |table|
      # versions
      assert table.all? { |result| result.to_hash_with_versions['cf1:a'].length == 2 }
      assert table.versions(1).all? { |result| result.to_hash_with_versions['cf1:a'].length == 1 }
    end
  end

  def test_inspect
    assert @table.inspect =~ /NAME => '#{TABLE}'/
  end
end
