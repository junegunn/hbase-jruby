#!/usr/bin/env ruby

$LOAD_PATH.unshift File.expand_path('..', __FILE__)
require 'helper'

class TestTable < TestHBaseJRubyBase 
  def test_table
    @hbase.table(TABLE) do |table|
      assert_equal TABLE, table.name
      assert table.exists?
    end

    table = @hbase.table(TABLE)
    2.times do
      assert_equal TABLE, table.name
      assert table.exists?
      table.close

      # Gets another HTable instance from HTablePool
      table.put('rowkey' => { 'cf1:a' => 1 })
    end
  end

  def test_put_then_get
    bignum = 123456789123456789123456789
    # Single record put
    assert_equal 1, @table.put('row1',
                               'cf1:a' => 2,
                               'cf1:b' => 'b',
                               'cf1:c' => 6.28,
                               'cf1:d' => false,
                               'cf1:e' => bignum + 1,
                               'cf1:f' => :bol,
                               'cf1:g' => BigDecimal.new("456.123"),
                               'cf1:str1' => "Goodbye", 'cf1:str2' => "Cruel world")
    assert_equal 1, @table.put('row1',
                               'cf1:a' => 1,
                               'cf1:b' => 'a',
                               'cf1:c' => 3.14,
                               'cf1:d' => true,
                               'cf1:e' => bignum,
                               'cf1:f' => :sym,
                               'cf1:g' => BigDecimal.new("123.456"),
                               'cf1:str1' => "Hello", 'cf1:str2' => "World")
    # Batch put
    assert_equal 2, @table.put(
      'row2' => { 'cf1:a' => 2, 'cf1:b' => 'b', 'cf1:c' => 6.28 },
      'row3' => { 'cf1:a' => 4, 'cf1:b' => 'c', 'cf1:c' => 6.28 })

    # single-get (latest version)
    assert_equal 'row1', @table.get('row1').rowkey
    assert_equal 1,      @table.get('row1').fixnum('cf1:a')
    assert_equal 'a',    @table.get('row1').string('cf1:b')
    assert_equal 'a',    String.from_java_bytes(@table.get('row1').raw('cf1:b'))
    assert_equal 3.14,   @table.get('row1').float('cf1:c')
    assert_equal true,   @table.get('row1').boolean('cf1:d')
    assert_equal bignum, @table.get('row1').bignum('cf1:e')
    assert_equal :sym,   @table.get('row1').symbol('cf1:f')
    assert_equal BigDecimal.new("123.456"), @table.get('row1').bigdecimal('cf1:g')

    # single-get-multi-col
    assert_equal %w[Hello World], @table.get('row1').string(['cf1:str1', 'cf1:str2'])

    # single-get-multi-ver
    assert_equal [1, 2],               @table.get('row1').fixnums('cf1:a').values
    assert_equal %w[a b],              @table.get('row1').strings('cf1:b').values
    assert_equal %w[a b],              @table.get('row1').raws('cf1:b').values.map { |v| String.from_java_bytes v }
    assert_equal [3.14, 6.28],         @table.get('row1').floats('cf1:c').values
    assert_equal [true, false],        @table.get('row1').booleans('cf1:d').values
    assert_equal [bignum, bignum + 1], @table.get('row1').bignums('cf1:e').values
    assert_equal [:sym, :bol],         @table.get('row1').symbols('cf1:f').values
    assert_equal [
      BigDecimal.new("123.456"),
      BigDecimal.new("456.123")], @table.get('row1').bigdecimals('cf1:g').values

    assert @table.get('row1').fixnums('cf1:a').keys.all? { |k| k.instance_of? Fixnum }

    # single-get-multi-col-multi=ver
    ret = @table.get('row1').strings(['cf1:str1', 'cf1:str2'])
    assert_equal ['Hello', 'World'], ret.map(&:values).map(&:first)
    assert_equal ['Goodbye', 'Cruel world'], ret.map(&:values).map(&:last)

    # multi-get
    assert_equal %w[row1 row2 row3], @table.get(['row1', 'row2', 'row3']).map { |r| r.rowkey }
    assert_equal [1, 2, 4         ], @table.get(['row1', 'row2', 'row3']).map { |r| r.integer('cf1:a') }
    assert_equal [3.14, 6.28, 6.28], @table.get(['row1', 'row2', 'row3']).map { |r| r.float('cf1:c') }
    assert_equal [nil, nil        ], @table.get(['xxx', 'yyy'])

    # Unavailable columns
    assert_equal nil,    @table.get('row1').symbol('cf1:xxx')
    assert_equal nil,    @table.get('row1').integer('cf1:xxx')

    # Row not found
    assert_equal nil,    @table.get('xxx')
  end

  def test_to_hash
    data = {
      'cf1:a' => 'Hello',
      'cf1:b' => 200,
      'cf1:c' => 3.14,
      'cf2:d' => :world,
      'cf2:e' => false,
      'cf3:f' => 1234567890123456789012345678901234567890,
      'cf3'   => true
    }
    schema = {
      'cf1:a' => :string,
      'cf1:b' => :integer,
      'cf1:c' => :float,
      'cf2:d' => :symbol,
      'cf2:e' => :boolean,
      'cf3:f' => :biginteger,
      'cf3'   => :boolean
    }
    @table.put('row1', data)
    @table.put('row2', 'cf1:a' => 'Goodbye')

    assert_equal data, @table.get('row1').to_hash(schema).map { |k, v| { k.to_s => v } }.inject(:merge)
    assert_equal 1,    @table.get('row1').to_hash_with_versions(schema)['cf1:a'].length
    assert_equal 1,    @table.get('row1').to_hash_with_versions(schema)[HBase::ColumnKey(:cf1, :a)].length

    # Better testing for versioned values
    @table.put('row1', data)
    assert_equal data, @table.get('row1').to_hash(schema).map { |k, v| { k.to_s => v } }.inject(:merge)

    assert_equal 2, @table.get('row1').to_hash_with_versions(schema)['cf1:a'].length
    assert_equal 1, @table.get('row1').to_hash_with_versions(schema)['cf3:f'].length

    # get option: :versions
    assert_equal 1, @table.versions(1).get('row1').to_hash_with_versions(schema)['cf1:a'].length

    # scoped get with filters
    assert_equal 2, @table.get(['row1', 'row2']).count
    assert_equal 1, @table.filter('cf1:a' => 'Hello').get(['row1', 'row2']).compact.count

    # scoped get with projection
    assert_equal %w[cf3 cf3:f], @table.project('cf3').get('row1').to_hash.keys.map(&:to_s)
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
    @table.put('row1', 'cf1' => 0, 'cf1:a' => 1, 'cf1:b' => 2, 'cf2:c' => 3, 'cf2:d' => 4)
    sleep 0.1
    @table.put('row1', 'cf2:d' => 5)
    sleep 0.1
    @table.put('row1', 'cf2:d' => 6)
    versions = @table.get('row1').to_hash_with_versions['cf2:d'].keys
    assert versions[0] > versions[1]
    assert versions[1] > versions[2]

    # Deletes a version
    @table.delete('row1', 'cf2:d', versions[0], versions[2])
    new_versions = @table.get('row1').to_hash_with_versions['cf2:d'].keys
    assert_equal new_versions, [versions[1]]

    # Deletes a column
    assert_equal 3, @table.get('row1').integer('cf2:c')
    @table.delete('row1', 'cf2:c')
    assert_nil @table.get('row1').to_hash['cf2:c']

    # Deletes a column with empty qualifier
    assert_equal 0, @table.get('row1').integer('cf1')
    @table.delete('row1', 'cf1:')
    assert_equal 1, @table.get('row1').integer('cf1:a')
    assert_equal 2, @table.get('row1').integer('cf1:b')
    assert_nil @table.get('row1').to_hash['cf1']
    assert_nil @table.get('row1').to_hash['cf1:']

    # Deletes a column family
    assert_equal 1, @table.get('row1').integer('cf1:a')
    assert_equal 2, @table.get('row1').integer('cf1:b')
    @table.delete('row1', 'cf1') # No trailing colon
    assert_nil @table.get('row1').to_hash['cf1:a']
    assert_nil @table.get('row1').to_hash['cf1:b']

    # Deletes a row
    @table.delete('row1')
    assert_nil @table.get('row1')

    # Batch delete
    @table.put('row2', 'cf1:a' => 1)
    @table.put('row3', 'cf1:a' => 1, 'cf1:b' => 2)

    @table.delete ['row2'], ['row3', 'cf1:a']
    assert_nil @table.get('row2')
    assert_nil @table.get('row3').to_hash['cf1:a']
    assert_equal 2, @table.get('row3').integer('cf1:b')
  end

  def test_count
    (101..150).each do |i|
      @table.put(i, 'cf1:a' => i, 'cf2:b' => i, 'cf3:c' => i * 3)
    end

    assert_equal 50, @table.count
    assert_instance_of HBase::Scoped, @table.each
    assert_equal 50, @table.each.count

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

    # Unscope
    assert_equal 50, @table.range(111..150).filter('cf1:a' => 131...140, 'cf2:b' => 132..133).unscope.count
  end

  def test_invalid_range
    assert_raise(ArgumentError) { @table.range }
    assert_raise(ArgumentError) { @table.range(1, 2, 3) }
  end

  def test_scan
    insert = lambda do
      (40..70).each do |i|
        @table.put(i, 'cf1:a' => i, 'cf2:b' => i * 2, 'cf3:c' => i * 3, 'cf3:d' => 'dummy', 'cf3:e' => 3.14)
      end
    end
    insert.call

    assert_instance_of HBase::Scoped, @table.each

    # Test both for HBase::Table and HBase::Scoped
    [@table, @table.each].each do |table|
      # project
      project_cols = ['cf1:a', 'cf3:c']
      assert table.project(*project_cols).all? { |result|
        result.to_hash.keys == project_cols
      }

      # project: additive
      assert_equal project_cols + ['cf3:d'], table.project(*project_cols).project('cf3:d').first.to_hash.keys.map(&:to_s)

      # project: family
      assert_equal %w[cf1:a cf3:c cf3:d cf3:e], table.project('cf1:a', 'cf3').first.to_hash.keys.map(&:to_s)

      # filter: Hash
      #   to_a.length instead of count :)
      assert_equal 1,  table.filter('cf1:a' => 50).to_a.length
      assert_equal 3,  table.filter('cf1:a' => [50, 60, 70]).to_a.length
      assert_equal 2,  table.filter('cf1:a' => [50, 60, 70], 'cf2:b' => [100, 140]).to_a.length
      assert_equal 20, table.filter('cf1:a' => [41..50, 55, 61...70]).to_a.length
      assert_equal 12, table.filter('cf1:a' => [41..50, 61, 70]).to_a.length
      assert_equal 0,  table.filter('cf1:a' => 50, 'cf2:b' => 60).to_a.length
      assert_equal 1,  table.filter('cf1:a' => 50, 'cf2:b' => 90..100).to_a.length
      assert_equal 0,  table.filter('cf1:a' => 50, 'cf2:b' => 90...100).to_a.length
      assert_equal 6,  table.filter('cf1:a' => 50..60, 'cf2:b' => 100..110).to_a.length
      assert_equal 10, table.filter('cf1:a' => { :> => 50,  :<= => 60 }).to_a.length
      assert_equal 9,  table.filter('cf1:a' => { :> => 50,  :<= => 60, :!= => 55 }).to_a.length
      assert_equal 10, table.filter('cf1:a' => { :>= => 50, :<= => 60, :!= => 55 }).to_a.length
      assert_equal 9,  table.filter('cf1:a' => { :>= => 50, :< => 60,  :!= => 55 }).to_a.length
      assert_equal 1,  table.filter('cf1:a' => { :> => 50,  :<= => 60, :== => 55 }).to_a.length
      assert_equal 2,  table.filter('cf1:a' => { :> => 50,  :<= => 60, :== => [55, 57] }).to_a.length
      assert_equal 9,  table.filter('cf1:a' => { gte: 50, lt: 60, ne: 55 }).to_a.length
      assert_equal 7,  table.filter('cf1:a' => { gte: 50, lt: 60, ne: [55, 57, 59] }).to_a.length

      assert_raise(ArgumentError) { table.filter('cf1:a' => { xxx: 50 }) }
      assert_raise(ArgumentError) { table.filter('cf1:a' => { eq: { 1 => 2 } }) }

      # filter: Hash + additive
      assert_equal 6, table.filter('cf1:a' => 50..60).filter('cf2:b' => 100..110).to_a.length

      # filter: Java filter
      # Bug: https://issues.apache.org/jira/browse/HBASE-6954
      import org.apache.hadoop.hbase.filter.ColumnPaginationFilter
      assert_equal 3, table.filter(ColumnPaginationFilter.new(3, 1)).first.to_hash.keys.length

      # filter: Java filter list
      import org.apache.hadoop.hbase.filter.FilterList 
      import org.apache.hadoop.hbase.filter.ColumnRangeFilter
      assert_equal %w[cf2:b cf3:c],
          table.filter(FilterList.new [
             ColumnRangeFilter.new('a'.to_java_bytes, true, 'd'.to_java_bytes, true),
             ColumnPaginationFilter.new(2, 1),
          ]).first.to_hash.keys.map(&:to_s)

      # filter: invalid filter type
      assert_raise(ArgumentError) {
        table.filter(3.14)
      }

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

  def test_scan_on_non_string_rowkey
    (1..20).each do |rk|
      @table.put rk, 'cf1:a' => rk
    end
    assert_equal 9, @table.range(1..9).count
    assert_equal [1, 2, 3, 4, 5, 6, 7, 8, 9], @table.range(1..9).map { |row| row.rowkey :integer }
    assert_equal 8, @table.range(1...9).count

    @table.truncate!

    (1..20).each do |rk|
      @table.put rk.to_s, 'cf1:a' => rk
    end
    assert_equal 20, @table.range('1'..'9').count
    assert_equal %w[1 10 11 12 13 14 15 16 17 18 19 2 20 3 4 5 6 7 8 9], @table.range('1'..'9').map(&:rowkey)

    assert_equal 19, @table.range('1'...'9').count

    @table.truncate!
    data = { 'cf1:1' => 1 } # doesn't matter
    (1..15).each do |i|
      @table.put i, data
      @table.put i.to_s, data
    end

    assert_equal [1, 2, 3], @table.range(1..3).map { |r| r.rowkey :integer }
    assert_equal %w[1 10 11 12 13 14 15 2 3], @table.range('1'..'3').map { |r| r.rowkey :string }
  end

  def test_non_string_column_name
    @table.put 'rowkey', Hash[ (1..20).map { |cq| [HBase::ColumnKey('cf1', cq), cq] } ]

    assert((1..20).all? { |cq| @table.get('rowkey').integer(HBase::ColumnKey('cf1', cq)) == cq })

    assert @table.project(['cf1', 10], ['cf1', 20]).map { |r|
      [r.integer(HBase::ColumnKey('cf1', 10)), r.integer(HBase::ColumnKey.new('cf1', 20))]
    }.all? { |e| e == [10, 20] }

    hash = @table.get('rowkey').to_hash(
      HBase::ColumnKey('cf1', 1) => :fixnum,
      HBase::ColumnKey('cf1', 2) => :fixnum,
    )
    assert_equal 1, hash[HBase::ColumnKey(:cf1, 1)]
    assert_equal 2, hash[HBase::ColumnKey(:cf1, 2)]
    assert_equal 3, HBase::Util.from_bytes(:fixnum, hash[HBase::ColumnKey(:cf1, 3)])
  end

  def test_table_descriptor
    assert_instance_of org.apache.hadoop.hbase.client.UnmodifyableHTableDescriptor, @table.descriptor

    # Should be read-only
    assert_raise {
      @table.descriptor.setMaxFileSize 100 * 1024 ** 2
    }
  end

  def test_null_value
    10.times do |i|
      @table.put i, 'cf1:nil' => i % 2 == 0 ? nil : true
    end
    assert_equal 10, @table.count
    assert_equal 5, @table.filter('cf1:nil' => nil).count
  end

  def test_scoped_get_intra_row_scan
    # Preparation
    all_data = {}
    (1..100).each do |rk|
      data = {}
      (1..200).each do |cq|
        data[HBase::ColumnKey(:cf1, cq)] = rk + cq
      end
      all_data[rk] = data
    end
    @table.put all_data

    # One simple filter (Rowkey 10 ~ 19)
    scoped1 = @table.filter(HBase::ColumnKey('cf1', 100) => 110...120)
    ret = scoped1.get((1..100).to_a)
    assert_equal 100, ret.count
    assert_equal 10, ret.compact.count

    # Two filters
    scoped2 = scoped1.filter(
      # Rowkey 10 ~ 19 & 9 ~ 14 = 10 ~ 14
      HBase::ColumnKey('cf1', 1) => 10..15
    )
    ret = scoped2.get((1..100).to_a)
    assert_equal 100, ret.count
    assert_equal 5, ret.compact.count

    # Range
    assert_equal 4, scoped2.range(11).get((1..100).to_a).compact.count
    assert_equal 3, scoped2.range(11..13).get((1..100).to_a).compact.count
    assert_equal 2, scoped2.range(11...13).get((1..100).to_a).compact.count
    assert_equal 2, scoped2.range(11, 13).get((1..100).to_a).compact.count
    assert_equal 3, scoped2.range(nil, 13).get((1..100).to_a).compact.count
  end

  def test_prefix_filter
    ('aa'..'zz').each do |rk|
      @table.put rk, 'cf1:a' => 1
    end

    assert_equal 26, @table.range(:prefix => 'c').count
    assert_equal 52, @table.range(:prefix => ['a', 'c']).count
    assert_equal 52, @table.range(nil, 'd', :prefix => ['a', 'c', 'd']).count
    assert_equal 52, @table.range('b', :prefix => ['a', 'c', 'd']).count
    assert_equal 78, @table.range('a', 'e', :prefix => ['a', 'c', 'd']).count
  end

  def test_advanced_projection
    @table.put :rk, Hash[ ('aa'..'zz').map { |cq| [ "cf1:#{cq}", 100 ] } ]

    assert_equal 26,   @table.project(:prefix => 'd').first.count
    assert_equal 52,   @table.project(:prefix => ['d', 'f']).first.count
    assert_equal 52,   @table.project(:range => 'b'...'d').first.count
    assert_equal 105,  @table.project(:range => ['b'...'d', 'x'..'za']).first.count
    assert_equal 10,   @table.project(:offset => 10, :limit => 10).first.count
    assert_equal 'da', @table.project(:offset => 26 * 3, :limit => 10).first.first.cq
    assert_equal 10,   @table.project(:offset => 26 * 3).project(:limit => 10).first.count
    assert_equal 'da', @table.project(:offset => 26 * 3).project(:limit => 10).first.first.cq

    assert_raise(ArgumentError) { @table.project(:offset => 'a', :limit => 10).to_a }
    assert_raise(ArgumentError) { @table.project(:offset => 10, :limit => 'a').to_a }
    assert_raise(ArgumentError) { @table.project(:offset => 100).to_a }
    assert_raise(ArgumentError) { @table.project(:limit => 10).to_a }
    assert_raise(ArgumentError) { @table.project(:offset => 10, :limit => 10).project(:limit => 10).to_a }
    assert_raise(ArgumentError) { @table.project(:offset => 10, :limit => 10).project(:offset => 10).to_a }
  end

  def test_batch
    @table.put :rk, Hash[ ('aa'..'zz').map { |cq| [ "cf1:#{cq}", 100 ] } ]

    assert_equal [10, 10, 6], @table.batch(10).project(:prefix => 'd').map(&:count)

    # # README example
    # (1..100).each do |rk|
    #   @table.put rk, Hash[ ('aa'..'zz').map { |cq| [ "cf1:#{cq}", 100 ] } ]
    # end
    # scoped = @table.each
    # scoped.range(1..100).
    #        project(:prefix => 'c').
    #        batch(10).
    #        map { |row| [row.rowkey(:fixnum), row.count].map(&:to_s).join ': ' }
  end
end

