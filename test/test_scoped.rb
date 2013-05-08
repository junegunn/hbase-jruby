#!/usr/bin/env ruby

$LOAD_PATH.unshift File.expand_path('..', __FILE__)
require 'helper'

class TestScoped < TestHBaseJRubyBase
  def test_invalid_limit
    assert_raise(ArgumentError) { @table.limit }
    assert_raise(ArgumentError) { @table.limit(-1) }
    assert_raise(ArgumentError) { @table.limit("hello") }
  end

  def test_invalid_versions
    assert_raise(ArgumentError) { @table.versions }
    assert_raise(ArgumentError) { @table.versions(0) }
    assert_raise(ArgumentError) { @table.versions("hello") }
  end

  def test_invalid_batch
    assert_raise(ArgumentError) { @table.batch }
    assert_raise(ArgumentError) { @table.batch(0) }
    assert_raise(ArgumentError) { @table.batch("hello") }
  end

  def test_invalid_range
    assert_raise(ArgumentError) { @table.range }
    assert_raise(ArgumentError) { @table.range(:xxx => 'row1') }
    assert_raise(ArgumentError) { @table.range(1, 2, 3) }
    assert_raise(ArgumentError) { @table.range(nil, nil) }
    assert_raise(ArgumentError) { @table.range(1..3, 4..5) }
  end

  def test_invalid_project
    assert_raise(ArgumentError) { @table.project(:offset => 'a', :limit => 10).to_a }
    assert_raise(ArgumentError) { @table.project(:offset => 10, :limit => 'a').to_a }

    @table.project(:offset => 100) # Fine yet
    @table.project(:limit => 10)
    assert_raise(ArgumentError) { @table.project(:offset => 100).to_a }
    assert_raise(ArgumentError) { @table.project(:limit  => 10).to_a }
    assert_raise(ArgumentError) { @table.project(:offset => -1) }
    assert_raise(ArgumentError) { @table.project(:limit  => -1) }
    assert_raise(ArgumentError) { @table.project(:offset => :a) }
    assert_raise(ArgumentError) { @table.project(:limit  => :a) }
    assert_raise(ArgumentError) { @table.project(:xxx    => 1) }
  end

  def test_invalid_filter
    assert_raise(ArgumentError) { @table.filter(3.14) }
    assert_raise(ArgumentError) { @table.filter('cf1:a' => { :xxx => 50 }) }
    assert_raise(ArgumentError) { @table.filter('cf1:a' => { :eq => { 1 => 2 } }) }
  end

  def test_each_and_count
    (101..150).each do |i|
      @table.put(i, 'cf1:a' => i, 'cf2:b' => i, 'cf3:c' => i * 3)
    end

    assert_instance_of HBase::Scoped, @table.each
    scoped = @table.each
    assert_equal scoped, scoped.each

    assert_equal 50, @table.count
    assert_equal 50, @table.each.count
    assert_equal 50, @table.to_a.length # each

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

    # Count with block
    assert_equal 5,  @table.range(111..150).filter('cf1:a' => 131..140).
                            count { |result| result.fixnum('cf1:a') % 2 == 0 }

    # Unscope
    assert_equal 50, @table.range(111..150).filter('cf1:a' => 131...140, 'cf2:b' => 132..133).unscope.count
  end

  def test_range_on_short_int
    (1..10).each do |i|
      @table.put({ :short => i }, 'cf1:a' => 'dummy')
    end

    assert_equal 5, @table.range({ :short => 6 }).count
    assert_equal 2, @table.range({ :short => 6 }, { :short => 8 }).count
    assert_equal 2, @table.range({ :short => 6 }, { :short => 8, :prefix => []}).count
    assert_equal 2, @table.range({ :short => 6 }, { :short => 8 }, :prefix => []).count
    assert_equal 3, @table.range(nil, { :short => 4 }).count
    assert_equal 3, @table.range(nil, { :short => 4, :prefix => [] }).count
    assert_equal 3, @table.range(nil, { :short => 4 }, :prefix => []).count
  end

  def test_filter_on_short_int
    @table.put(1, 'cf1:a' => { :long  => 100 })
    @table.put(2, 'cf1:a' => { :int   => 100 })
    @table.put(3, 'cf1:a' => { :short => 100 })
    @table.put(4, 'cf1:a' => { :byte  => 100 })
    @table.put(5, 'cf1:a' => { :byte  => 110 })

    assert_equal 1, @table.filter('cf1:a' => { :long  => 100 }).count
    assert_equal 1, @table.filter('cf1:a' => { :int   => 100 }).count
    assert_equal 1, @table.filter('cf1:a' => { :short => 100 }).count
    assert_equal 1, @table.filter('cf1:a' => { :byte  => 100 }).count

    assert_equal 1, @table.filter('cf1:a' => { :long  => 100 }).first.rowkey(:fixnum)
    assert_equal 2, @table.filter('cf1:a' => { :int   => 100 }).first.rowkey(:fixnum)
    assert_equal 3, @table.filter('cf1:a' => { :short => 100 }).first.rowkey(:fixnum)
    assert_equal 4, @table.filter('cf1:a' => { :byte  => 100 }).first.rowkey(:fixnum)

    assert_equal 5, @table.filter('cf1:a' => { :gt => { :byte => 100 } }).first.rowkey(:fixnum)
  end

  def test_filter_operator_and_short_int
    assert_raise(ArgumentError) {
      @table.filter('cf1:a' => { :long  => 100, :gt => 10 })
    }
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
      assert_equal 9,  table.filter('cf1:a' => { :> => 50,  :<= => 60, :ne => 55 }).to_a.length
      assert_equal 10, table.filter('cf1:a' => { :>= => 50, :<= => 60, :ne => 55 }).to_a.length
      assert_equal 9,  table.filter('cf1:a' => { :>= => 50, :< => 60,  :ne => 55 }).to_a.length
      assert_equal 1,  table.filter('cf1:a' => { :> => 50,  :<= => 60, :== => 55 }).to_a.length
      assert_equal 2,  table.filter('cf1:a' => { :> => 50,  :<= => 60, :== => [55, 57] }).to_a.length
      assert_equal 9,  table.filter('cf1:a' => { :gte => 50, :lt => 60, :ne => 55 }).to_a.length
      assert_equal 7,  table.filter('cf1:a' => { :gte => 50, :lt => 60, :ne => [55, 57, 59] }).to_a.length

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
    assert_equal [1, 2, 3, 4, 5, 6, 7, 8, 9], @table.range(1..9).map { |row| row.rowkey :fixnum }
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

    assert_equal [1, 2, 3], @table.range(1..3).map { |r| r.rowkey :fixnum }
    assert_equal %w[1 10 11 12 13 14 15 2 3], @table.range('1'..'3').map { |r| r.rowkey :string }
  end

  def test_non_string_column_name
    @table.put 'rowkey', Hash[ (1..20).map { |cq| [HBase::ColumnKey('cf1', cq), cq] } ]

    assert((1..20).all? { |cq| @table.get('rowkey').fixnum(HBase::ColumnKey('cf1', cq)) == cq })

    assert @table.project(['cf1', 10], ['cf1', 20]).map { |r|
      [r.fixnum(HBase::ColumnKey('cf1', 10)), r.fixnum(HBase::ColumnKey.new('cf1', 20))]
    }.all? { |e| e == [10, 20] }

    hash = @table.get('rowkey').to_hash(
      HBase::ColumnKey('cf1', 1) => :fixnum,
      HBase::ColumnKey('cf1', 2) => :fixnum
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
    assert           @table.range(:prefix => 'c').get('cc')
    assert_nil       @table.range(:prefix => 'c').get('dd')
    assert           @table.range(:prefix => ['d', 'c']).get('dd')
    assert_equal 52, @table.range(:prefix => ['a', 'c']).count
    assert_equal 78, @table.range(:prefix => ['d', 'a', 'c']).count
    assert_equal 52, @table.range(nil, 'd', :prefix => ['d', 'a', 'c']).count
    assert_equal 52, @table.range('b', :prefix => ['d', 'a', 'c']).count
    assert_equal 78, @table.range('a', 'e', :prefix => ['d', 'a', 'c']).count
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

  def test_while
    (0...100).each do |idx|
      @table.put idx, 'cf1:a' => idx % 10, 'cf2:b' => 'Hello'
    end

    assert_equal 20, @table.filter('cf1:a' => { :lte => 1 }, 'cf2:b' => 'Hello').count
    assert_equal 2,  @table.while( 'cf1:a' => { :lte => 1 }, 'cf2:b' => 'Hello').count

    # while == filter for gets
    assert_equal 20, @table.filter('cf1:a' => { :lte => 1 }, 'cf2:b' => 'Hello').get((0..100).to_a).compact.length
    assert_equal 20, @table.while( 'cf1:a' => { :lte => 1 }, 'cf2:b' => 'Hello').get((0..100).to_a).compact.length
  end

  def test_min_max
    (0...100).each do |idx|
      @table.put idx, 'cf1:a' => 1
      assert_equal 0,   @table.to_a.reverse.min.rowkey(:fixnum)
      assert_equal idx, @table.to_a.reverse.max.rowkey(:fixnum)
    end
  end

  def test_regex
    ('aa'..'zz').each do |rowkey|
      @table.put rowkey, 'cf1:a' => rowkey
    end

    assert_equal  1, @table.filter('cf1:a' => /gg/).count
    assert_equal  1, @table.filter('cf1:a' => /GG/i).count
    assert_equal 51, @table.filter('cf1:a' => /g/).count
    assert_equal  0, @table.filter('cf1:a' => /G/).count
    assert_equal 51, @table.filter('cf1:a' => /G/i).count
    assert_equal 26, @table.filter('cf1:a' => /g./).count
    assert_equal 26, @table.filter('cf1:a' => /^g/).count
    assert_equal 26, @table.filter('cf1:a' => /g$/).count
    assert_equal  2, @table.filter('cf1:a' => /gg|ff/).count
    assert_equal 28, @table.filter('cf1:a' => ['aa', 'cc', /^g/]).count
    assert_equal 54, @table.filter('cf1:a' => ['aa', 'cc', /^g/, { :gte => 'xa', :lt => 'y'}]).count
  end

  def test_java_bytes_prefix
    (1..100).each do |i|
      (1..100).each do |j|
        @table.put((HBase::ByteArray(i) + HBase::ByteArray(j)).to_java_bytes, 'cf1:a' => i * j)
      end
    end

    assert_equal 100, @table.range(:prefix => HBase::ByteArray(50)).count
    assert_equal 100, @table.range(:prefix => HBase::ByteArray(50).to_java_bytes).count
    assert_equal 200, @table.range(HBase::ByteArray(50), HBase::ByteArray(52)).count
    assert_equal 1,   @table.range(:prefix => (HBase::ByteArray(50) + HBase::ByteArray(50))).count

    assert_equal 2,   @table.range(:prefix => [
                                   (HBase::ByteArray(50) + HBase::ByteArray(50)).java,
                                   (HBase::ByteArray(50) + HBase::ByteArray(51)).java ]).count

    # Fails on 0.1.3
    assert_equal 1,   @table.range(:prefix => (HBase::ByteArray(50) + HBase::ByteArray(50)).java).count
  end

  def test_time_range_at
    t1, t2, t3, t4 =
      Time.now - 4000,
      Time.now - 3000,
      Time.now - 2000,
      Time.now - 1000
    @table.put :rowkey1 => { 'cf1:a' => { t1 => 1 } }
    @table.put :rowkey2 => { 'cf1:a' => { t2 => 2 } }
    @table.put :rowkey3 => { 'cf1:a' => { t3 => 3 } }
    @table.put :rowkey4 => { 'cf1:a' => { t4 => 4 } }

    assert_equal 4, @table.count

    assert_equal 1, @table.time_range(t2, t3).count
    assert_equal 2, @table.time_range(t2, t3 + 1).count
    assert_equal 2, @table.time_range(t2, t4).count
    assert_equal 4, @table.time_range(0, t4 + 1).count

    assert_equal [2, 3], @table.time_range(t2, t4).map { |r| r.fixnum 'cf1:a' }.sort
    assert_equal %w[rowkey2 rowkey3], @table.time_range(t2, t4).map { |r| r.rowkey :string }.sort

    assert_equal 1,   @table.at(t2).count
    assert_equal 0,   @table.at(t2 - 1).count
    assert_equal 0,   @table.at(t2 + 1).count
    assert_equal [2], @table.at(t2).map { |r| r.fixnum 'cf1:a' }

    @table.put :rowkey5 => { 'cf1:a' => { t1 => 'a', t4 => 'A' }, 'cf1:b' => { t4 => 'B' }}
    assert_equal 'A', @table.get(:rowkey5).string('cf1:a')
    assert_equal 'B', @table.get(:rowkey5).string('cf1:b')

    assert_equal 'a', @table.time_range(t1, t3).get(:rowkey5).string('cf1:a')
    assert_equal nil, @table.time_range(t1, t3).get(:rowkey5).string('cf1:b')

    # according to current hbase impl, later call overrides the previous time ranges. but, why do this?
    assert_equal 2, @table.time_range(t2, t3).at(t1).count
  end

  def test_with_java_scan
    ('a'..'z').each do |rk|
      @table.put rk, 'cf1:a' => 1
    end

    assert_equal 2, @table.with_java_scan { |scan|
      scan.setStartRow HBase::Util.to_bytes 'a'
      scan.setStopRow HBase::Util.to_bytes 'd'
    }.with_java_scan { |scan|
      scan.setStartRow HBase::Util.to_bytes 'b'
    }.count
  end

  def test_with_java_get
    t1, t2, t3, t4 =
      Time.now - 4000,
      Time.now - 3000,
      Time.now - 2000,
      Time.now - 1000
    @table.put :r1 => { 'cf1:a' => { t1 => 1 } }
    @table.put :r2 => { 'cf1:a' => { t2 => 2 } }
    @table.put :r3 => { 'cf1:a' => { t3 => 3 } }
    @table.put :r4 => { 'cf1:a' => { t4 => 4 } }

    assert_equal 4, @table.count

    rks = [:r1, :r2, :r3, :r4]
    assert_equal 4, @table.get(rks).compact.count

    scoped = @table.with_java_get { |get|
      get.setTimeRange(t1.to_i * 1000, t4.to_i * 1000)
    }
    assert_equal 3, scoped.get(rks).compact.count
    assert_equal 2, scoped.with_java_get { |get|
      get.setTimeRange(t2.to_i * 1000, t4.to_i * 1000)
    }.get(rks).compact.count
  end
end

