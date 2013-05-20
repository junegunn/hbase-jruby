#!/usr/bin/env ruby

$LOAD_PATH.unshift File.expand_path('..', __FILE__)
require 'helper'
require 'bigdecimal'

class TestTable < TestHBaseJRubyBase
  def test_table_block
    @hbase.table(TABLE) do |table|
      assert_equal TABLE, table.name
      assert table.exists?
    end
  end

  def test_table_htable
    htables = []
    table = @hbase.table(TABLE)
    4.times do
      htables << table.htable
    end
    assert_equal 1, htables.uniq.length

    # Multi-threaded
    htables = []
    table = @hbase.table(TABLE)
    4.times do
      Thread.new {
        htables << table.htable
      }.join
    end
    assert_equal 4, htables.uniq.length

    assert_equal @hbase.table(TABLE).htable, @hbase[TABLE].htable
  end

  def test_put_then_get
    # Single record put
    assert_equal 1, @table.put('row1',
                               'cf1:a'     => 2,
                               'cf1:b'     => 'b',
                               'cf1:c'     => 6.28,
                               'cf1:d'     => false,
                               'cf1:f'     => :bol,
                               'cf1:g'     => BigDecimal.new("456.123"),
                               'cf1:byte'  => { :byte  => 100 },
                               'cf1:short' => { :short => 200 },
                               'cf1:int'   => { :int   => 300 },
                               'cf1:str1'  => "Goodbye", 'cf1:str2' => "Cruel world")
    assert_equal 1, @table.put('row1',
                               'cf1:a'     => 1,
                               'cf1:b'     => 'a',
                               'cf1:c'     => 3.14,
                               'cf1:d'     => true,
                               'cf1:f'     => :sym,
                               'cf1:g'     => BigDecimal.new("123.456"),
                               'cf1:byte'  => { :byte  => 101 },
                               'cf1:short' => { :short => 201 },
                               'cf1:int'   => { :int   => 301 },
                               'cf1:int2'  => { :int   => 401 },
                               'cf1:str1'  => "Hello", 'cf1:str2' => "World")
    # Batch put
    assert_equal 2, @table.put(
      'row2' => { 'cf1:a' => 2, 'cf1:b' => 'b', 'cf1:c' => 6.28 },
      'row3' => { 'cf1:a' => 4, 'cf1:b' => 'c', 'cf1:c' => 6.28 })

    # single-get (latest version)
    result = @table.get('row1')
    # Test enumerator
    assert_equal result.to_a, result.each.each.to_a
    assert_equal result.to_a, result.each.take_while { true }.to_a

    assert_equal 'row1', @table.get('row1').rowkey
    assert_equal 'row1', @table.get('row1').rowkey
    assert_equal 1,      @table.get('row1').fixnum('cf1:a')
    assert_equal 'a',    @table.get('row1').string('cf1:b')
    assert_equal 'a',    String.from_java_bytes(@table.get('row1').raw('cf1:b'))
    assert_equal 3.14,   @table.get('row1').float('cf1:c')
    assert_equal true,   @table.get('row1').boolean('cf1:d')
    assert_equal :sym,   @table.get('row1').symbol('cf1:f')
    assert_equal BigDecimal.new("123.456"), @table.get('row1').bigdecimal('cf1:g')
    assert_equal 101,   @table.get('row1').byte('cf1:byte')
    assert_equal 201,   @table.get('row1').short('cf1:short')
    assert_equal 301,   @table.get('row1').int('cf1:int')

    # single-get-multi-col (deprecated since 0.3)
    # assert_equal %w[Hello World], @table.get('row1').string(['cf1:str1', 'cf1:str2'])
    # assert_equal [301, 401], @table.get('row1').int(['cf1:int', 'cf1:int2'])

    # single-get-multi-ver
    assert_equal [1, 2],        @table.get('row1').fixnums('cf1:a').values
    assert_equal %w[a b],       @table.get('row1').strings('cf1:b').values
    assert_equal %w[a b],       @table.get('row1').raws('cf1:b').values.map { |v| String.from_java_bytes v }
    assert_equal [3.14, 6.28],  @table.get('row1').floats('cf1:c').values
    assert_equal [true, false], @table.get('row1').booleans('cf1:d').values
    assert_equal [:sym, :bol],  @table.get('row1').symbols('cf1:f').values
    assert_equal [
      BigDecimal.new("123.456"),
      BigDecimal.new("456.123")], @table.get('row1').bigdecimals('cf1:g').values
    assert_equal [101, 100], @table.get('row1').bytes('cf1:byte').values
    assert_equal [201, 200], @table.get('row1').shorts('cf1:short').values
    assert_equal [301, 300], @table.get('row1').ints('cf1:int').values

    assert @table.get('row1').fixnums('cf1:a').keys.all? { |k| k.instance_of? Fixnum }

    # single-get-multi-col-multi=ver (deprecated since 0.3)
    # rets = @table.get('row1').strings(['cf1:str1', 'cf1:str2'])
    # assert_equal ['Hello', 'World'], rets.map(&:values).map(&:first)
    # assert_equal ['Goodbye', 'Cruel world'], rets.map(&:values).map(&:last)

    # multi-get
    assert_equal %w[row1 row2 row3], @table.get(['row1', 'row2', 'row3']).map { |r| r.rowkey }
    assert_equal [1, 2, 4         ], @table.get(['row1', 'row2', 'row3']).map { |r| r.fixnum('cf1:a') }
    assert_equal [3.14, 6.28, 6.28], @table.get(['row1', 'row2', 'row3']).map { |r| r.float('cf1:c') }
    assert_equal [nil, nil        ], @table.get(['xxx', 'yyy'])

    # Unavailable columns
    assert_equal nil, @table.get('row1').symbol('cf1:xxx')
    assert_equal nil, @table.get('row1').fixnum('cf1:xxx')

    # Unavailable columns (plural form)
    assert_equal({}, @table.get('row1').strings('cf1:xxx'))
    assert_equal({}, @table.get('row1').strings('cfx:xxx'))

    # Row not found
    assert_equal nil, @table.get('xxx')
  end

  # Put added after a delete is overshadowed if its timestamp is older than than that of the tombstone
  # https://issues.apache.org/jira/browse/HBASE-2847
  def test_put_delete_put
    pend("https://issues.apache.org/jira/browse/HBASE-2847") do
      data = { 'cf1:pdp' => { 1250000000000 => 'A1' } }
      @table.put :rowkey => data
      assert_equal 'A1', @table.get(:rowkey).string('cf1:pdp')
      @table.delete :rowkey
      assert_nil @table.get(:rowkey)
      @table.put :rowkey => data
      assert_equal 'A1', @table.get(:rowkey).string('cf1:pdp')
    end
  end

  def test_put_timestamp
    rowkey = :test_put_timestamp
    @table.put rowkey => {
      'cf1:b' => 'B1',
      'cf1:a' => {
        1250000000000 => 'A1',
        1260000000000 => 'A2',
        Time.at(1270000000) => 'A3', # Ruby Time support
      },
    }

    assert_equal [1270000000000, 'A3'], @table.get(rowkey).strings('cf1:a').first
    assert_equal 'A2', @table.get(rowkey).strings('cf1:a')[1260000000000]
    assert_equal [1250000000000, 'A1'], @table.get(rowkey).strings('cf1:a').to_a.last
    assert_equal ['B1'], @table.get(rowkey).strings('cf1:b').values
  end

  def test_increment
    @table.put('row1', 'cf1:counter' => 1, 'cf1:counter2' => 100)
    assert_equal 1, @table.get('row1').fixnum('cf1:counter')

    @table.increment('row1', 'cf1:counter', 1)
    assert_equal 2, @table.get('row1').fixnum('cf1:counter')

    @table.increment('row1', 'cf1:counter', 2)
    assert_equal 4, @table.get('row1').fixnum('cf1:counter')

    # Multi-column increment
    @table.increment('row1', 'cf1:counter' => 4, 'cf1:counter2' => 100)
    assert_equal 8,   @table.get('row1').fixnum('cf1:counter')
    assert_equal 200, @table.get('row1').fixnum('cf1:counter2')

    # Multi-row multi-column increment
    @table.put('row2', 'cf1:counter' => 1, 'cf1:counter2' => 100)
    @table.increment 'row1' => { 'cf1:counter' => 4, 'cf1:counter2' => 100 },
                     'row2' => { 'cf1:counter' => 1, 'cf1:counter2' => 100 }
    assert_equal 12,  @table.get('row1').fixnum('cf1:counter')
    assert_equal 300, @table.get('row1').fixnum('cf1:counter2')
    assert_equal 2,   @table.get('row2').fixnum('cf1:counter')
    assert_equal 200, @table.get('row2').fixnum('cf1:counter2')
  end

  def test_delete
    @table.put('row1', 'cf1' => 0, 'cf1:a' => 1, 'cf1:b' => 2, 'cf2:c' => 3, 'cf2:d' => 4)
    sleep 0.1
    @table.put('row1', 'cf2:d' => 5)
    sleep 0.1
    @table.put('row1', 'cf2:d' => 6)
    versions = @table.get('row1').to_H[%w[cf2 d]].keys
    assert versions[0] > versions[1]
    assert versions[1] > versions[2]

    # Deletes a version (Fixnum and Time as timestamps)
    @table.delete('row1', 'cf2:d', versions[0], Time.at(versions[2] / 1000.0))
    new_versions = @table.get('row1').to_H[%w[cf2 d]].keys
    assert_equal new_versions, [versions[1]]

    # Deletes a column
    assert_equal 3, @table.get('row1').fixnum('cf2:c')
    @table.delete('row1', 'cf2:c')
    assert_nil @table.get('row1').to_h['cf2:c']

    # Deletes a column with empty qualifier
    assert_equal 0, @table.get('row1').fixnum('cf1')
    @table.delete('row1', 'cf1:')
    assert_equal 1, @table.get('row1').fixnum('cf1:a')
    assert_equal 2, @table.get('row1').fixnum('cf1:b')
    assert_nil @table.get('row1').to_h['cf1']
    assert_nil @table.get('row1').to_h['cf1:']

    # Deletes a column family
    assert_equal 1, @table.get('row1').fixnum('cf1:a')
    assert_equal 2, @table.get('row1').fixnum('cf1:b')
    @table.delete('row1', 'cf1') # No trailing colon
    assert_nil @table.get('row1').to_h['cf1:a']
    assert_nil @table.get('row1').to_h['cf1:b']

    # Deletes a row
    @table.delete('row1')
    assert_nil @table.get('row1')

    # Batch delete
    @table.put('row2', 'cf1:a' => 1)
    @table.put('row3', 'cf1:a' => 1, 'cf1:b' => 2)

    @table.delete ['row2'], ['row3', 'cf1:a']
    assert_nil @table.get('row2')
    assert_nil @table.get('row3').to_h['cf1:a']
    assert_equal 2, @table.get('row3').fixnum('cf1:b')
  end

  def test_delete_row
    @table.put(1 => { 'cf1:a' => 1 }, 's' => { 'cf1:a' => 2 }, { :short => 3 } => { 'cf1:a' => 3 })

    assert_equal 1, @table.get(1).fixnum('cf1:a')
    assert_equal 2, @table.get('s').fixnum('cf1:a')
    assert_equal 3, @table.get({ :short => 3 }).fixnum('cf1:a')
    assert_equal 3, @table.count

    @table.delete_row 1, { :short => 3 }

    assert_equal nil, @table.get(1)
    assert_equal 2,   @table.get('s').fixnum('cf1:a')
    assert_equal nil, @table.get({ :short => 3 })
    assert_equal 1,   @table.count
  end
end

