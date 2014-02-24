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
    num_registered_threads = @hbase.instance_variable_get(:@threads).length
    4.times do
      Thread.new {
        htables << table.htable
        assert_equal num_registered_threads + 1,
                     @hbase.instance_variable_get(:@threads).length
      }.join
    end
    assert_equal 4, htables.uniq.length
    # XXX Implementation detail XXX
    assert_equal num_registered_threads + 1,
                 @hbase.instance_variable_get(:@threads).length

    assert_equal @hbase.table(TABLE).htable, @hbase[TABLE].htable
  end

  def test_table_close
    htables = Set.new
    htables << @hbase[TABLE].htable
    htables << @hbase[TABLE].htable
    assert_equal 1, htables.length

    @hbase[TABLE].close
    htables << @hbase[TABLE].htable
    htables << @hbase[TABLE].htable
    assert_equal 2, htables.length
  end

  def test_put_then_get
    row1 = next_rowkey.to_s
    row2 = next_rowkey.to_s
    row3 = next_rowkey.to_s
    # Single record put
    assert_equal 1, @table.put(row1,
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
    assert_equal 1, @table.put(row1,
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
      row2 => { 'cf1:a' => 2, 'cf1:b' => 'b', 'cf1:c' => 6.28 },
      row3 => { 'cf1:a' => 4, 'cf1:b' => 'c', 'cf1:c' => 6.28 })

    # single-get (latest version)
    result = @table.get(row1)
    # Test enumerator
    assert_equal result.to_a, result.each.each.to_a
    assert_equal result.to_a, result.each.take_while { true }.to_a

    assert_equal row1, @table.get(row1).rowkey(:string)
    assert_equal row1, @table.get(row1).rowkey(:string)
    assert_equal 1,      @table.get(row1).fixnum('cf1:a')
    assert_equal 'a',    @table.get(row1).string('cf1:b')
    assert_equal 'a',    String.from_java_bytes(@table.get(row1).raw('cf1:b'))
    assert_equal 'a',    @table.get(row1).byte_array('cf1:b').as(:string)
    assert_equal 3.14,   @table.get(row1).float('cf1:c')
    assert_equal true,   @table.get(row1).boolean('cf1:d')
    assert_equal :sym,   @table.get(row1).symbol('cf1:f')
    assert_equal BigDecimal.new("123.456"), @table.get(row1).bigdecimal('cf1:g')
    assert_equal 101,   @table.get(row1).byte('cf1:byte')
    assert_equal 201,   @table.get(row1).short('cf1:short')
    assert_equal 301,   @table.get(row1).int('cf1:int')

    # single-get-multi-col (deprecated since 0.3)
    # assert_equal %w[Hello World], @table.get(row1).string(['cf1:str1', 'cf1:str2'])
    # assert_equal [301, 401], @table.get(row1).int(['cf1:int', 'cf1:int2'])

    # single-get-multi-ver
    assert_equal [1, 2],        @table.get(row1).fixnums('cf1:a').values
    assert_equal %w[a b],       @table.get(row1).strings('cf1:b').values
    assert_equal %w[a b],       @table.get(row1).raws('cf1:b').values.map { |v| String.from_java_bytes v }
    assert_equal %w[a b],       @table.get(row1).byte_arrays('cf1:b').values.map { |v| v.as :string }
    assert_equal [3.14, 6.28],  @table.get(row1).floats('cf1:c').values
    assert_equal [true, false], @table.get(row1).booleans('cf1:d').values
    assert_equal [:sym, :bol],  @table.get(row1).symbols('cf1:f').values
    assert_equal [
      BigDecimal.new("123.456"),
      BigDecimal.new("456.123")], @table.get(row1).bigdecimals('cf1:g').values
    assert_equal [101, 100], @table.get(row1).bytes('cf1:byte').values
    assert_equal [201, 200], @table.get(row1).shorts('cf1:short').values
    assert_equal [301, 300], @table.get(row1).ints('cf1:int').values

    assert @table.get(row1).fixnums('cf1:a').keys.all? { |k| k.instance_of? Fixnum }

    # single-get-multi-col-multi=ver (deprecated since 0.3)
    # rets = @table.get(row1).strings(['cf1:str1', 'cf1:str2'])
    # assert_equal ['Hello', 'World'], rets.map(&:values).map(&:first)
    # assert_equal ['Goodbye', 'Cruel world'], rets.map(&:values).map(&:last)

    # multi-get
    assert_equal [row1, row2, row3], @table.get([row1, row2, row3]).map { |r| r.rowkey :string }
    assert_equal [1, 2, 4         ], @table.get([row1, row2, row3]).map { |r| r.fixnum('cf1:a') }
    assert_equal [3.14, 6.28, 6.28], @table.get([row1, row2, row3]).map { |r| r.float('cf1:c') }
    assert_equal [nil, nil        ], @table.get(['xxx', 'yyy'])

    # Unavailable columns
    assert_equal nil, @table.get(row1).symbol('cf1:xxx')
    assert_equal nil, @table.get(row1).fixnum('cf1:xxx')

    # Unavailable columns (plural form)
    assert_equal({}, @table.get(row1).strings('cf1:xxx'))
    assert_equal({}, @table.get(row1).strings('cfx:xxx'))

    # Row not found
    assert_equal nil, @table.get('xxx')
  end

  # Put added after a delete is overshadowed if its timestamp is older than than that of the tombstone
  # https://issues.apache.org/jira/browse/HBASE-2847
  def test_put_delete_put
    rowkey = next_rowkey
    pend("https://issues.apache.org/jira/browse/HBASE-2847") do
      data = { 'cf1:pdp' => { 1250000000000 => 'A1' } }
      @table.put rowkey => data
      assert_equal 'A1', @table.get(rowkey).string('cf1:pdp')
      @table.delete rowkey
      assert_nil @table.get(rowkey)
      @table.put rowkey => data
      assert_equal 'A1', @table.get(rowkey).string('cf1:pdp')
    end
  end

  def test_put_timestamp
    rowkey = next_rowkey
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
    row1 = next_rowkey.to_s
    row2 = next_rowkey.to_s

    @table.put(row1, 'cf1:counter' => 1, 'cf1:counter2' => 100)
    assert_equal 1, @table.get(row1).fixnum('cf1:counter')

    ret = @table.increment(row1, 'cf1:counter', 1)
    assert_equal 2, ret['cf1:counter']
    assert_equal 2, @table.get(row1).fixnum('cf1:counter')

    ret = @table.increment(row1, 'cf1:counter', 2)
    assert_equal 4, ret['cf1:counter']
    assert_equal 4, @table.get(row1).fixnum('cf1:counter')

    # Multi-column increment
    ret = @table.increment(row1, 'cf1:counter' => 4, 'cf1:counter2' => 100)
    assert_equal 8,   ret['cf1:counter']
    assert_equal 8,   @table.get(row1).fixnum('cf1:counter')
    assert_equal 200, ret['cf1:counter2']
    assert_equal 200, ret[%w[cf1 counter2]]
    assert_equal 200, @table.get(row1).fixnum('cf1:counter2')

    # Multi-row multi-column increment
    @table.put(row2, 'cf1:counter' => 1, 'cf1:counter2' => 100)
    ret = @table.increment row1 => { 'cf1:counter' => 4, 'cf1:counter2' => 100 },
                           row2 => { 'cf1:counter' => 1, 'cf1:counter2' => 100 }
    assert_equal 12,  ret[row1]['cf1:counter']
    assert_equal 300, ret[row1]['cf1:counter2']
    assert_equal 2,   ret[row2]['cf1:counter']
    assert_equal 200, ret[row2]['cf1:counter2']
    assert_equal 200, ret[row2][%w[cf1 counter2]]
    assert_equal 12,  @table.get(row1).fixnum('cf1:counter')
    assert_equal 300, @table.get(row1).fixnum('cf1:counter2')
    assert_equal 2,   @table.get(row2).fixnum('cf1:counter')
    assert_equal 200, @table.get(row2).fixnum('cf1:counter2')
  end

  def test_delete
    row1 = next_rowkey.to_s
    row2 = next_rowkey.to_s
    row3 = next_rowkey.to_s

    @table.put(row1, 'cf1:' => 0, 'cf1:a' => 1, 'cf1:b' => 2, 'cf2:c' => 3, 'cf2:d' => 4)
    sleep 0.1
    @table.put(row1, 'cf2:d' => 5)
    sleep 0.1
    @table.put(row1, 'cf2:d' => 6)
    versions = @table.get(row1).to_H[%w[cf2 d]].keys
    assert versions[0] > versions[1]
    assert versions[1] > versions[2]

    # Deletes a version (Fixnum and Time as timestamps)
    @table.delete(row1, 'cf2:d', versions[0], Time.at(versions[2] / 1000.0))
    new_versions = @table.get(row1).to_H[%w[cf2 d]].keys
    assert_equal new_versions, [versions[1]]

    # Deletes a column
    assert_equal 3, @table.get(row1).fixnum('cf2:c')
    @table.delete(row1, 'cf2:c')
    assert_nil @table.get(row1).to_h['cf2:c']

    # Deletes a column with empty qualifier
    assert_equal 0, @table.get(row1).fixnum('cf1:')
    @table.delete(row1, 'cf1:')
    assert_equal 1, @table.get(row1).fixnum('cf1:a')
    assert_equal 2, @table.get(row1).fixnum('cf1:b')
    assert_nil @table.get(row1).to_h['cf1:']

    # Deletes a column family
    assert_equal 1, @table.get(row1).fixnum('cf1:a')
    assert_equal 2, @table.get(row1).fixnum('cf1:b')
    @table.delete(row1, 'cf1') # No trailing colon
    assert_nil @table.get(row1).to_h['cf1:a']
    assert_nil @table.get(row1).to_h['cf1:b']

    # Deletes a row
    @table.delete(row1)
    assert_nil @table.get(row1)

    # Batch delete
    @table.put(row2, 'cf1:a' => 1)
    @table.put(row3, 'cf1:a' => 1, 'cf1:b' => 2)

    @table.delete [row2], [row3, 'cf1:a']
    assert_nil @table.get(row2)
    assert_nil @table.get(row3).to_h['cf1:a']
    assert_equal 2, @table.get(row3).fixnum('cf1:b')
  end

  def test_delete_advanced
    row1 = next_rowkey.to_s
    drow = next_rowkey.to_s

    @table.put(row1, 'cf1:' => 0, 'cf1:a' => 1, 'cf1:b' => 2, 'cf2:c' => 3, 'cf2:d' => 4)
    @table.delete(row1, 'cf1:', 'cf1:b', 'cf2')
    assert_equal 1, @table.get(row1).to_h.keys.length
    assert_equal 1, @table.get(row1).fixnum('cf1:a')

    ts = Time.now
    @table.put(drow, 'cf1:a' => { 1000 => 1, 2000 => 2, 3000 => 3 },
                        'cf1:b' => { 4000 => 4, 5000 => 5, 6000 => 6 },
                        'cf2:c' => 3, 'cf2:d' => 4, 'cf3:e' => 5)
    @table.delete(drow, 'cf1:a', 1000, Time.at(2),
                           'cf2:c',
                           'cf1:b', 5000,
                           'cf3')

    assert_equal 3, @table.get(drow).to_h.keys.length

    assert_equal 1, @table.get(drow).to_H['cf1:a'].length
    assert_equal 2, @table.get(drow).to_H['cf1:b'].length
    assert_equal 3000, @table.get(drow).to_H['cf1:a'].keys.first
    assert_equal [6000, 4000], @table.get(drow).to_H['cf1:b'].keys
  end

  def test_delete_advanced_with_schema
    row1 = next_rowkey.to_s
    drow = next_rowkey.to_s

    @hbase.schema[@table.name] = {
      :cf1 => {
        :a => :int,
        :b => :short,
      },
      :cf2 => {
        :c => :long,
        :d => :byte
      },
      :cf3 => {
        :e => :fixnum
      }
    }
    @table.put(row1, 'cf1:' => 0, :a => 1, :b => 2, :c => 3, :d => 4)
    @table.delete(row1, 'cf1:', :b, 'cf2')
    assert_equal 1, @table.get(row1).to_h.keys.length
    assert_equal 1, @table.get(row1).to_h[:a]
    assert_equal 1, @table.get(row1).int(:a)

    ts = Time.now
    @table.put(drow, :a => { 1000 => 1, 2000 => 2, 3000 => 3 },
                        :b => { 4000 => 4, 5000 => 5, 6000 => 6 },
                        :c => 3,
                        :d => 4,
                        :e => 5)
    @table.delete(drow, :a, 1000, Time.at(2),
                           :c,
                           [:cf1, :b], 5000,
                           'cf3')

    assert_equal 3, @table.get(drow).to_h.keys.length

    assert_equal 1, @table.get(drow).to_H[:a].length
    assert_equal 2, @table.get(drow).to_H[:b].length
    assert_equal 3000, @table.get(drow).to_H[:a].keys.first
    assert_equal [6000, 4000], @table.get(drow).to_H[:b].keys
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

  def test_check
    assert_raise(ArgumentError) { @table.check(1, :a => 1, :b => 2) }
    assert_raise(ArgumentError) { @table.check(1) }
  end

  def test_check_and_put
    @hbase.schema[@table.name] = {
      :cf1 => {
        :a => :short,
        :b => :short,
        :c => :short
      }
    }

    [
      # Without schema
      [next_rowkey, 'cf1:a', 'cf1:b', 'cf1:c'],
      [next_rowkey, 'cf1:a', 'cf1:b', 'cf1:c'],
      # With schema
      [next_rowkey, :a, :b, :c],
      [next_rowkey, :a, :b, :c],
    ].each do |args|
      rk, a, b, c = args

      @table.put rk, 'cf1:a' => 100

      # not nil
      assert_equal false, @table.check(rk, a => 200).put(b => 300)
      assert_equal nil, @table.get(rk).short(b)

      assert_equal true, @table.check(rk, a => 100).put(b => 300)
      assert_equal 300, @table.get(rk).short(b)

      # nil
      assert_equal false, @table.check(rk, a => nil).put(c => 300)
      assert_equal nil, @table.get(rk).short(c)

      assert_equal true, @table.check(rk, c => nil).put(c => 300)
      assert_equal 300, @table.get(rk).short(c)
    end
  end

  def test_check_and_delete
    @hbase.schema[@table.name] = {
      :cf1 => {
        :a => :short,
        :b => :short,
        :c => :short
      }
    }

    [
      ['cf1:a', 'cf1:b', 'cf1:c', 'cf2:d'],
      [:a, :b, :c, :d]
    ].each do |abcd|
      a, b, c, d = abcd

      rk = next_rowkey
      ts = Time.now
      @table.put rk, a => 100, b => 200,
        c => { ts => 300, (ts - 1000) => 400, (ts - 2000).to_i => 500 },
        d => 1000
      assert_equal 3, @table.get(rk).to_H[:c].length

      assert_equal false, @table.check(rk, a => 200).delete(b)
      assert_equal 200, @table.get(rk)[b]

      assert_equal true, @table.check(rk, a => 100).delete(b)
      assert_equal nil, @table.get(rk)[b]

      assert_equal true, @table.check(rk, a => 100).delete(c, ts, (ts - 2000).to_i, 'cf2')
      assert_equal 1, @table.get(rk).to_H[:c].length
      assert_equal (ts - 1000).to_i, @table.get(rk).to_H[:c].keys.first / 1000
      assert_equal nil, @table.get(rk)[d]

      assert_equal true, @table.check(rk, a => 100).delete
      assert_equal nil, @table.get(rk)

      @table.delete rk

      @hbase.schema[@table.name] = {
        :cf1 => { :a => :fixnum, :b => :fixnum, :c => :fixnum },
        :cf2 => { :d => :fixnum }
      }
    end
  end

  def test_append
    rk = next_rowkey
    @table.put rk, 'cf1:a' => 'hello', 'cf2:b' => 'foo'
    result = @table.append rk, 'cf1:a' => ' world', 'cf2:b' => 'bar'
    assert_equal 'hello world', result['cf1:a'].to_s
    assert_equal 'foobar',      result[%w[cf2 b]].to_s
    assert_equal 'hello world', @table.get(rk).string('cf1:a')
    assert_equal 'foobar',      @table.get(rk).string('cf2:b')
  end

  def test_mutate
    rk = next_rowkey
    @table.put rk,
      'cf1:a' => 100, 'cf1:b' => 'hello', 'cf1:c' => 'hola',
      'cf2:d' => 3.14
    ret = @table.mutate(rk) { |m|
      m.put 'cf1:a' => 200
      m.delete 'cf1:c', 'cf2'
      m.put 'cf1:z' => true
    }
    assert_equal nil, ret

    row = @table.get(rk)
    assert_equal 200,     row.long('cf1:a')
    assert_equal 'hello', row.string('cf1:b')
    assert_equal nil,     row.string('cf1:c')
    assert_equal nil,     row['cf1:c']
    assert_equal true,    row.boolean('cf1:z')
    assert_equal nil,     row.float('cf2:d')
    assert_equal nil,     row['cf2:d']

    @table.mutate(rk) { |m| } # Nothing
    @table.mutate(rk) { |m| m.delete }
    assert_equal nil, @table.get(rk)
  end

  def test_invalid_column_key
    assert_raise(ArgumentError) {
      @table.put next_rowkey, :some_column => 1
    }
  end

  def test_batch
    rk1, rk2, rk3 = next_rowkey, next_rowkey, next_rowkey

    ret = @table.batch { |b|
      b.put rk1, 'cf1:a' => 1, 'cf1:b' => 2, 'cf2:c' => 'hello'
      b.put rk2, 'cf1:a' => 2, 'cf1:b' => 3, 'cf2:c' => 'hello'
      b.put rk3, 'cf1:a' => 3, 'cf1:b' => 4, 'cf2:c' => 'hello'
    }
    assert_equal 3, ret.length
    assert_equal :put, ret[0][:type]
    assert_equal :put, ret[1][:type]
    assert_equal :put, ret[2][:type]
    assert_equal true, ret[0][:result]
    assert_equal true, ret[1][:result]
    assert_equal true, ret[2][:result]

    # FIXME: Mutation in batch hangs on 0.96
    mutation_in_batch = @aggregation
    ret = @table.batch { |b|
      b.put rk3, 'cf1:c' => 5
      b.delete rk1, 'cf1:a'
      b.increment rk2, 'cf1:a' => 10, 'cf1:b' => 20
      b.append rk2, 'cf2:c' => ' world'
      b.get(rk1)
      b.filter('cf1:a' => 0).get(rk1)
      b.versions(1).project('cf2').get(rk1)
      if mutation_in_batch
        b.mutate(rk3) do |m|
          m.put 'cf2:d' => 'hola'
          m.put 'cf2:e' => 'mundo'
          m.delete 'cf1:b'
        end
      else
        @table.mutate(rk3) do |m|
          m.put 'cf2:d' => 'hola'
          m.put 'cf2:e' => 'mundo'
          m.delete 'cf1:b'
        end
      end
    }
    if mutation_in_batch
      assert_equal 8, ret.length
      assert_equal [:put, :delete, :increment, :append, :get, :get, :get, :mutate], ret.map { |r| r[:type] }
      assert_equal [true, true, true], ret.values_at(0, 1, 7).map { |r| r[:result] }
    else
      assert_equal 7, ret.length
      assert_equal [:put, :delete, :increment, :append, :get, :get, :get], ret.map { |r| r[:type] }
      assert_equal [true, true], ret.values_at(0, 1).map { |r| r[:result] }
    end
    assert_equal 12,            ret[2][:result]['cf1:a']
    assert_equal 23,            ret[2][:result]['cf1:b']
    assert_equal 'hello world', ret[3][:result]['cf2:c'].to_s
    # assert_equal nil,           ret[5][:result].long('cf1:a') # No guarantee
    assert_equal 2,             ret[4][:result].long('cf1:b')
    assert_equal nil,           ret[5][:result]
    assert_equal nil,           ret[6][:result].fixnum('cf1:b')
    assert_equal 'hello',       ret[6][:result].string('cf2:c')

    assert_equal nil,           @table.get(rk1)['cf1:a']
    assert_equal 12,            @table.get(rk2).long('cf1:a')
    assert_equal 23,            @table.get(rk2).long('cf1:b')
    assert_equal 5,             @table.get(rk3).long('cf1:c')
    assert_equal 'hello world', @table.get(rk2).string('cf2:c')
    assert_equal 'hola',        @table.get(rk3).string('cf2:d')
    assert_equal 'mundo',       @table.get(rk3).string('cf2:e')
    assert_equal nil,           @table.get(rk3).string('cf2:b')
  end

  def test_batch_exception
    rk = next_rowkey
    @table.put rk, 'cf1:a' => 1

    begin
      @table.batch do |b|
        b.put next_rowkey, 'cf1:a' => 1
        b.put next_rowkey, 'cf100:a' => 1
        b.get rk
        b.put next_rowkey, 'cf200:a' => 1
      end
      assert false
    rescue HBase::BatchException => e
      assert_equal 4, e.results.length
      assert_equal true, e.results[0][:result]
      assert_equal false, e.results[1][:result]
      assert_equal 1, e.results[2][:result].fixnum('cf1:a')
      assert_equal false, e.results[3][:result]

      assert e.results[1][:exception].is_a?(java.lang.Exception)
      assert e.results[3][:exception].is_a?(java.lang.Exception)
      assert e.java_exception.is_a?(java.lang.Exception)
    end
  end
end

