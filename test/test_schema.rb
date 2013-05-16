#!/usr/bin/env ruby

$LOAD_PATH.unshift File.expand_path('..', __FILE__)
require 'helper'

class TestSchema < TestHBaseJRubyBase
  def test_schema
    @table.schema = {
      # Schema allows you to omit column family names,
      # and to retrieve data without specifying types every time
      :cf1 => {
        :a    => :fixnum,
        :b    => :symbol,
        :c    => :int,
        /^d/i => :float,
        'd2'  => :short,
      },
      # Every column from cf2 is :string
      :cf2 => :string,

      # cf3:e is a 8-byte integer
      'cf3:f' => :fixnum,
    }

    # PUT
    @table.put 1, 'cf1:a' => 100, 'cf1:b' => :symb, 'cf1:c' => 200, 'cf1:d' => 3.14, 'cf1:d2' => 300, 'cf2:e' => "Hello", 'cf3:f' => 400, 'cf1:x' => 500
    @table.put 2, :a => 100, :b => :symb, :c => 200, :d => 3.14, :d2 => 300, 'cf2:e' => "Hello", 'cf3:f' => 400, 'cf1:x' => 500

    # GET
    row = @table.get(2)
    assert_equal @table.get(1).to_hash.values.map { |b| HBase::ByteArray.new b },
                           row.to_hash.values.map { |b| HBase::ByteArray.new b }
    assert_equal 100,     row[:a]
    assert_equal 100,     row['a']
    assert_equal 100,     row['cf1:a']

    assert_equal :symb,   row[:b]
    assert_equal :symb,   row['b']
    assert_equal :symb,   row['cf1:b']

    assert_equal 200,     row[:c]
    assert_equal 200,     row['c']
    assert_equal 200,     row['cf1:c']

    assert_equal 3.14,    row[:d]
    assert_equal 3.14,    row['d']
    assert_equal 3.14,    row['cf1:d']

    assert_equal 300,     row[:d2]
    assert_equal 300,     row['d2']
    assert_equal 300,     row['cf1:d2']

    assert_equal 'Hello', row['cf2:e']
    assert_equal 400,     row['cf3:f']

    assert_equal 500, HBase::Util.from_bytes(:long, row['cf1:x'])

    # FILTER
    # PROJECT
  end
end

