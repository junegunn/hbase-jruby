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

    data = {
      :a => 100,
      :b => :symb,
      :c => 200,
      :d => 3.14,
      :d2 => 300,
      'cf2:e' => 'Hello',
      'cf3:f' => 400,
      'cf1:x' => 500
    }

    # PUT
    @table.put 1, 'cf1:a' => data[:a], 'cf1:b' => data[:b], 'cf1:c' => data[:c],
                  'cf1:d' => data[:d], 'cf1:d2' => data[:d2], 'cf2:e' => data['cf2:e'],
                  'cf3:f' => data['cf3:f'], 'cf1:x' => data['cf1:x']
    @table.put 2, data

    # GET
    row = @table.get(2)
    assert_equal @table.get(1).to_h.values.map { |b| HBase::ByteArray.new b },
                           row.to_h.values.map { |b| HBase::ByteArray.new b }
    assert_equal @table.get(1).to_h.keys, row.to_h.keys
    assert_equal [:a, :b, :c, :d, :d2, "cf1:x", "cf2:e", "cf3:f"], row.to_h.keys

    assert_equal data[:a],      row[:a]
    assert_equal data[:a],      row['a']
    assert_equal data[:a],      row['cf1:a']

    assert_equal data[:b],      row[:b]
    assert_equal data[:b],      row['b']
    assert_equal data[:b],      row['cf1:b']

    assert_equal data[:c],      row[:c]
    assert_equal data[:c],      row['c']
    assert_equal data[:c],      row['cf1:c']

    assert_equal data[:d],      row[:d]
    assert_equal data[:d],      row['d']
    assert_equal data[:d],      row['cf1:d']

    assert_equal data[:d2],     row[:d2]
    assert_equal data[:d2],     row['d2']
    assert_equal data[:d2],     row['cf1:d2']

    assert_equal data['cf2:e'], row['cf2:e']
    assert_equal data['cf3:f'], row['cf3:f']

    assert_equal data['cf1:x'], HBase::Util.from_bytes(:long, row['cf1:x'])

    data1 = @table.get(1).to_h
    data1[:a] *= 2
    @table.put 3, data1

    # PUT again
    assert_equal data[:a] * 2, @table.get(3)[:a]

    # PROJECT
    assert_equal [:a, :c, 'cf2:e', 'cf3:f'],
      @table.project(:a, 'cf1:c', :cf2, :cf3).first.to_h.keys

    # FILTER
  end

  def test_schema_readme
    @table.schema = {
      :cf1 => {
        :name   => :string,
        :age    => :fixnum,
        :sex    => :symbol,
        :height => :float,
        :weight => :float,
        :alive  => :boolean
      },
      :cf2 => {
        :description => :string,
        /^score.*/   => :float
      }
    }

    data = {
      :name        => 'John Doe',
      :age         => 20,
      :sex         => :male,
      :height      => 6.0,
      :weight      => 175,
      :description => 'N/A',
      :score1      => 8.0,
      :score2      => 9.0,
      :score3      => 10.0,
      :alive       => true
    }

    @table.put(100 => data)

    john = @table.get(100)

    data.each do |k, v|
      assert_equal v, john[k]
      assert_equal v, john[k.to_s]
    end

    assert_equal 20,   john['cf1:age']
    assert_equal 10.0, john['cf2:score3']

    assert_equal data, john.to_h
    assert_equal data, Hash[ john.to_H.map { |k, v| [k, v.first.last] } ]
  end
end

