#!/usr/bin/env ruby

$LOAD_PATH.unshift File.expand_path('..', __FILE__)
require 'helper'
require 'bigdecimal'

class TestCell < TestHBaseJRubyBase
  import org.apache.hadoop.hbase.KeyValue
  Util = HBase::Util

  def test_cell
    ts = Time.now.to_i * 1000
    {
      'value'                   => :string,
      :value                    => :symbol,
      123                       => :fixnum,
      123.456                   => :double,
      true                      => :boolean,
      false                     => :boolean,
      BigDecimal.new("123.456") => :bigdecimal,
      { :int   => 10240 }       => :int,
      { :short => 1024 }        => :short,
      { :byte  => 100 }         => :byte,
      { :float => 123.0 }       => :float
    }.each do |value, type|
      kv = KeyValue.new("rowkey".to_java_bytes, "hello".to_java_bytes, "world".to_java_bytes, ts, Util.to_bytes(value))
      cell = HBase::Cell.new(@table, kv) # FIXME

      assert_equal "rowkey", cell.rowkey(:string)
      assert_equal "hello",  cell.cf, cell.family
      assert_equal "world",  cell.cq, cell.qualifier
      assert_equal ts,       cell.ts
      if value.is_a?(Hash)
        assert_equal value.values.first, cell.send(type)
      else
        assert_equal value, cell.send(type)
      end
      assert HBase::Util.java_bytes?(cell.raw)
      assert_instance_of String, cell.inspect
    end
  end

  def test_order
    ts = Time.now.to_i * 1000

    val = "val".to_java_bytes
    cells =
      [
        KeyValue.new("rowkey".to_java_bytes, "apple".to_java_bytes,  "alpha".to_java_bytes, ts, val),
        KeyValue.new("rowkey".to_java_bytes, "apple".to_java_bytes,  "alpha".to_java_bytes, ts - 1000, val),
        KeyValue.new("rowkey".to_java_bytes, "apple".to_java_bytes,  "beta".to_java_bytes,  ts, val),
        KeyValue.new("rowkey".to_java_bytes, "banana".to_java_bytes, "beta".to_java_bytes,  ts, val),
        KeyValue.new("rowkey".to_java_bytes, "banana".to_java_bytes, "gamma".to_java_bytes, ts, val),
      ].map { |kv| HBase::Cell.new @table, kv }

    assert_equal cells, cells.reverse.sort
  end
end
