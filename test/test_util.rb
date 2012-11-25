#!/usr/bin/env ruby

$LOAD_PATH.unshift File.expand_path('..', __FILE__)
require 'helper'
require 'bigdecimal'

class TestUtil < Test::Unit::TestCase
  Util = HBase::Util

  def test_bytea_conversion
    Util.import_java_classes!

    assert_raise(ArgumentError) { Util.to_bytes(10 ** 30) }

    [:fixnum, :int, :integer].each do |type|
      assert_equal 100, Util.from_bytes( type, Util.to_bytes(100) )
    end
    [:float, :double].each do |type|
      assert_equal 3.14, Util.from_bytes( type, Util.to_bytes(3.14) )
    end
    [:string, :str].each do |type|
      assert_equal "Hello", Util.from_bytes( type, Util.to_bytes("Hello") )
    end
    [:bool, :boolean].each do |type|
      assert_equal true,  Util.from_bytes( type, Util.to_bytes(true) )
      assert_equal false, Util.from_bytes( type, Util.to_bytes(false) )
    end
    [:symbol, :sym].each do |type|
      assert_equal :hello, Util.from_bytes( type, Util.to_bytes(:hello) )
    end

    bd  = BigDecimal.new("123456789.123456789")
    jbd = java.math.BigDecimal.new("9876543210.987654321")
    [:bigdecimal].each do |type|
      assert_equal bd,  Util.from_bytes( type, Util.to_bytes(bd) )
      assert_equal jbd, Util.from_bytes( type, Util.to_bytes(jbd) )
    end

    assert_equal String.from_java_bytes("asdf".to_java_bytes),
                 String.from_java_bytes( Util.from_bytes( :raw, "asdf".to_java_bytes ) )

    assert_equal 0, Util.to_bytes(nil).length

    assert_raise(ArgumentError) { Util.from_bytes(:xxx, [].to_java(Java::byte)) }
    assert_raise(ArgumentError) { Util.to_bytes({}) }
  end

  def test_parse_column_name
    assert_equal ['abc', 'def'],  parse_to_str('abc:def') 
    assert_equal ['abc', 'def:'], parse_to_str('abc:def:') 
    assert_equal ['abc', ''],     parse_to_str('abc:')
    assert_equal ['abc', nil],    parse_to_str('abc')
    assert_equal ['abc', ':::'],  parse_to_str('abc::::')

    assert_equal [:abc, :def],   parse_to_str([:abc, :def], :symbol)
    assert_equal [123, 456],     parse_to_str([123, 456], :fixnum)
    assert_equal ['abc', 'def'], parse_to_str(
                                   org.apache.hadoop.hbase.KeyValue.new(
                                     'rowkey'.to_java_bytes,
                                     'abc'.to_java_bytes,
                                     'def'.to_java_bytes))

    assert_equal [:abc, :def],   parse_to_str(HBase::ColumnKey.new(:abc, :def), :symbol)

    assert_raise(ArgumentError) { Util.parse_column_name(nil) }
    assert_raise(ArgumentError) { Util.parse_column_name('') }

    assert_equal nil, Util.from_bytes(:string, nil)
  end

  def test_append_0
    assert_equal [97, 97, 97, 0], Util.append_0("aaa".to_java_bytes).to_a
  end

private
  def parse_to_str v, type = :string
    Util.parse_column_name(v).map { |e| Util.from_bytes type, e }
  end
end
