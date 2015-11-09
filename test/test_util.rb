#!/usr/bin/env ruby

$LOAD_PATH.unshift File.expand_path('..', __FILE__)
require 'helper'
require 'bigdecimal'

class TestUtil < Minitest::Test
  Util = HBase::Util

  def test_bytea_conversion
    assert_raises(ArgumentError) { Util.to_bytes(10 ** 30) }

    [:fixnum, :long].each do |type|
      assert_equal 100, Util.from_bytes( type, Util.to_bytes(100) )
      assert_equal 100, Util.from_bytes( type, Util.to_bytes(HBase::ByteArray(100)) )
    end

    assert_equal 100, Util.from_bytes( :byte,  Util.to_bytes(:byte => 100) )
    assert_equal 100, Util.from_bytes( :short, Util.to_bytes(:short => 100) )
    assert_equal 100, Util.from_bytes( :int, Util.to_bytes(:int => 100) )
    assert_raises(ArgumentError) { Util.to_bytes(:short => "Hello") }
    assert_raises(ArgumentError) { Util.to_bytes(:xxx => 100) }
    assert_raises(ArgumentError) { Util.to_bytes(:short => 100, :int => 200) }

    [:float, :double].each do |type|
      assert_equal 314, (Util.from_bytes( type, Util.to_bytes(type => 3.14) ) * 100).to_i
    end
    assert_equal 4, Util.to_bytes(:float => 3.14).length
    assert_equal 8, Util.to_bytes(:double => 3.14).length
    assert_equal 314, (Util.from_bytes(:float, Util.to_bytes(:float => 3.14)) * 100).to_i
    assert_equal 3.14, Util.from_bytes(:double, Util.to_bytes(:double => 3.14))

    [:string, :str].each do |type|
      assert_equal "Hello", Util.from_bytes( type, Util.to_bytes("Hello") )
      assert_equal "Hello", Util.from_bytes( type, Util.to_bytes(HBase::ByteArray("Hello")) )
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

    byte_array = Util.from_bytes :byte_array, "1234".to_java_bytes
    assert_instance_of HBase::ByteArray, byte_array
    assert_equal "1234", byte_array.as(:string)

    assert_raises(ArgumentError) { Util.from_bytes(:xxx, [].to_java(Java::byte)) }
    assert_raises(ArgumentError) { Util.to_bytes({}) }
  end

  def test_parse_column_name
    assert_equal ['abc', 'def' ], parse_to_str('abc:def')
    assert_equal ['abc', 'def:'], parse_to_str('abc:def:')
    assert_equal ['abc', ''    ], parse_to_str('abc:')
    assert_equal ['abc', nil   ], parse_to_str('abc')
    assert_equal ['abc', ':::' ], parse_to_str('abc::::')

    assert_equal [:abc,  :def ], parse_to_str([:abc, :def], :symbol)
    assert_equal [123,   456  ], parse_to_str([123, 456], :fixnum)
    assert_equal ['abc', 'def'], parse_to_str(
                                   org.apache.hadoop.hbase.KeyValue.new(
                                     'rowkey'.to_java_bytes,
                                     'abc'.to_java_bytes,
                                     'def'.to_java_bytes))

    assert_equal [:abc, :def],   parse_to_str([:abc, :def], :symbol)

    assert_raises(ArgumentError) { Util.parse_column_name(nil) }
    assert_raises(ArgumentError) { Util.parse_column_name('') }

    assert_equal nil, Util.from_bytes(:string, nil)
  end

  def test_append_0
    assert_equal [97, 97, 97, 0], Util.append_0("aaa".to_java_bytes).to_a
  end

  def test_java_bytes
    ["Hello", 1234, :symbol].each do |v|
      assert_equal false, Util.java_bytes?(v)
    end

    ["Hello".to_java_bytes, Util.to_bytes(1234), Util.to_bytes(:symbol)].each do |v|
      assert Util.java_bytes?(v)
    end
  end

private
  def parse_to_str v, type = :string
    Util.parse_column_name(v).map { |e| Util.from_bytes type, e }
  end
end
