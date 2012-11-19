#!/usr/bin/env ruby

require "test-unit"
require 'simplecov'
SimpleCov.start

$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
require "hbase-jruby"
require 'bigdecimal'
require 'json'

# Required
HBase.resolve_dependency! 'cdh4.1.2'

class TestUtil < Test::Unit::TestCase
  Util = HBase::Util
  include Util

  def test_bytea_conversion
    Util.import_java_classes!

    [:fixnum, :int, :integer].each do |type|
      assert_equal 100, Util.from_bytes( type, Util.to_bytes(100) )
    end
    [:float, :double].each do |type|
      assert_equal 3.14, Util.from_bytes( type, Util.to_bytes(3.14) )
    end
    [:bignum, :biginteger, :bigint].each do |type|
      assert_equal 12345678901234567890, Util.from_bytes( type, Util.to_bytes(12345678901234567890) )
    end
    [:string, :str].each do |type|
      assert_equal "Hello", Util.from_bytes( type, Util.to_bytes("Hello") )
    end
    [:bool, :boolean].each do |type|
      assert_equal true, Util.from_bytes( type, Util.to_bytes(true) )
      assert_equal false, Util.from_bytes( type, Util.to_bytes(false) )
    end
    [:symbol, :sym].each do |type|
      assert_equal :hello, Util.from_bytes( type, Util.to_bytes(:hello) )
    end
    bd = BigDecimal.new("123456789.123456789")
    [:bigdecimal].each do |type|
      assert_equal bd, Util.from_bytes( type, Util.to_bytes(bd) )
    end
  end

  def test_parse_column_name
    assert_equal ['abc', 'def'],  parse_to_str('abc:def') 
    assert_equal ['abc', 'def:'], parse_to_str('abc:def:') 
    assert_equal ['abc', nil],    parse_to_str('abc:')
    assert_equal ['abc', nil],    parse_to_str('abc')
    assert_equal ['abc', ':::'],  parse_to_str('abc::::')

    assert_equal [:abc, :def], parse_to_str([:abc, :def], :symbol)
    assert_equal [123, 456],   parse_to_str([123, 456], :fixnum)
  end

  def test_append_0
    assert_equal [97, 97, 97, 0], Util.append_0("aaa".to_java_bytes).to_a
  end

private
  def parse_to_str v, type = :string
    Util.parse_column_name(v).map { |e| Util.from_bytes type, e }
  end
end
