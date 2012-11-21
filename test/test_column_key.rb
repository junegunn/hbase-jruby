#!/usr/bin/env ruby

$LOAD_PATH.unshift File.expand_path('..', __FILE__)
require 'helper'

class TestColumnKey < Test::Unit::TestCase
  Util = HBase::Util

  def test_types
    ck = HBase::ColumnKey("hello", "world")
    assert_equal "hello", ck.family
    assert_equal "hello", ck.cf
    assert_equal "world", ck.qualifier
    assert_equal "world", ck.cq
    assert_equal 'hello:world', ck.to_s

    ck = HBase::ColumnKey("hello".to_java_bytes, 123)
    assert_equal "hello", ck.family
    assert_equal "hello", ck.cf
    assert_equal 123, ck.qualifier(:fixnum)
    assert_equal 123, ck.cq(:fixnum)

    ck = HBase::ColumnKey(:hello, nil)
    assert_equal "hello", ck.family
    assert_equal "hello", ck.cf
    assert_equal '', ck.qualifier(:string)
    assert_equal '', ck.cq(:string)
    assert_equal 'hello', ck.to_s
  end

  def test_eql
    ck1 = HBase::ColumnKey(:hello, :world)
    ck2 = HBase::ColumnKey("hello", "world")

    assert_equal ck1, ck2
    assert_equal ck1, "hello:world"
  end

  def test_order
    assert_equal (1..100).to_a,
      (1..100).to_a.reverse.map { |cq|
        HBase::ColumnKey(:cf, cq)
      }.sort.map { |ck| ck.cq :fixnum }
  end

  def test_as_hash_key
    assert({ HBase::ColumnKey(:hello, :world) => true }[ ck2 = HBase::ColumnKey("hello", "world") ])
  end
end
