#!/usr/bin/env ruby

$LOAD_PATH.unshift File.expand_path('..', __FILE__)
require 'helper'

class TestByteArray < Test::Unit::TestCase
  def test_order
    [
      [(1..100).to_a, :fixnum],
      [('aa'..'zz').to_a, :string],
    ].each do |pair|
      arr, type = pair
      assert_equal arr,
        arr.reverse.map { |e| HBase::ByteArray.new(e) }.sort.map { |ba|
          HBase::Util.from_bytes type, ba.java
        }
    end
  end

  def test_stopkey_bytes_for_prefix
    assert_equal HBase::ByteArray.new("hellp"),
      HBase::ByteArray.new( HBase::ByteArray.new("hello").stopkey_bytes_for_prefix )
    assert_equal HBase::ByteArray.new("BLUF"),
      HBase::ByteArray.new( HBase::ByteArray.new("BLUE").stopkey_bytes_for_prefix )
    assert_nil HBase::ByteArray.new([127, 127, 127].to_java(Java::byte)).stopkey_bytes_for_prefix
    assert_equal HBase::ByteArray.new([126, 127].to_java(Java::byte)),
      HBase::ByteArray.new(
        HBase::ByteArray.new([126, 126, 127, 127, 127, 127].to_java(Java::byte)).stopkey_bytes_for_prefix
      )
  end

  def test_as_hash_key
    hash = {
      HBase::ByteArray.new("Hello") => 1,
      HBase::ByteArray.new("World") => 2
    }
    assert_equal 1, hash[ HBase::ByteArray.new("Hello") ]
    assert_equal 2, hash[ HBase::ByteArray.new("World".to_java_bytes) ]
  end

  def test_concat
    concat = HBase::ByteArray(100) + HBase::ByteArray(200)
    assert_instance_of HBase::ByteArray, concat
    assert_equal 16, concat.to_java_bytes.to_a.length

    assert_equal 100, HBase::Util.from_bytes( :fixnum, concat.to_java_bytes.to_a[0, 8].to_java(Java::byte) )
    assert_equal 200, HBase::Util.from_bytes( :fixnum, concat.java.to_a[8, 8].to_java(Java::byte) )

    assert HBase::ByteArray(100, 200).eql?(concat)
  end

  def test_default_constructor
    assert_equal 0, HBase::ByteArray().java.to_a.length
    assert_equal 0, HBase::ByteArray.new.java.to_a.length
  end

  def test_slice
    ba = HBase::ByteArray(100, 200, "Hello", 3.14)

    assert_equal 100, ba[0, 8].decode(:fixnum)
    assert_equal 200, ba[8...16].decode(:fixnum)
    assert_equal "Hello", ba[16, 5].decode(:string)
    assert_equal 3.14, ba[21..-1].decode(:float)
  end

  def test_length_shift
    ba = HBase::ByteArray(100, 200, "Hello", false, 3.14)

    assert_equal 30, ba.length
    assert_equal 100, ba.shift(:fixnum)
    assert_equal 22, ba.length
    assert_equal 200, ba.shift(:fixnum)
    assert_equal 14, ba.length
    assert_raise(ArgumentError) { ba.shift(:string) }
    assert_equal "Hello", ba.shift(:string, 5)
    assert_equal 9, ba.length
    assert_equal false, ba.shift(:boolean)
    assert_equal 8, ba.length
    assert_equal 3.14, ba.shift(:float)
    assert_equal 0, ba.length
  end
end
