#!/usr/bin/env ruby

$LOAD_PATH.unshift File.expand_path('..', __FILE__)
require 'helper'

class TestAggregation < TestHBaseJRubyBase
  def test_aggregation
    (1..100).each do |idx|
      @table.put idx, 'cf1:a' => idx, 'cf1:b' => idx * 2
    end

    assert_nil @table.enable_aggregation!
    assert_nil @table.enable_aggregation! # no prob!

    lci = org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter.new
    [nil, :fixnum, :long, lci].each do |ci|
      assert_equal 100,  @table.project('cf1:a').aggregate(:row_count, *[*ci].compact)
      assert_equal 5050, @table.project('cf1:a').aggregate(:sum, *[*ci].compact)
      assert_equal 1,    @table.project('cf1:a').aggregate(:min, *[*ci].compact)
      assert_equal 100,  @table.project('cf1:a').aggregate(:max, *[*ci].compact)
      assert_equal 50.5, @table.project('cf1:a').aggregate(:avg, *[*ci].compact)
      assert_equal 28,   @table.project('cf1:a').aggregate(:std, *[*ci].compact).to_i # FIXME: 28 or 29?
    end

    [%w[cf1:a cf1:b], %w[cf1]].each do |prj|
      assert_equal 5050 * 3, @table.project(*prj).aggregate(:sum)
      assert_equal 1,        @table.project(*prj).aggregate(:min)
      assert_equal 200,      @table.project(*prj).aggregate(:max)
    end

    # No projection
    assert_raise(ArgumentError) { @table.aggregate(:sum) }
    assert_raise(ArgumentError) { @table.each.aggregate(:sum) }

    # Invalid type
    assert_raise(ArgumentError) { @table.project('cf1:a').aggregate(:sum, :float) }

    @table.drop!
  end
end

