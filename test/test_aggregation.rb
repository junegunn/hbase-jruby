#!/usr/bin/env ruby

$LOAD_PATH.unshift File.expand_path('..', __FILE__)
require 'helper'

class TestAggregation < TestHBaseJRubyBase 
  def test_aggregation
    (1..100).each do |idx|
      @table.put idx, 'cf1:a' => idx, 'cf1:b' => idx * 2
    end

    @table.enable_aggregation!

    lci = org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter.new
    [nil, :fixnum, :int, :integer, lci].each do |ci|
      assert_equal 100,  @table.project('cf1:a').aggregate(:row_count, *[*ci])
      assert_equal 5050, @table.project('cf1:a').aggregate(:sum, *[*ci])
      assert_equal 1,    @table.project('cf1:a').aggregate(:min, *[*ci])
      assert_equal 100,  @table.project('cf1:a').aggregate(:max, *[*ci])
      assert_equal 50.5, @table.project('cf1:a').aggregate(:avg, *[*ci])
      assert_equal 28,   @table.project('cf1:a').aggregate(:std, *[*ci]).to_i # FIXME: 28 or 29?
    end

    [%w[cf1:a cf1:b], %w[cf1]].each do |prj|
      assert_equal 5050 * 3, @table.project(*prj).aggregate(:sum)
      assert_equal 1,        @table.project(*prj).aggregate(:min)
      assert_equal 200,      @table.project(*prj).aggregate(:max)
    end

    # No projection
    assert_raise(ArgumentError) { @table.aggregate(:sum) }
    assert_raise(ArgumentError) { @table.each.aggregate(:sum) }
  end
end

