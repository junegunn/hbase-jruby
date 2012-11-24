#!/usr/bin/env ruby

$LOAD_PATH.unshift File.expand_path('..', __FILE__)
require 'helper'

class TestHBase < TestHBaseJRubyBase 
  def test_tables
    assert @hbase.table_names.include?(TABLE)
    assert @hbase.tables.map(&:name).include?(TABLE)
  end

  def test_close
    @hbase.close

    # TODO: Still usable after close?
    assert @hbase.table_names.include?(TABLE)
    assert_equal 1, @table.put('rowkey' => { 'cf1:a' => 1 })
  end

  def test_admin
    assert_instance_of org.apache.hadoop.hbase.client.HBaseAdmin, @hbase.admin
    @hbase.admin do |admin|
      assert_instance_of org.apache.hadoop.hbase.client.HBaseAdmin, admin
    end
  end

  def test_config
    assert_instance_of org.apache.hadoop.conf.Configuration, @hbase.config
  end

  def test_shared_config
    hbase2 = HBase.new @hbase.config
    assert_equal @hbase.config, hbase2.config
  end
end

