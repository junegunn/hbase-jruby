#!/usr/bin/env ruby

$LOAD_PATH.unshift File.expand_path('..', __FILE__)
require 'helper'

class TestHBase < TestHBaseJRubyBase
  def test_tables
    assert @hbase.table_names.include?(TABLE)
    assert @hbase.list.include?(TABLE)
    assert @hbase.tables.map(&:name).include?(TABLE)
  end

  def test_close
    table = @hbase[TABLE]
    table.exists?
    assert @hbase.list.is_a?(Array)

    assert !@hbase.closed?
    @hbase.close
    assert @hbase.closed?

    assert_raise(RuntimeError) { @hbase.list }
    assert_raise(RuntimeError) { table.exists? }
    assert_raise(RuntimeError) { table.drop! }
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

