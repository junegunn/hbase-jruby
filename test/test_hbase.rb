#!/usr/bin/env ruby

$LOAD_PATH.unshift File.expand_path('..', __FILE__)
require 'helper'

class TestHBase < TestHBaseJRubyBase
  def test_log4j
    HBase.log4j = File.expand_path('../log4j/log4j.properties', __FILE__)
    HBase.log4j = File.expand_path('../log4j/log4j.xml', __FILE__)

    begin
      HBase.log4j = File.expand_path('../log4j/log4j_invalid.xml', __FILE__)
      assert false, "Exception must be thrown when invalid XML is given"
    rescue Exception
    end
  end

  def test_tables
    assert @hbase.table_names.include?(TABLE)
    assert @hbase.list.include?(TABLE)
    assert @hbase.tables.map(&:name).include?(TABLE)
  end

  def test_close
    table = @hbase[TABLE]
    table.exists?
    assert @hbase.list.is_a?(Array)

    assert_equal table.htable, Thread.current[:hbase_jruby][@hbase][TABLE]

    assert !@hbase.closed?
    assert !table.closed?
    @hbase.close
    assert @hbase.closed?
    assert table.closed?

    assert !Thread.current[:hbase_jruby].has_key?(@hbase)

    assert_raise(RuntimeError) { @hbase.list }
    assert_raise(RuntimeError) { table.exists? }
    assert_raise(RuntimeError) { table.drop! }
    # ...

    # get, delete, put, delete_row, increment, each
    assert_raise(RuntimeError) { table.first }
    assert_raise(RuntimeError) { table.get :key }
    assert_raise(RuntimeError) { table.put :key => {'cf1:a' => 100} }
    assert_raise(RuntimeError) { table.delete :key }
    assert_raise(RuntimeError) { table.delete_row :key }
    assert_raise(RuntimeError) { table.increment :key, 'cf1:a' => 1 }
    assert_raise(RuntimeError) { table.project('cf1:a').aggregate(:row_count) }

    # Reconnect and check
    @hbase = connect
    table = @hbase[TABLE]
    assert_equal table.htable, Thread.current[:hbase_jruby][@hbase][TABLE]
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

  def test_thread_local_cleanup
    # Open a new connection
    hbase2 = HBase.new @hbase.config

    # Thread-local htable cache is empty
    assert_nil Thread.current[:hbase_jruby][hbase2]

    table = hbase2[TABLE]
    assert_nil Thread.current[:hbase_jruby][hbase2]

    table.htable
    # Thread-local htable cache has now been created
    assert Thread.current[:hbase_jruby][hbase2][TABLE]

    sleeping = {}
    mutex = Mutex.new
    threads = 4.times.map { |i|
      Thread.new {
        Thread.current[:htable] = hbase2[TABLE].htable
        mutex.synchronize { sleeping[Thread.current] = true }
        sleep
      }
    }
    sleep 0.1 while mutex.synchronize { sleeping.length } < 4
    threads.each do |t|
      assert t[:htable]
      assert t[:hbase_jruby][hbase2][TABLE]
      assert_equal t[:htable], t[:hbase_jruby][hbase2][TABLE]

      t.kill
    end

    # Now close the connection
    hbase2.close

    # Threads-local htable cache deleted
    assert_nil Thread.current[:hbase_jruby][hbase2]
    threads.each do |t|
      assert_nil t[:hbase_jruby][hbase2]
    end

    # Connection is already closed
    assert_raise(RuntimeError) { hbase2[TABLE] }
    assert_raise(RuntimeError) { table.htable  }
  end

  def test_reset_pool
    hbase2 = HBase.new @hbase.config
    omit_unless(hbase2.use_table_pool?) do
      table  = hbase2[TABLE]

      htable = table.htable
      assert_equal htable, table.htable
      assert_equal htable, hbase2[TABLE.to_sym].htable

      assert_nil hbase2.reset_table_pool

      assert_not_equal htable, table.htable
      assert_not_equal htable, hbase2[TABLE].htable

      htable = table.htable
      assert_equal htable, table.htable
      assert_equal htable, hbase2[TABLE.to_sym].htable
    end
    hbase2.close
  end
end

