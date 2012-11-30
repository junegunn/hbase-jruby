#!/usr/bin/env ruby

$LOAD_PATH.unshift File.expand_path('..', __FILE__)
require 'helper'

class TestTableAdmin < TestHBaseJRubyBase
  def teardown
    @table.drop! if @table.exists?
  end

  def test_create_table_symbol_string
    t = @hbase.table(:test_hbase_jruby_create_table)
    t.drop! if t.exists?
    [ :cf, 'cf', {:cf => {}} ].each do |cf|
      assert_false t.exists?
      t.create! cf
      assert t.exists?
      t.drop!
    end
  end

  def test_disable_and_drop
    @table.disable!
    @table.disable!
    @table.drop!
    assert_false @table.exists?
  end

  def test_create_table_props
    max_fs = 1024 ** 3
    @table.drop!
    @table.create!({ :cf1 => {}, :cf2 => {} }, :max_filesize => max_fs)
    assert_equal max_fs, @table.descriptor.get_max_file_size

    max_fs = 300 * 1024 ** 2
    @table.drop!
    @table.create! :cf1, :max_filesize => max_fs
    assert_equal max_fs, @table.descriptor.get_max_file_size

    @table.drop!
  end

  def test_create_table_invalid_input
    @table.drop!
    assert_raise(ArgumentError) do
      @table.create! 3.14
    end

    assert_raise(ArgumentError) do
      @table.create! :cf1 => { :bloom => 'by beach house' }
    end

    assert_raise(ArgumentError) do
      @table.create! :cf1 => { :bloomfilter => :xxx }
    end
  end

  def test_enabled_disabled
    assert @table.enabled?
    assert !@table.disabled?
    @table.disable!
    assert !@table.enabled?
    assert @table.disabled?
    @table.enable!
    assert @table.enabled?
    assert !@table.disabled?
  end

# def test_rename!
#   new_name = TABLE + '_new'
#   @table.rename! new_name
#   assert_equal new_name, @table.name
#   assert_equal new_name, @table.descriptor.get_name_as_string
#   @table.drop!
# end

  def test_table_properties
    assert_raise(ArgumentError) do
      @table.alter! :hello => :world
    end

    max_fs = 512 * 1024 ** 2
    mem_fs =  64 * 1024 ** 2
    
    @table.alter!(
      :max_filesize       => max_fs,
      :memstore_flushsize => mem_fs,
      :readonly           => false,
      :deferred_log_flush => true
    )
    
    assert_equal max_fs, @table.descriptor.get_max_file_size
    assert_equal mem_fs, @table.descriptor.get_mem_store_flush_size
    assert_equal false,  @table.descriptor.is_read_only
    assert_equal true,   @table.descriptor.is_deferred_log_flush

    @table.drop!
  end

  def test_column_family_alteration!
    assert @table.descriptor.getFamilies.map(&:getNameAsString).include?('cf2')
    @table.delete_family! :cf2
    assert !@table.descriptor.getFamilies.map(&:getNameAsString).include?('cf2')
    @table.add_family! :cf4, {}
    assert @table.descriptor.getFamilies.map(&:getNameAsString).include?('cf4')

    # TODO: test more props
    @table.alter_family! :cf4, :versions => 10
    assert_equal 10, @table.descriptor.getFamilies.select { |cf| cf.getNameAsString == 'cf4' }.first.getMaxVersions

    assert_raise(ArgumentError) {
      @table.alter_family! :cf4, :hello => 'world'
    }
    assert_raise(ArgumentError) {
      @table.alter_family! :cf4, :bloomfilter => :xxx
    }

    @table.drop!
  end

  def test_add_coprocessor!
    coproc = 'org.apache.hadoop.hbase.coprocessor.AggregateImplementation'
    assert_false @table.has_coprocessor? coproc
    @table.add_coprocessor! coproc, :priority => 20000, :params => { :abc => 'def', 'xyz' => 1 }
    assert @table.has_coprocessor? coproc

    # TODO
    assert_raise(NotImplementedError) do
      @table.remove_coprocessor! 'org.apache.hadoop.hbase.coprocessor.AggregateImplementation'
    end

    @table.drop!
  end

  def test_inspect
    @table.drop!
    assert "{NAME => '#{TABLE}'}", @table.inspect # FIXME

    @table.create! :cf => {
      :blockcache          => true,
      :blocksize           => 128 * 1024,
      :bloomfilter         => :row,
      :compression         => :snappy,
    # :data_block_encoding => org.apache.hadoop.hbase.io.encoding.DataBlockEncoding::DIFF,
    # :encode_on_disk      => true,
    # :keep_deleted_cells  => true,
      :in_memory           => true,
      :min_versions        => 5,
      :replication_scope   => 0,
      :ttl                 => 100,
      :versions            => 10,
    }
    props = eval @table.inspect.gsub(/([A-Z_]+) =>/) { ":#{$1.downcase} =>" }
    assert_equal TABLE, props[:name]
    cf = props[:families].first
    assert_equal 'cf', cf[:name]
    assert_equal 'ROW', cf[:bloomfilter]
    assert_equal '0', cf[:replication_scope]
    assert_equal '10', cf[:versions]
    assert_equal 'SNAPPY', cf[:compression]
    assert_equal '5', cf[:min_versions]
    assert_equal '100', cf[:ttl]
    assert_equal '131072', cf[:blocksize]
    assert_equal 'true', cf[:in_memory]
    assert_equal 'true', cf[:blockcache]

    @table.drop!
  end
end

