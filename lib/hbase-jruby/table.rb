require 'bigdecimal'
require 'thread'

class HBase
# @!attribute [r] name
#   @return [String] The name of the table
class Table
  attr_reader :name

  include Enumerable
  include Admin

  # Returns a read-only org.apache.hadoop.hbase.HTableDescriptor object
  # @return [org.apache.hadoop.hbase.client.UnmodifyableHTableDescriptor]
  def descriptor
    htable.get_table_descriptor
  end

  # Closes the table and returns HTable object back to the HTablePool.
  # @return [nil]
  def close
    (Thread.current[:htable] || {}).each do |objid, htable|
      htable.close
    end

    Thread.current[:htable] = {}

    nil
  end

  # Checks if the table of the name exists
  # @return [true, false] Whether table exists
  def exists?
    with_admin { |admin| admin.tableExists @name }
  end

  # Checks if the table is enabled
  # @return [true, false] Whether table is enabled
  def enabled?
    with_admin { |admin| admin.isTableEnabled(@name) }
  end

  # Checks if the table is disabled
  # @return [true, false] Whether table is disabled
  def disabled?
    !enabled?
  end

  # @overload create!(column_family_name, props = {})
  #   Create the table with one column family of the given name
  #   @param [#to_s] The name of the column family
  #   @param [Hash] props Table properties
  #   @return [nil]
  # @overload create!(column_family_hash, props = {})
  #   Create the table with the specified column families
  #   @param [Hash] Column family properties
  #   @param [Hash] props Table properties
  #   @return [nil]
  #   @example
  #     table.create!(
  #       # Column family with default options
  #       :cf1 => {},
  #       # Another column family with custom properties
  #       :cf2 => {
  #         :blockcache         => true,
  #         :blocksize          => 128 * 1024,
  #         :bloomfilter        => :row,
  #         :compression        => :snappy,
  #         :in_memory          => true,
  #         :keep_deleted_cells => true,
  #         :min_versions       => 2,
  #         :replication_scope  => 0,
  #         :ttl                => 100,
  #         :versions           => 5
  #       }
  #     )
  # @overload create!(table_descriptor)
  #   Create the table with the given HTableDescriptor
  #   @param [org.apache.hadoop.hbase.HTableDescriptor] Table descriptor
  #   @return [nil]
  def create! desc, props = {}
    todo = nil
    with_admin do |admin|
      raise RuntimeError, 'Table already exists' if admin.tableExists(@name)

      case desc
      when HTableDescriptor
        patch_table_descriptor! desc, props
        admin.createTable desc
      when Symbol, String
        todo = lambda { create!({desc => {}}, props) }
      when Hash
        htd = HTableDescriptor.new(@name.to_java_bytes)
        patch_table_descriptor! htd, props
        desc.each do |name, opts|
          htd.addFamily hcd(name, opts)
        end

        admin.createTable htd
      else
        raise ArgumentError, 'Invalid table description'
      end
    end
    todo.call if todo # Avoids mutex relocking
  end

  # Alters the table
  # @param [Hash] props Table properties
  # @return [nil]
  # @example
  #   table.alter!(
  #     :max_filesize       => 512 * 1024 ** 2,
  #     :memstore_flushsize =>  64 * 1024 ** 2,
  #     :readonly           => false,
  #     :deferred_log_flush => true
  #   )
  def alter! props
    with_admin do |admin|
      htd = admin.get_table_descriptor(@name.to_java_bytes)
      patch_table_descriptor! htd, props
      while_disabled(admin) do
        admin.modifyTable @name.to_java_bytes, htd
        wait_async_admin(admin)
      end
    end
  end

  # Adds the column family
  # @param [#to_s] name The name of the column family
  # @param [Hash] opts Column family properties
  # @return [nil]
  def add_family! name, opts
    with_admin do |admin|
      while_disabled(admin) do
        admin.addColumn @name, hcd(name.to_s, opts)
        wait_async_admin(admin)
      end
    end
  end

  # Alters the column family
  # @param [#to_s] name The name of the column family
  # @param [Hash] opts Column family properties
  # @return [nil]
  def alter_family! name, opts
    with_admin do |admin|
      while_disabled(admin) do
        admin.modifyColumn @name, hcd(name.to_s, opts)
        wait_async_admin(admin)
      end
    end
  end

  # Removes the column family
  # @param [#to_s] name The name of the column family
  # @return [nil]
  def delete_family! name
    with_admin do |admin|
      while_disabled(admin) do
        admin.deleteColumn @name, name.to_s
        wait_async_admin(admin)
      end
    end
  end

  # Enables the table
  # @return [nil]
  def enable!
    with_admin do |admin|
      admin.enableTable @name unless admin.isTableEnabled(@name) 
    end
  end

  # Disables the table
  # @return [nil]
  def disable!
    with_admin do |admin|
      admin.disableTable @name if admin.isTableEnabled(@name) 
    end
  end

  # Truncates the table by dropping it and recreating it.
  # @return [nil]
  def truncate!
    htd = htable.get_table_descriptor
    drop!
    create! htd
  end

  # Drops the table
  # @return [nil]
  def drop!
    with_admin do |admin|
      raise RuntimeError, 'Table does not exist' unless admin.tableExists @name

      admin.disableTable @name if admin.isTableEnabled(@name) 
      admin.deleteTable  @name
      close
    end
  end

  # @overload get(rowkey)
  #   Single GET.
  #   Gets a record with the given rowkey. If the record is not found, nil is returned.
  #   @param [Object] rowkey Rowkey
  #   @return [HBase::Result, nil]
  # @overload get(rowkeys)
  #   Batch GET. Gets an array of records with the given rowkeys.
  #   Nonexistent records will be returned as nils.
  #   @param [Array<Object>] rowkeys Rowkeys
  #   @return [Array<HBase::Result>]
  def get rowkeys
    each.get rowkeys
  end

  # @overload put(rowkey, data)
  #   Put operation on a rowkey
  #   @param [Object] rowkey Rowkey
  #   @param [Hash] data Data to put
  #   @return [Fixnum] Number of puts succeeded
  # @overload put(data)
  #   Put operation on multiple rowkeys
  #   @param [Hash<Hash>] data Data to put indexed by rowkeys
  #   @return [Fixnum] Number of puts succeeded
  def put *args
    return put(args.first => args.last) if args.length == 2

    puts = args.first.map { |rowkey, props| putify rowkey, props }
    htable.put puts
    puts.length
  end

  # @overload delete(rowkey)
  #   Deletes a row with the given rowkey
  #   @param [Object] rowkey
  #   @return [nil]
  #   @example
  #     table.delete('a000')
  # @overload delete(rowkey, column_family)
  #   Deletes columns with the given column family from the row
  #   @param [Object] rowkey
  #   @param [String] column_family
  #   @return [nil]
  #   @example
  #     table.delete('a000', 'cf1')
  # @overload delete(rowkey, column)
  #   Deletes a column
  #   @param [Object] rowkey
  #   @param [String, Array] column Column expression in String "FAMILY:QUALIFIER", or in Array [FAMILY, QUALIFIER]
  #   @return [nil]
  #   @example
  #     table.delete('a000', 'cf1:col1')
  # @overload delete(rowkey, column, timestamp)
  #   Deletes a version of a column
  #   @param [Object] rowkey
  #   @param [String, Array] column Column expression in String "FAMILY:QUALIFIER", or in Array [FAMILY, QUALIFIER]
  #   @param [Fixnum] timestamp Timestamp.
  #   @return [nil]
  #   @example
  #     table.delete('a000', 'cf1:col1', 1352978648642)
  # @overload delete(*delete_specs)
  #   Batch deletion
  #   @param [Array<Array>] delete_specs
  #   @return [nil]
  #   @example
  #     table.delete(
  #       ['a000', 'cf1:col1', 1352978648642],
  #       ['a001', 'cf1:col1'],
  #       ['a002', 'cf1'],
  #       ['a003'])
  def delete *args
    specs = args.first.is_a?(Array) ? args : [args]

    htable.delete specs.map { |spec|
      rowkey, cfcq, *ts = spec
      cf, cq = Util.parse_column_name(cfcq) if cfcq

      Delete.new(Util.to_bytes rowkey).tap { |del| 
        if !ts.empty?
          ts.each do |t|
            del.deleteColumn cf, cq, t
          end
        elsif cq
          # Delete all versions
          del.deleteColumns cf, cq
        elsif cf
          del.deleteFamily cf
        end
      }
    }
  end

  # @overload increment(rowkey, column, by)
  #   Atomically increase column value by the specified amount
  #   @param [Object] rowkey Rowkey
  #   @param [String, Array] column Column expression in String "FAMILY:QUALIFIER", or in Array [FAMILY, QUALIFIER]
  #   @param [Fixnum] by Increment amount
  #   @return [Fixnum] Column value after increment
  #   @example
  #     table.increment('a000', 'cf1:col1', 1)
  # @overload increment(rowkey, column_by_hash)
  #   Atomically increase values of multiple columns
  #   @param [Object] rowkey Rowkey
  #   @param [Hash] column_by_hash Column expression to increment amount pairs
  #   @example
  #     table.increment('a000', 'cf1:col1' => 1, 'cf1:col2' => 2)
  def increment rowkey, *args
    if args.first.is_a?(Hash)
      cols = args.first
      htable.increment Increment.new(Util.to_bytes rowkey).tap { |inc|
        cols.each do |col, by|
          cf, cq = Util.parse_column_name(col)
          inc.addColumn cf, cq, by
        end
      }
    else
      col, by = args
      cf, cq = Util.parse_column_name(col)
      htable.incrementColumnValue Util.to_bytes(rowkey), cf, cq, by || 1
    end
  end

  # Returns the count of the rows in the table
  # @return [Fixnum]
  def count
    each.count
  end

  # Scan through the table
  # @yield [HBase::Result] Yields each row in the scope
  # @return [HBase::Scoped]
  def each
    if block_given?
      Scoped.send(:new, self).each { |r| yield r }
    else
      Scoped.send(:new, self)
    end
  end

  # @see HBase::Scoped#range
  # @return [HBase::Scoped]
  def range *key_range
    each.range(*key_range)
  end

  # @see HBase::Scoped#project
  # @return [HBase::Scoped]
  def project *columns
    each.project(*columns)
  end

  # @see HBase::Scoped#filter
  # @return [HBase::Scoped]
  def filter *filters
    each.filter(*filters)
  end

  # @see HBase::Scoped#limit
  # @return [HBase::Scoped]
  def limit rows
    each.limit rows
  end

  # @see HBase::Scoped#versions
  # @return [HBase::Scoped]
  def versions vs
    each.versions vs
  end

  # @see HBase::Scoped#caching
  # @return [HBase::Scoped]
  def caching rows
    each.caching rows
  end

  # @see HBase::Scoped#batch
  # @return [HBase::Scoped]
  def batch b
    each.batch b
  end

  # Returns the underlying org.apache.hadoop.hbase.client.HTable object (local to current thread)
  # @return [org.apache.hadoop.hbase.client.HTable]
  def htable
    # @htable ||= @pool.get_table(@name)
    (local_htables = Thread.current[:htable] ||= {})[object_id] ||
      (local_htables[object_id] = @pool.get_table(@name))
  end

  # Returns table description
  # @return [String] Table description
  def inspect
    if exists?
      htable.get_table_descriptor.to_s
    else
      # FIXME
      "{NAME => '#{@name}'}"
    end
  end

private
  def initialize config, htable_pool, name
    @config  = config
    @pool    = htable_pool
    @name    = name.to_s
    @htable  = nil
  end

  def while_disabled admin
    begin
      admin.disableTable @name if admin.isTableEnabled(@name) 
      yield
    ensure
      admin.enableTable @name
    end
  end

  def putify rowkey, props
    Put.new(Util.to_bytes rowkey).tap { |put|
      props.each do |col, val|
        cf, cq = Util.parse_column_name(col)
        put.add cf, cq, Util.to_bytes(val)
      end
    }
  end

  def hcd name, opts
    method_map = {
      :blockcache          => :setBlockCacheEnabled,
      :blocksize           => :setBlocksize,
      :bloomfilter         => :setBloomFilterType,
      :compression         => :setCompressionType,
      :data_block_encoding => :setDataBlockEncoding,
      :encode_on_disk      => :setEncodeOnDisk,
      :in_memory           => :setInMemory,
      :keep_deleted_cells  => :setKeepDeletedCells,
      :min_versions        => :setMinVersions,
      :replication_scope   => :setScope,
      :ttl                 => :setTimeToLive,
      :versions            => :setMaxVersions,
    }
    HColumnDescriptor.new(name.to_s).tap do |hcd|
      opts.each do |key, val|
        if method_map[key]
          hcd.send method_map[key],
            ({
              :bloomfilter => proc { |v|
                const_shortcut StoreFile::BloomType, v, "Invalid bloom filter type"
              },
              :compression => proc { |v|
                const_shortcut Compression::Algorithm, v, "Invalid compression algorithm"
              }
            }[key] || proc { |a| a }).call(val)
        else
          raise ArgumentError, "Invalid option: #{key}"
        end
      end#opts
    end
  end

  def const_shortcut base, v, message
    vs = v.to_s.upcase.to_sym
    #if base.constants.map { |c| base.const_get c }.include?(v)
    if base.constants.map { |c| base.const_get c }.any? { |cv| v == cv }
      v
    elsif base.constants.include? vs
      base.const_get vs
    else
      raise ArgumentError, [message, v.to_s].join(': ')
    end
  end

  def patch_table_descriptor! htd, props
    props.each do |key, value|
      method = {
        :max_filesize       => :setMaxFileSize,
        :readonly           => :setReadOnly,
        :memstore_flushsize => :setMemStoreFlushSize,
        :deferred_log_flush => :setDeferredLogFlush
      }[key]
      raise ArgumentError, "Invalid table property: #{key}" unless method

      htd.send method, value
    end
    htd
  end
end#Table
end#HBase

