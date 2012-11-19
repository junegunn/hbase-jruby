require 'bigdecimal'

class HBase
# @!attribute [r] name
#   @return [String] The name of the table
class Table
  attr_reader :name

  include Enumerable

  # Returns a read-only org.apache.hadoop.hbase.HTableDescriptor object
  # @return [org.apache.hadoop.hbase.client.UnmodifyableHTableDescriptor]
  def descriptor
    htable.get_table_descriptor
  end

  # Closes the table and returns HTable object back to the HTablePool.
  # @return [nil]
  def close
    @htable.close if @htable
    @htable = nil
  end

  # Checks if the table of the name exists
  # @return [true, false] Whether table exists
  def exists?
    @admin.tableExists @name
  end

  # Checks if the table is enabled
  # @return [true, false] Whether table is enabled
  def enabled?
    @admin.isTableEnabled(@name)
  end

  # Checks if the table is disabled
  # @return [true, false] Whether table is disabled
  def disabled?
    !enabled?
  end

  # @overload create!(column_family_name)
  #   Create the table with one column family of the given name
  #   @param [String, Symbol] The name of the column family
  #   @return [nil]
  # @overload create!(column_family_hash)
  #   Create the table with the specified column families
  #   @param [Hash] Column family properties
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
  def create! desc
    raise RuntimeError, 'Table already exists' if exists?

    case desc
    when HTableDescriptor
      @admin.createTable desc
    when Symbol, String
      create!(desc => {})
    when Hash
      htd = HTableDescriptor.new(@name.to_java_bytes)
      desc.each do |name, opts|
        htd.addFamily hcd(name, opts)
      end

      @admin.createTable htd
    else
      raise ArgumentError, 'Invalid table description'
    end
  end

# # Renames the table (FIXME DOESN'T WORK)
# # @param [#to_s] New name
# # @return [String] New name
# def rename! new_name
#   new_name = new_name.to_s
#   htd = @admin.get_table_descriptor(@name.to_java_bytes)
#   htd.setName new_name.to_java_bytes

#   while_disabled do
#     @admin.modifyTable @name.to_java_bytes, htd
#     @name = new_name
#   end
# end

  # Alters the table
  # @param [Hash] Table parameters
  # @return [nil]
  # @example
  #   table.alter!(
  #     :max_filesize       => 512 * 1024 ** 2,
  #     :memstore_flushsize =>  64 * 1024 ** 2,
  #     :readonly           => false,
  #     :deferred_log_flush => true
  #   )
  def alter! table_attrs
    htd = @admin.get_table_descriptor(@name.to_java_bytes)
    table_attrs.each do |key, value|
      method = {
        :max_filesize       => :setMaxFileSize,
        :readonly           => :setReadOnly,
        :memstore_flushsize => :setMemStoreFlushSize,
        :deferred_log_flush => :setDeferredLogFlush
      }[key]
      raise ArgumentError, "Invalid option: #{key}" unless method

      htd.send method, value
    end
    while_disabled do
      @admin.modifyTable @name.to_java_bytes, htd
    end
  end

  # Adds the column family
  # @param [#to_s] name The name of the column family
  # @param [Hash] opts Column family properties
  # @return [nil]
  def add_family! name, opts
    while_disabled do
      @admin.addColumn @name, hcd(name.to_s, opts)
    end
  end

  # Alters the column family
  # @param [#to_s] name The name of the column family
  # @param [Hash] opts Column family properties
  # @return [nil]
  def alter_family! name, opts
    while_disabled do
      @admin.modifyColumn @name, hcd(name.to_s, opts)
    end
  end

  # Removes the column family
  # @param [#to_s] name The name of the column family
  # @return [nil]
  def delete_family! name
    while_disabled do
      @admin.deleteColumn @name, name.to_s
    end
  end

  # Enables the table
  # @return [nil]
  def enable!
    @admin.enableTable @name unless enabled?
  end

  # Disables the table
  # @return [nil]
  def disable!
    @admin.disableTable @name if enabled?
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
    raise RuntimeError, 'Table does not exist' unless exists?

    disable!
    @admin.deleteTable @name
  end

  # @overload get(rowkey, opts = {})
  #   Gets a record with the given rowkey. If the record is not found, nil is returned.
  #   @param [Object] rowkey Rowkey
  #   @param [Hash] opts
  #   @option opts [Fixnum] :versions Number of versions to fetch
  #   @return [HBase::Result, nil]
  # @overload get(*rowkeys, opts = {})
  #   Gets an array of records with the given rowkeys. Nonexistent records will be returned as nils.
  #   @param [Hash] opts
  #   @option opts [Fixnum] :versions Number of versions to fetch
  #   @return [Array<HBase::Result>]
  def get *rowkeys
    if rowkeys.last.is_a?(Hash)
      opts    = rowkeys.last
      rowkeys = rowkeys[0...-1]
    end
    ret = htable.get(rowkeys.map { |rk| getify rk, opts }).map { |result|
      result.isEmpty ? nil : Result.new(result)
    }
    ret.length == 1 ? ret.first : ret
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
  #   @param [String] column Column expression: "FAMILY:QUALIFIER"
  #   @return [nil]
  #   @example
  #     table.delete('a000', 'cf1:col1')
  # @overload delete(rowkey, column, timestamp)
  #   Deletes a version of a column
  #   @param [Object] rowkey
  #   @param [String] column Column expression: "FAMILY:QUALIFIER"
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
  #   @param [String] column Column expression: "FAMILY:QUALIFIER"
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
    each.range *key_range
  end

  # @see HBase::Scoped#project
  # @return [HBase::Scoped]
  def project *columns
    each.project *columns
  end

  # @see HBase::Scoped#filter
  # @return [HBase::Scoped]
  def filter *filters
    each.filter *filters
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

  # Returns the underlying org.apache.hadoop.hbase.client.HTable object
  # @return [org.apache.hadoop.hbase.client.HTable]
  def htable
    @htable ||= @pool.get_table(@name)
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
  def initialize admin, htable_pool, name
    @admin   = admin
    @pool    = htable_pool
    @name    = name.to_s
  end

  def while_disabled
    begin
      disable!
      yield
    ensure
      enable!
    end
  end

  def getify rowkey, opts = {}
    Get.new(Util.to_bytes rowkey).tap { |get|
      if opts && opts[:versions]
        get.setMaxVersions opts[:versions]
      else
        get.setMaxVersions
      end
    }
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
          v =
            ({
              :bloomfilter => proc { |v|
                const_shortcut BloomType, v, "Invalid bloom filter type"
              },
              :compression => proc { |v|
                const_shortcut Algorithm, v, "Invalid compression algorithm"
              }
            }[key] || proc { |a| a }).call(val)

          hcd.send method_map[key], v
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
end#Table
end#HBase

