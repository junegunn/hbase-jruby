require 'bigdecimal'

class HBase
# @!attribute [r] name
#   @return [String] The name of the table
class Table
  attr_reader :name

  include Enumerable

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
      method_map = {
        :BLOCKCACHE          => :setBlockCacheEnabled,
        :BLOCKSIZE           => :setBlocksize,
        :BLOOMFILTER         => :setBloomFilterType,
        :COMPRESSION         => :setCompressionType,
        :DATA_BLOCK_ENCODING => :setDataBlockEncoding,
        :ENCODE_ON_DISK      => :setEncodeOnDisk,
        :IN_MEMORY           => :setInMemory,
        :KEEP_DELETED_CELLS  => :setKeepDeletedCells,
        :MIN_VERSIONS        => :setMinVersions,
        :REPLICATION_SCOPE   => :setScope,
        :TTL                 => :setTimeToLive,
        :VERSIONS            => :setMaxVersions,
      }
      desc.each do |fam, opts|
        hcd = HColumnDescriptor.new(fam.to_s)

        opts.each do |key, val|
          k = key.to_s.upcase.to_sym
          if method_map[k]
            v =
              ({
                :BLOOMFILTER => proc { |v|
                  const_shortcut BloomType, v, "Invalid bloom filter type"
                },
                :COMPRESSION => proc { |v|
                  const_shortcut Algorithm, v, "Invalid compression algorithm"
                }
              }[k] || proc { |a| a }).call(val)

            hcd.send method_map[k], v
          else
            raise ArgumentError, "Invalid option: #{key}"
          end
        end#opts

        htd.addFamily hcd
      end

      @admin.createTable htd
    else
      raise ArgumentError, 'Invalid table description'
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

  # Truncates the table
  # @return [nil]
  def truncate!
    @admin.truncate @name
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
      # TODO options?
      opts    = rowkeys.last
      rowkeys = rowkeys[0...-1]
    end
    ret = htable.get(rowkeys.map { |rk| getify rk, opts }).map { |result|
      result.isEmpty ? nil : Result.new(self, result)
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

      Delete.new(encode_rowkey rowkey).tap { |del| 
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
      htable.increment Increment.new(encode_rowkey rowkey).tap { |inc|
        cols.each do |col, by|
          cf, cq = Util.parse_column_name(col)
          inc.addColumn cf, cq, by
        end
      }
    else
      col, by = args
      cf, cq = Util.parse_column_name(col)
      htable.incrementColumnValue encode_rowkey(rowkey), cf, cq, by || 1
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

  # Encodes rowkey
  # @param [Object] rowkey
  # @return [byte[]] Byte array representation of the rowkey
  def encode_rowkey rowkey
    if @stringify_rowkey
      rowkey.to_s.to_java_bytes
    else
      # It's up to you
      rowkey
    end
  end

  # Decodes rowkey
  # @param [byte[]] rowkey Byte array representation of the rowkey
  # @return [String, byte[]]
  def decode_rowkey rowkey
    if @stringify_rowkey
      Bytes.to_string rowkey
    else
      # It's up to you
      rowkey
    end
  end

  # Returns table description
  # @return [String] Table description
  def inspect
    htable.get_table_descriptor.to_s
  end

private
  def initialize admin, htable_pool, name, stringify_rowkey
    @admin   = admin
    @pool    = htable_pool
    @name    = name.to_s
    @stringify_rowkey = stringify_rowkey
  end

  def getify rowkey, opts = {}
    Get.new(encode_rowkey rowkey).tap { |get|
      if opts && opts[:versions]
        get.setMaxVersions opts[:versions]
      else
        get.setMaxVersions
      end
    }
  end

  def putify rowkey, props
    Put.new(encode_rowkey rowkey).tap { |put|
      props.each do |col, val|
        cf, cq = Util.parse_column_name(col)
        put.add cf, cq, Util.to_bytes(val)
      end
    }
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

