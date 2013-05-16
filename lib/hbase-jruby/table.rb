require 'bigdecimal'
require 'thread'

class HBase
# @!attribute [r] name
#   @return [String] The name of the table
# @!attribute [r] config
#   @return [org.apache.hadoop.conf.Configuration]
class Table
  attr_reader :name
  attr_reader :config
  attr_reader :schema

  include Enumerable
  include Admin
  include Scoped::Aggregation::Admin
  include HBase::Util

  # (INTERNAL) Returns the underlying org.apache.hadoop.hbase.client.HTable object (local to current thread)
  # @return [org.apache.hadoop.hbase.client.PooledHTable]
  def htable
    check_closed

    # [:hbase_jruby][HBase connection][Table name]
    local_vars = Thread.current[:hbase_jruby] ||= {}
    local_htables = local_vars[@hbase] ||= {}
    local_htables[@name] ||= @pool.get_table(@name)
  end

  # @deprecated
  # @return [nil]
  def close
    nil
  end

  # Returns whether if the connection is closed
  # @return [Boolean]
  def closed?
    @hbase.closed?
  end

  [:get, :count, :aggregate,
   :range, :project, :filter, :while,
   :limit, :versions, :caching, :batch,
   :time_range, :at
  ].each do |method|
    define_method(method) do |*args|
      self.scoped.send(method, *args)
    end
  end

  def with_java_scan &block
    self.scoped.with_java_scan(&block)
  end

  def with_java_get &block
    self.scoped.with_java_get(&block)
  end

  # Performs PUT operations
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
    check_closed

    return put(args.first => args.last) if args.length == 2

    puts = args.first.map { |rowkey, props| putify rowkey, props }
    htable.put puts
    puts.length
  end

  # Deletes data
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
  # @overload delete(rowkey, column, *timestamps)
  #   Deletes specified versions of a column
  #   @param [Object] rowkey
  #   @param [String, Array] column Column expression in String "FAMILY:QUALIFIER", or in Array [FAMILY, QUALIFIER]
  #   @param [*Fixnum] timestamps Timestamps.
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
    check_closed

    specs = args.first.is_a?(Array) ? args : [args]

    htable.delete specs.map { |spec|
      rowkey, cfcq, *ts = spec
      cf, cq = Util.parse_column_name(cfcq) if cfcq

      Delete.new(Util.to_bytes rowkey).tap { |del|
        if !ts.empty?
          ts.each do |t|
            del.deleteColumn cf, cq, time_to_long(t)
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

  # Delete rows.
  # @param [*Object] rowkeys List of rowkeys of rows to delete
  # @return [nil]
  def delete_row *rowkeys
    check_closed

    htable.delete rowkeys.map { |rk| Delete.new(Util.to_bytes rk) }
  end

  # Atomically increase numeric values
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
    check_closed

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

  # Scan through the table
  # @yield [row] Yields each row in the scope
  # @yieldparam [HBase::Row] row
  # @return [Enumerator]
  def each &block
    scoped.each(&block)
  end

  # Returns HBase::Scoped object for this table
  # @return [HBase::Scoped]
  def scoped
    Scoped.send(:new, self)
  end

  def schema= definition
    lookup = empty_lookup_table
    definition.each do |cf, cols|
      # Column => Type
      if cf.to_s.index ':'
        if cols.is_a?(Symbol)
          lookup[:type][:exact][cf]        = cols
          lookup[:type][:exact][cf.to_sym] = cols
          lookup[:type][:exact][cf.to_s]   = cols
        else
          raise ArgumentError, "invalid schema"
        end
      # Family => { Column => Type }
      else
        # Type
        if cols.is_a?(Symbol) || cols.is_a?(String)
          lookup[:type][:pattern][/^#{cf}:/] = cols
        # Qualifiers
        elsif cols.is_a?(Hash)
          cols.each do |cq, type|
            raise ArgumentError, "invalid schema" unless type.is_a?(Symbol)

            # Pattern
            if cq.is_a?(Regexp)
              lookup[:name][:pattern][cq] = cf
              lookup[:type][:pattern][cq] = type
            # Exact
            else
              [cq, cq.to_s, cq.to_sym, [cf, cq].join(':')].each do |key|
                lookup[:name][:exact][key] = [cf, cq].join ':'
                lookup[:type][:exact][key] = type
              end
            end
          end
        else
          raise ArgumentError, "invalid schema"
        end
      end
    end

    @lookup = lookup
    @schema = definition.dup
  end

  # @private
  def fullname_of? cq
    lookup(@lookup[:name], cq) { |partial, input|
      [partial, input].join ':'
    } || cq
  end

  # @private
  def type_of? cq
    lookup @lookup[:type], cq
  end

private
  def lookup dic, cq
    2.times do |i|
      if match = dic[:exact][cq]
        return match
      elsif pair = dic[:pattern].find { |k, v| cq.to_s =~ k }
        return block_given? ? yield(pair[1], cq) : pair[1]
      end

      break if i == 1
      cq = cq.to_s.split(':')[1]
      break if cq.nil?
    end
    nil
  end

  def initialize hbase, config, htable_pool, name
    @hbase  = hbase
    @config = config
    @pool   = htable_pool
    @name   = name.to_s
    @schema = {}
    @lookup = empty_lookup_table
  end

  def empty_lookup_table
    {
      :name => {
        :exact   => {},
        :pattern => {},
      },
      :type => {
        :exact   => {},
        :pattern => {},
      }
    }
  end

  def check_closed
    raise RuntimeError, "HBase connection is already closed" if @hbase.closed?
  end

  def putify rowkey, props
    Put.new(Util.to_bytes rowkey).tap { |put|
      props.each do |col, val|
        cf, cq = Util.parse_column_name(fullname_of? col)
        type   = type_of? col

        case val
        when Hash
          val.each do |t, v|
            case t
            # Timestamp / Ruby Time
            when Time, Fixnum
              put.add cf, cq, time_to_long(t), Util.to_typed_bytes(type, v)
            # Types: :byte, :short, :int, ...
            else
              put.add cf, cq, Util.to_typed_bytes(t, v)
            end
          end
        else
          put.add cf, cq, Util.to_typed_bytes(type, val)
        end
      end
    }
  end
end#Table
end#HBase

