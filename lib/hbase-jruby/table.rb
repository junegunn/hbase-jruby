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

    case args.length
    when 1
      puts = args.first.map { |rowkey, props| make_put rowkey, props }
      htable.put puts
      puts.length
    when 2
      htable.put make_put(*args)
      1
    else
      raise ArgumentError, 'invalid number of arguments'
    end
  end

  # Deletes data
  # @overload delete(rowkey)
  #   Deletes a row with the given rowkey
  #   @param [Object] rowkey
  #   @return [nil]
  #   @example
  #     table.delete('a000')
  # @overload delete(rowkey, *extra)
  #   Deletes columns in the row.
  #   @param [Object] rowkey
  #   @param [*Object] extra [Family|Qualifier [Timestamp ...]] ...
  #   @return [nil]
  #   @example
  #     # A column (with schema)
  #     table.delete('a000', :title)
  #     # Two columns (with schema)
  #     table.delete('a000', :title, :author)
  #     # A column (without schema)
  #     table.delete('a000', 'cf1:col1')
  #     # Columns in cf1 family
  #     table.delete('a000', 'cf1')
  #     # A version
  #     table.delete('a000', :author, 1352978648642)
  #     # Two versions
  #     table.delete('a000', :author, 1352978648642, 1352978647642)
  #     # Combination of columns and versions
  #     table.delete('a000', :author, 1352978648642, 1352978647642,
  #                          :title,
  #                          :image, 1352978648642)
  # @overload delete(*delete_specs)
  #   Batch deletion
  #   @param [*Array] delete_specs
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

    htable.delete specs.map { |spec| spec.empty? ? nil : make_delete(*spec) }.compact
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
  # @overload increment(inc_spec)
  #   Increase values of multiple columns from multiple rows.
  #   @param [Hash] inc_spec { rowkey => { col => by } }
  #   @example
  #     table.increment 'a000' => { 'cf1:col1' => 1, 'cf1:col2' => 2 },
  #                     'a001' => { 'cf1:col1' => 3, 'cf1:col2' => 4 }
  def increment rowkey, *args
    check_closed

    if args.empty? && rowkey.is_a?(Hash)
      rowkey.each do |key, spec|
        increment key, spec
      end
    elsif args.first.is_a?(Hash)
      cols = args.first
      htable.increment Increment.new(Util.to_bytes rowkey).tap { |inc|
        cols.each do |col, by|
          cf, cq, _ = lookup_and_parse col
          inc.addColumn cf, cq, by
        end
      }
    else
      col, by = args
      cf, cq = lookup_and_parse col
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

  # Returns CheckedOperation instance for check-and-put and check-and-delete
  # @param [Object] rowkey
  # @param [Hash] cond
  # @return [HBase::Table::CheckedOperation]
  def check rowkey, cond
    raise ArgumentError, "invalid check condition" unless cond.length == 1
    col, val = cond.first

    cf, cq, type = lookup_and_parse(col)

    CheckedOperation.new self, Util.to_bytes(rowkey),
      cf, cq,
      (val.nil? ? nil : Util.to_typed_bytes(type, val))
  end

  # @private
  def lookup_schema col
    @hbase.schema.lookup @name_sym, col
  end

  # @private
  def lookup_and_parse col
    @hbase.schema.lookup_and_parse @name_sym, col
  end

private
  def initialize hbase, config, htable_pool, name
    @hbase    = hbase
    @config   = config
    @pool     = htable_pool
    @name     = name.to_s
    @name_sym = name.to_sym
  end

  def check_closed
    raise RuntimeError, "HBase connection is already closed" if @hbase.closed?
  end

  def make_put rowkey, props
    Put.new(Util.to_bytes rowkey).tap { |put|
      props.each do |col, val|
        cf, cq, type = lookup_and_parse col

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

  def make_delete rowkey, *extra
    Delete.new(Util.to_bytes rowkey).tap { |del|
      cf = cq = nil
      prcd = false

      prc = lambda do
        unless prcd
          if cq
            # Delete all versions
            del.deleteColumns cf, cq
          elsif cf
            del.deleteFamily cf
          end
        end
      end

      extra.each do |x|
        case x
        when Fixnum, Time
          if cq
            del.deleteColumn cf, cq, time_to_long(x)
            prcd = true
          else
            raise ArgumentError, 'qualifier not given'
          end
        else
          prc.call
          cf, cq, _ = lookup_and_parse x
          prcd = false
        end
      end
      prc.call
    }
  end
end#Table
end#HBase

