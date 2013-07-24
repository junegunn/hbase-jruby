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
    unless local_htables = local_vars[@hbase]
      @hbase.send :register_thread, Thread.current
      local_htables = local_vars[@hbase] = {}
    end
    local_htables[@name] ||= @hbase.send :get_htable, @name
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
    case args.length
    when 1
      puts = args.first.map { |rowkey, props| @mutation.put rowkey, props }
      htable.put puts
      puts.length
    when 2
      htable.put @mutation.put(*args)
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
    specs = args.first.is_a?(Array) ? args : [args]

    htable.delete specs.map { |spec| spec.empty? ? nil : @mutation.delete(*spec) }.compact
  end

  # Delete rows.
  # @param [*Object] rowkeys List of rowkeys of rows to delete
  # @return [nil]
  def delete_row *rowkeys
    htable.delete rowkeys.map { |rk| Delete.new(Util.to_bytes rk) }
  end

  # Atomically increase numeric values
  # @overload increment(rowkey, column, by)
  #   Atomically increase column value by the specified amount
  #   @param [Object] rowkey Rowkey
  #   @param [String, Array] column Column expression in String "FAMILY:QUALIFIER", or in Array [FAMILY, QUALIFIER]
  #   @param [Fixnum] by Increment amount
  #   @return [Hash]
  #   @example
  #     table.increment('a000', 'cf1:col1', 1)
  # @overload increment(rowkey, column_by_hash)
  #   Atomically increase values of multiple columns
  #   @param [Object] rowkey Rowkey
  #   @param [Hash] column_by_hash Column expression to increment amount pairs
  #   @return [Hash]
  #   @example
  #     table.increment('a000', 'cf1:col1' => 1, 'cf1:col2' => 2)
  # @overload increment(inc_spec)
  #   Increase values of multiple columns from multiple rows.
  #   @param [Hash] inc_spec { rowkey => { col => by } }
  #   @return [Hash]
  #   @example
  #     table.increment 'a000' => { 'cf1:col1' => 1, 'cf1:col2' => 2 },
  #                     'a001' => { 'cf1:col1' => 3, 'cf1:col2' => 4 }
  def increment rowkey, *args
    if args.empty? && rowkey.is_a?(Hash)
      INSENSITIVE_ROW_HASH.clone.tap { |result|
        rowkey.each do |key, spec|
          result[Util.java_bytes?(key) ? ByteArray[key] : key] = increment(key, spec)
        end
      }
    else
      incr = @mutation.increment(rowkey, *args)
      Row.send(:new, self, htable.increment(incr)).to_h.tap { |h|
        h.each do |k, v|
          h[k] = Util.from_bytes :fixnum, v unless v.is_a?(Fixnum)
        end
      }
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
    raise ArgumentError, 'invalid check condition' unless cond.length == 1
    col, val = cond.first

    cf, cq, type = lookup_and_parse(col)

    # If the passed value is null, the check is for the lack of column
    CheckedOperation.new self, @mutation, Util.to_bytes(rowkey),
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
  def initialize hbase, config, name
    @hbase    = hbase
    @config   = config
    @name     = name.to_s
    @name_sym = name.to_sym
    @mutation = Mutation.new(self)
  end

  def check_closed
    raise RuntimeError, "HBase connection is already closed" if @hbase.closed?
  end

  INSENSITIVE_ROW_HASH = {}.tap { |h|
    h.instance_eval do
      def [] key
        if Util.java_bytes?(key)
          key = ByteArray[key]
        end
        super key
      end
    end
  }
end#Table
end#HBase

