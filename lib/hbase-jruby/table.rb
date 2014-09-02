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
      local_htables = local_vars[@hbase] = {}
    end
    local_htables[@name] ||= @hbase.send :get_htable, @name
  end

  # Return HTable instance back to the table pool.
  # Generally this is not required unless you use unlimited number of threads
  # @return [nil]
  def close
    check_closed

    (t = Thread.current[:hbase_jruby]) &&
    (t = t[@hbase]) &&
    (t = t.delete @name) &&
    t.close
  end

  # Returns whether if the connection is closed
  # @return [Boolean]
  def closed?
    @hbase.closed?
  end

  [:get, :count, :aggregate,
   :range, :project, :filter, :while,
   :limit, :versions, :caching, # :batch,
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

    list = specs.map { |spec| spec.empty? ? nil : @mutation.delete(*spec) }.compact
    if list.length == 1
      htable.delete list.first
    else
      htable.delete list
    end
  end

  # Delete rows.
  # @param [*Object] rowkeys List of rowkeys of rows to delete
  # @return [nil]
  def delete_row *rowkeys
    list = rowkeys.map { |rk| Delete.new(Util.to_bytes rk) }
    if list.length == 1
      htable.delete list.first
    else
      htable.delete list
    end
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

  # Appends values to one or more columns within a single row.
  # @param [Object] rowkey Rowkey
  # @param [Hash] spec Hash (column to value)
  # @return [Hash] Updated values
  # @example
  #   table.put :rowkey, col1: 'hello', col2: 'foo'
  #   table.append :rowkey, col1: ' world', col2: 'bar'
  #     # { col1: 'hello world', col2: 'foobar' }
  def append rowkey, spec
    result = htable.append @mutation.append(rowkey, spec)
    Row.send(:new, self, result).to_h if result # (maybe null)
  end

  # Performs multiple mutations atomically on a single row.
  # Currently Put and Delete are supported.
  # The mutations are performed in the order in which they were specified.
  # @param [Object] rowkey Rowkey
  # @yield [HBase::Table::Mutation::Mutator]
  # @return [nil]
  # @example
  #   table.mutate do |m|
  #     m.put a: 100, b: 'hello'
  #     m.delete :c, :d
  #     m.put e: 3.14
  #   end
  def mutate(rowkey, &block)
    ms = @mutation.mutate(rowkey, &block)
    htable.mutateRow ms if ms
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
    Scoped.send(:new, self, @hbase.config.get('hbase.client.scanner.caching').to_i)
  end

  # Returns CheckedOperation instance for check-and-put and check-and-delete
  # @param [Object] rowkey
  # @param [Hash] cond
  # @return [HBase::Table::CheckedOperation]
  def check rowkey, cond
    raise ArgumentError, 'invalid check condition' unless cond.length == 1
    col, val = cond.first

    cf, cq, type = lookup_and_parse(col, true)

    # If the passed value is null, the check is for the lack of column
    CheckedOperation.new self, @mutation, Util.to_bytes(rowkey),
      cf, cq,
      (val.nil? ? nil : Util.to_typed_bytes(type, val))
  end

  # Method that does a batch call on Deletes, Gets, Puts, Increments, Appends
  # and RowMutations. The ordering of execution of the actions is not defined.
  # An Array of Hashes are returned as the results of each operation. For
  # Delete, Put, and RowMutation, :result entry of the returned Hash is
  # Boolean. For Increment and Append, it will be plain Hashes, and for Get,
  # HBase::Rows will be returned.
  # When an error has occurred, you can still access the partial results using
  # `results` method of the thrown BatchException instance.
  # @yield [HBase::Table::BatchAction]
  # @return [Array<Hash>]
  # @raise [HBase::BatchException]
  def batch arg = nil
    if arg
      # Backward compatibility
      return scoped.batch(arg)
    else
      raise ArgumentError, "Block not given" unless block_given?
    end
    b = BatchAction.send(:new, self, @mutation)
    yield b
    results = Array.new(b.actions.length).to_java
    process = lambda do
      results.each_with_index do |r, idx|
        action = b.actions[idx]
        type = action[:type]
        case type
        when :get
          action[:result] = (r.nil? || r.empty?) ? nil : Row.send(:new, self, r)
        when :append
          action[:result] = r && Row.send(:new, self, r).to_h
        when :increment
          action[:result] = r &&
            Row.send(:new, self, r).to_h.tap { |h|
              h.each do |k, v|
                h[k] = Util.from_bytes :fixnum, v unless v.is_a?(Fixnum)
              end
            }
        else
          case r
          when java.lang.Exception
            action[:result] = false
            action[:exception] = r
          when nil
            action[:result] = false
          else
            action[:result] = true
          end
        end
      end
      b.actions
    end

    begin
      htable.batch b.actions.map { |a| a[:action] }, results
      process.call
    rescue Exception => e
      raise HBase::BatchException.new(e, process.call)
    end
  end

  # @private
  def lookup_schema col
    @hbase.schema.lookup @name_sym, col
  end

  # @private
  def lookup_and_parse col, expect_cq
    @hbase.schema.lookup_and_parse @name_sym, col, expect_cq
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

