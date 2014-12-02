class HBase
# Scope of data access
# @author Junegunn Choi <junegunn.c@gmail.com>
# @!attribute [r] table
#   @return [HBase::Table] HBase::Table instance for this scope
class Scoped
  include Enumerable
  include Scoped::Aggregation
  include HBase::Util

  attr_reader :table

  # A clean HBase::Scoped object for the same table
  # @return [HBase::Scope] A clean HBase::Scoped object for the same table
  def unscope
    Scoped.send(:new, @table, @dcaching)
  end

  # Count the number of rows in the scope
  # @return [Fixnum, Bignum] The number of rows in the scope
  # @param [Hash] options Counting options
  # @option options [Fixnum|nil] :caching The number of rows for caching that will be passed to scanners.
  #   Use higher values for faster scan.
  # @option options [Boolean] :cache_blocks Whether blocks should be cached for this scan
  def count options = {}
    options = { :caching      => nil,
                :cache_blocks => true }.merge(options)

    scan = block_given? ? filtered_scan : filtered_scan_minimum
    scan.cache_blocks = options[:cache_blocks]
    if options[:caching] && (@limit.nil? || options[:caching] < @limit)
      scan.caching = options[:caching]
    end

    cnt = 0
    if block_given?
      iterate(scan) do |result|
        cnt += 1 if yield(Row.send(:new, @table, result))
      end
    else
      iterate(scan) { |r| cnt += 1 }
    end
    cnt
  end

  # Performs GET operations
  # @overload get(rowkey, columns = [])
  #   Single GET. Returns all columns unless specified.
  #   Gets a record with the given rowkey. If the record is not found, nil is returned.
  #   @param [Object] rowkey Rowkey
  #   @param [Array<Symbol|String>] columns Schema symbols or "CF" or "CF:CQ"
  #   @return [HBase::Row, nil]
  # @overload get(rowkeys, columns = [])
  #   Batch GET. Gets an array of records with the given rowkeys.
  #   Nonexistent records will be returned as nils.
  #   @param [Array<Object>] *rowkeys Rowkeys
  #   @param [Array<Symbol|String>] columns Schema symbols or "CF" or "CF:CQ"
  #   @return [Array<HBase::Row>]
  def get rowkeys, columns: []
    case rowkeys
    when Array
      htable.get(rowkeys.map { |rk| getify(rk, columns) }).map { |result|
        result.isEmpty ? nil : Row.send(:new, @table, result)
      }
    else
      result = htable.get(getify rowkeys, columns)
      result.isEmpty ? nil : Row.send(:new, @table, result)
    end
  end

  # Iterate through the scope.
  # @yield [row] Yields each row in the scope
  # @yieldparam [HBase::Row] row
  def each
    return enum_for(:each) unless block_given?

    iterate(filtered_scan) do |result|
      yield Row.send(:new, @table, result)
    end
  end

  # Sets the number of rows for caching that will be passed to scanners.
  # @param [Fixnum] rows The number of rows to cache
  # @return [HBase::Scoped] HBase::Scoped object with the caching option
  def caching rows
    raise ArgumentError, "Invalid caching size. Must be a non-negative integer." unless rows.is_a?(Fixnum) && rows >= 0
    spawn :@caching, rows
  end

  # @overload range(start_key, opts = {})
  #   Returns an HBase::Scoped object with the specified rowkey range
  #   Overrides current range.
  #   @param [Object] start_key Start rowkey
  #   @param [Hash] opts Prefix filter
  #   @option opts [Object, Array<Object>] :prefix Only rows matching any of the given prefixes are returned
  #   @return [HBase::Scoped] HBase::Scoped object with the range
  # @overload range(start_key, stop_key, opts = {})
  #   Returns an HBase::Scoped object with the specified rowkey range
  #   Overrides current range.
  #   @param [Object, nil] start_key Start rowkey. Can be nil.
  #   @param [Object] stop_key Stop rowkey (exclusive)
  #   @param [Hash] opts Prefix filter
  #   @option opts [Object, Array<Object>] :prefix Only rows matching any of the given prefixes are returned
  #   @return [HBase::Scoped] HBase::Scoped object with the range
  # @overload range(start_stop_range, opts = {})
  #   Returns an HBase::Scoped object with the specified rowkey range
  #   Overrides current range.
  #   @param [Range] start_stop_range Rowkey scan range
  #   @param [Hash] opts Prefix filter
  #   @option opts [Object, Array<Object>] :prefix Only rows matching any of the given prefixes are returned
  #   @return [HBase::Scoped] HBase::Scoped object with the range
  # @overload range(opts)
  #   Returns an HBase::Scoped object with the specified rowkey range
  #   Overrides current range.
  #   @param [Hash] opts Prefix filter
  #   @option opts [Object, Array<Object>] :prefix Only rows matching any of the given prefixes are returned
  #   @return [HBase::Scoped] HBase::Scoped object with the range
  #   @example
  #     table.range(:prefix => '2012')
  #     table.range(:prefix => ['2010', '2012'])
  def range *key_range
    if (last = key_range.last).is_a?(Hash)
      prefixes = arrayfy(last[:prefix]).compact
      last = last.reject { |k, v| k == :prefix }

      key_range = key_range[0...-1] # defensive
      key_range << last unless last.empty?
    else
      prefixes = []
    end

    if key_range[0].is_a?(Range)
      raise ArgumentError, "Invalid range" unless key_range.length == 1
    elsif !prefixes.empty?
      raise ArgumentError, "Invalid range" unless [0, 1, 2].include?(key_range.length)
    else
      raise ArgumentError, "Invalid range" unless [1, 2].include?(key_range.length)
    end

    raise ArgumentError, "Invalid range" if !key_range.empty? && key_range.all? { |e| e.nil? }

    spawn :@range,
          key_range[0].is_a?(Range) ?
              key_range[0] :
              (key_range.empty? ? nil : key_range.map { |e| e.nil? ? nil : Util.to_bytes(e) }),
          :@prefixes,
          prefixes
  end

  # Returns an HBase::Scoped object with the filters added
  # @param [Array<Hash, FilterBase, FilterList>] filters
  # @return [HBase::Scoped] HBase::Scoped object also with the specified filters
  def filter *filters
    spawn :@filters, @filters + parse_filter_input(filters)
  end

  # Returns an HBase::Scoped object with the additional filters which will cause early termination of scan
  # @param [Array<Hash, FilterBase, FilterList>] filters
  # @return [HBase::Scoped]HBase::Scoped object with the additional filters which will cause early termination of scan
  def while *filters
    spawn :@filters, @filters + parse_filter_input(filters).map { |filter| WhileMatchFilter.new(filter) }
  end

  # Returns an HBase::Scoped object with the specified row number limit
  # @param [Fixnum|nil] rows Sets the maximum number of rows to return from scan
  # @return [HBase::Scoped] HBase::Scoped object with the specified row number limit
  def limit rows
    unless (rows.is_a?(Fixnum) && rows >= 0) || rows.nil?
      raise ArgumentError, "Invalid limit. Must be a non-negative integer or nil."
    end
    spawn :@limit, rows
  end

  # Returns an HBase::Scoped object with the specified time range
  # @param [Fixnum|Time] min Minimum timestamp (inclusive)
  # @param [Fixnum|Time] max Maximum timestamp (exclusive)
  # @return [HBase::Scoped] HBase::Scoped object with the specified time range
  def time_range min, max
    spawn :@trange, [min, max].map { |e| time_to_long e }
  end

  # Returns an HBase::Scoped object with the specified timestamp
  # @param [Fixnum|Time] ts Timestamp
  # @return [HBase::Scoped] HBase::Scoped object with the specified timestamp
  def at ts
    spawn :@trange, time_to_long(ts)
  end

  # Returns an HBase::Scoped object with the specified projection
  # @param [Array<String>] columns Array of column expressions
  # @return [HBase::Scoped] HBase::Scoped object with the specified projection
  def project *columns
    if columns.first.is_a?(Hash)
      hash = columns.first
      unless (hash.keys - [:prefix, :range, :limit, :offset]).empty?
        raise ArgumentError, "Invalid projection"
      end

      if l = hash[:limit]
        unless l.is_a?(Fixnum) && l >= 0
          raise ArgumentError, ":limit must be a non-negative integer"
        end
      end

      if o = hash[:offset]
        unless o.is_a?(Fixnum) && o >= 0
          raise ArgumentError, ":offset must be a non-negative integer"
        end
      end
    end
    spawn :@project, @project + columns.map { |c|
      cf, cq, type = @table.lookup_schema(c)
      cf ? [cf, cq] : c
    }
  end

  # Returns an HBase::Scoped object with the specified version number limit.
  # If not set or set to :all, all versions are fetched.
  # @param [Fixnum|:all] vs Sets the maximum number of versions
  # @return [HBase::Scoped] HBase::Scoped object with the version number limit
  def versions vs
    unless vs.is_a?(Fixnum) && vs > 0 || vs == :all
      raise ArgumentError, "Invalid versions. Must be a positive integer or :all."
    end
    spawn :@versions, vs
  end

  # Returns an HBase::Scoped object with the specified batch limit
  # @param [Fixnum] b Sets the maximum number of values to fetch each time
  # @return [HBase::Scoped] HBase::Scoped object with the specified batch limit
  def batch b
    raise ArgumentError, "Invalid batch size. Must be a positive integer." unless b.is_a?(Fixnum) && b > 0
    spawn :@batch, b
  end

  # Returns an HBase::Scoped object with the Scan-customization block added.
  # The given block will be evaluated just before an actual scan operation.
  # With method-chaining, multiple blocks can be registered to be evaluated sequentially.
  # @return [HBase::Scoped]
  # @yield [org.apache.hadoop.hbase.client.Scan]
  def with_java_scan &block
    raise ArgumentError, "Block not given" if block.nil?
    raise ArgumentError, "Invalid arity: should be 1" unless block.arity == 1
    spawn :@scan_cbs, @scan_cbs + [block]
  end

  # Returns an HBase::Scoped object with the Get-customization block added
  # The given block will be evaluated just before an actual get operation.
  # With method-chaining, multiple blocks can be registered to be evaluated sequentially.
  # @return [HBase::Scoped]
  # @yield [org.apache.hadoop.hbase.client.Get]
  def with_java_get &block
    raise ArgumentError, "Block not given" if block.nil?
    raise ArgumentError, "Invalid arity: should be 1" unless block.arity == 1
    spawn :@get_cbs, @get_cbs + [block]
  end

private
  # @param [HBase::Table] table
  def initialize table, default_caching
    @table    = table
    @filters  = []
    @project  = []
    @prefixes = []
    @range    = nil
    @versions = nil
    @batch    = nil
    @dcaching = default_caching
    @caching  = nil
    @limit    = nil
    @trange   = nil
    @scan_cbs = []
    @get_cbs  = []
  end

  def spawn *args
    self.dup.tap do |obj|
      args.each_slice(2) do |slice|
        attr, val = slice
        obj.instance_variable_set attr, val
      end
    end
  end

  def htable
    @table.htable
  end

  def process_projection! obj
    limit   = offset = nil
    ranges  = prefixes = []
    filters = []

    @project.each do |col|
      case col
      when Hash
        col.each do |prop, val|
          case prop
          when :prefix
            prefixes += arrayfy(val)
          when :range
            ranges += arrayfy(val)
          when :limit
            limit = val
          when :offset
            offset = val
          else
            # Shouldn't happen
            raise ArgumentError, "Invalid projection: #{prop}"
          end
        end
      else
        cf, cq = Util.parse_column_name col
        if cq
          obj.addColumn cf, cq
        else
          obj.addFamily cf
        end
      end
    end

    if (limit && !offset) || (!limit && offset)
      raise ArgumentError, "Both `limit` and `offset` must be specified"
    end

    # Column prefix filter
    unless prefixes.empty?
      # disjunctive
      filters <<
        MultipleColumnPrefixFilter.new(
          prefixes.map { |pref| Util.to_bytes(pref).to_a }.to_java(Java::byte[]))
    end

    # Column range filter
    unless ranges.empty?
      # disjunctive
      filters <<
        FilterList.new(FilterList::Operator::MUST_PASS_ONE,
          ranges.map { |range|
            raise ArgumentError, "Invalid range type" unless range.is_a? Range

            ColumnRangeFilter.new(
              Util.to_bytes(range.begin), true,
              Util.to_bytes(range.end), !range.exclude_end?) })
    end

    # Column pagniation filter (last)
    if limit && offset
      filters << ColumnPaginationFilter.new(limit, offset)
    end

    filters
  end

  def getify rowkey, cols = []
    Get.new(Util.to_bytes rowkey).tap { |get|
      set_max_versions get

      filters = []
      filters += process_projection!(get)

      range = @range || range_for_prefix
      case range
      when Range
        filters <<
          RowFilter.new(
            CompareFilter::CompareOp::GREATER_OR_EQUAL,
            BinaryComparator.new(Util.to_bytes range.begin))

        filters <<
          RowFilter.new(
            (range.exclude_end? ?
              CompareFilter::CompareOp::LESS :
              CompareFilter::CompareOp::LESS_OR_EQUAL),
            BinaryComparator.new(Util.to_bytes range.end))
      when Array
        filters <<
          RowFilter.new(
            CompareFilter::CompareOp::GREATER_OR_EQUAL,
            BinaryComparator.new(range[0])) if range[0]

        filters <<
          RowFilter.new(
            CompareFilter::CompareOp::LESS,
            BinaryComparator.new(range[1])) if range[1]
      else
        raise ArgumentError, "Invalid range"
      end if range

      # Prefix filters
      filters += [*build_prefix_filter].compact

      # RowFilter must precede the others
      filters += @filters

      get.setFilter FilterList.new(filters) unless filters.empty?

      # Timerange / Timestamp
      case @trange
      when Array
        get.setTimeRange(*@trange)
      when Time, Fixnum
        get.setTimeStamp @trange
      end

      # Customization
      @get_cbs.each do |prc|
        prc.call get
      end

      # add specific columns, if any
      cols.each do |col|
        cf, cq, _ = @table.lookup_and_parse col, false
        if cq
          get.add_column cf, cq
        else
          get.add_family cf
        end
      end
    }
  end

  def filter_for cf, cq, type, val
    case val
    when Range
      min, max = [val.begin, val.end].map { |k| Util.to_typed_bytes type, k }
      FilterList.new(FilterList::Operator::MUST_PASS_ALL, [
        SingleColumnValueFilter.new(
          cf, cq,
          CompareFilter::CompareOp::GREATER_OR_EQUAL, min
        ).tap { |f| f.setFilterIfMissing(true) },
        SingleColumnValueFilter.new(
          cf, cq,
          (val.exclude_end? ? CompareFilter::CompareOp::LESS :
                              CompareFilter::CompareOp::LESS_OR_EQUAL), max
        ).tap { |f| f.setFilterIfMissing(true) }
      ])
    when Hash
      FilterList.new(FilterList::Operator::MUST_PASS_ALL,
        val.map { |op, v|
          operator =
            case op
            when :gt, :>
              CompareFilter::CompareOp::GREATER
            when :gte, :>=
              CompareFilter::CompareOp::GREATER_OR_EQUAL
            when :lt, :<
              CompareFilter::CompareOp::LESS
            when :lte, :<=
              CompareFilter::CompareOp::LESS_OR_EQUAL
            when :eq, :==
              CompareFilter::CompareOp::EQUAL
            when :ne # , :!= # Ruby 1.8 compatibility
              CompareFilter::CompareOp::NOT_EQUAL
            else
              if val.length == 1
                return filter_for(cf, cq, nil, Util.to_typed_bytes(type, val))
              else
                raise ArgumentError, "Unknown operator: #{op}"
              end
            end
          case v
          when Array
            # XXX TODO Undocumented feature
            FilterList.new(
              case op
              when :ne # , :!=
                FilterList::Operator::MUST_PASS_ALL
              else
                FilterList::Operator::MUST_PASS_ONE
              end,
              v.map { |vv|
                SingleColumnValueFilter.new(cf, cq, operator, Util.to_typed_bytes(type, vv)).tap { |f|
                  f.setFilterIfMissing( op != :ne )
                }
              }
            )
          else
            SingleColumnValueFilter.new(cf, cq, operator, Util.to_typed_bytes(type, v)).tap { |f|
              f.setFilterIfMissing( op != :ne )
            }
          end
        }
      )
    when Regexp
      SingleColumnValueFilter.new(
        cf, cq,
        CompareFilter::CompareOp::EQUAL,
        RegexStringComparator.new(val.to_s)
      ).tap { |f| f.setFilterIfMissing(true) }
    when nil
      # - has value < '' -> not ok
      # - no value       -> ok
      SingleColumnValueFilter.new(
        cf, cq,
        CompareFilter::CompareOp::LESS,
        HBase::Util::JAVA_BYTE_ARRAY_EMPTY
      )
    else
      SingleColumnValueFilter.new(
        cf, cq,
        CompareFilter::CompareOp::EQUAL,
        Util.to_typed_bytes(type, val)
      ).tap { |f| f.setFilterIfMissing(true) }
    end
  end

  def set_max_versions obj
    case @versions
    when Fixnum
      obj.setMaxVersions @versions
    when :all
      obj.setMaxVersions
    end
  end

  def filtered_scan
    Scan.new.tap { |scan|
      # Range
      range = @range || range_for_prefix
      case range
      when Range
        scan.setStartRow Util.to_bytes range.begin

        if range.exclude_end?
          scan.setStopRow Util.to_bytes range.end
        else
          scan.setStopRow Util.append_0(Util.to_bytes range.end)
        end
      when Array
        scan.setStartRow range[0] if range[0]
        scan.setStopRow  range[1] if range[1]
      else
        # This shouldn't happen though.
        raise ArgumentError, "Invalid range"
      end if range

      # Caching
      scan.caching = @caching if @caching

      # Filters (with projection)
      prefix_filter = [*build_prefix_filter].compact
      filters = prefix_filter + @filters
      filters += process_projection!(scan)

      scan.setFilter FilterList.new(filters) unless filters.empty?

      # Limit
      if @limit
        if [@caching, @dcaching].compact.all? { |c| @limit < c }
          scan.caching = @limit
        end
      end

      # Versions
      set_max_versions scan

      # Timerange / Timestamp
      case @trange
      when Array
        scan.setTimeRange(*@trange)
      when Time, Fixnum
        scan.setTimeStamp @trange
      end

      # Batch
      scan.setBatch @batch if @batch

      # Customization
      @scan_cbs.each do |prc|
        prc.call scan
      end
    }
  end

  # Scanner for just counting records
  # @private
  def filtered_scan_minimum
    filtered_scan.tap do |scan|
      scan.setMaxVersions 1

      # FirstKeyOnlyFilter: A filter that will only return the first KV from each row-
      # - Not compatible with SingleColumnValueFilter
      # KeyOnlyFilter: A filter that will only return the key component of each KV
      # - Compatible with SingleColumnValueFilter
      ko = KeyOnlyFilter.new
      if flist = scan.getFilter
        if flist.is_a?(FilterList)
          flist.addFilter ko
        else
          flist = FilterList.new([flist, ko])
        end
      else
        flist = FilterList.new([ko, FirstKeyOnlyFilter.new])
      end
      scan.setFilter flist
    end
  end

  def build_prefix_filter
    return nil if @prefixes.empty?

    filters = @prefixes.map { |prefix|
      PrefixFilter.new(Util.to_bytes prefix)
    }

    if filters.length == 1
      filters.first
    else
      FilterList.new FilterList::Operator::MUST_PASS_ONE, filters
    end
  end

  def range_for_prefix
    return nil if @prefixes.empty?

    [@prefixes.map { |pref| ByteArray.new(pref) }.min.java, nil]
  end

  def parse_filter_input filters
    filters.map { |f|
      case f
      when Hash
        f.map { |col, val|
          cf, cq, type = @table.lookup_and_parse col, true

          case val
          when Array
            FilterList.new(FilterList::Operator::MUST_PASS_ONE,
              val.map { |v| filter_for cf, cq, type, v })
          else
            filter_for cf, cq, type, val
          end
        }.flatten
      when FilterBase, FilterList
        f
      else
        raise ArgumentError, "Unknown filter type"
      end
    }.flatten
  end

  def arrayfy val
    # No range splat
    if Util.java_bytes?(val)
      [val]
    elsif val.is_a?(Array)
      val
    else
      [val]
    end
  end

  def check_closed
    raise RuntimeError, "HBase connection is already closed" if @table.closed?
  end

  def iterate scan
    scanner = htable.getScanner(scan)
    if @limit
      scanner.each_with_index do |result, idx|
        yield result
        break if idx == @limit - 1
      end
    else
      scanner.each do |result|
        yield result
      end
    end
  ensure
    scanner.close if scanner
  end
end#Scoped
end#HBase

