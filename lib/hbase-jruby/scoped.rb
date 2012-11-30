class HBase
# Scope of data access
# @author Junegunn Choi <junegunn.c@gmail.com>
# @!attribute [r] table
#   @return [HBase::Table] HBase::Table instance for this scope
class Scoped
  include Enumerable
  include Scoped::Aggregation

  attr_reader :table

  # A clean HBase::Scoped object for the same table
  # @return [HBase::Scope] A clean HBase::Scoped object for the same table
  def unscope
    Scoped.send(:new, @table)
  end

  # Number of rows in the scope
  # @return [Fixnum, Bignum] The number of rows in the scope
  def count
    cnt = 0
    begin
      if block_given?
        scanner = htable.getScanner(filtered_scan)
        scanner.each do |result|
          cnt += 1 if yield(Result.send(:new, result))
        end
      else
        scanner = htable.getScanner(filtered_scan_minimum)
        scanner.each { cnt += 1 }
      end
    ensure
      scanner.close if scanner
    end
    cnt
  end

  # @overload get(rowkey)
  #   Single GET.
  #   Gets a record with the given rowkey. If the record is not found, nil is returned.
  #   @param [Object] rowkey Rowkey
  #   @return [HBase::Result, nil]
  # @overload get(rowkeys)
  #   Batch GET. Gets an array of records with the given rowkeys.
  #   Nonexistent records will be returned as nils.
  #   @param [Array<Object>] *rowkeys Rowkeys
  #   @return [Array<HBase::Result>]
  def get rowkeys
    case rowkeys
    when Array
      htable.get(rowkeys.map { |rk| getify rk }).map { |result|
        result.isEmpty ? nil : Result.new(result)
      }
    else
      result = htable.get(getify rowkeys)
      result.isEmpty ? nil : Result.new(result)
    end
  end

  # Iterate through the scope.
  # @yield [HBase::Result] Yields each row in the scope
  def each
    if block_given?
      begin
        scanner = htable.getScanner(filtered_scan)
        scanner.each do |result|
          yield Result.send(:new, result)
        end
      ensure
        scanner.close if scanner
      end
    else
      self
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
    if key_range.last.is_a?(Hash)
      prefixes  = [*key_range.last[:prefix]].compact
      raise ArgumentError,
        "Invalid range. Unknown option(s) specified." unless (key_range.last.keys - [:prefix]).empty?
      key_range = key_range[0...-1]
    end

    if prefixes
      raise ArgumentError, "Invalid range" unless [0, 1, 2].include?(key_range.length)
    else
      raise ArgumentError, "Invalid range" unless [1, 2].include?(key_range.length)
    end

    spawn :@range,
          key_range[0].is_a?(Range) ?
              key_range[0] :
              (key_range.empty? ? nil : key_range),
          :@prefixes,
          prefixes || []
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
  # @param [Fixnum] rows Sets the maximum number of rows to return from scan
  # @return [HBase::Scoped] HBase::Scoped object with the specified row number limit
  def limit rows
    raise ArgumentError, "Invalid limit. Must be a non-negative integer." unless rows.is_a?(Fixnum) && rows >= 0
    spawn :@limit, rows
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
    spawn :@project, @project + columns
  end

  # Returns an HBase::Scoped object with the specified version number limit.
  # If not set, all versions of each value are fetched by default.
  # @param [Fixnum] vs Sets the maximum number of versions
  # @return [HBase::Scoped] HBase::Scoped object with the version number limit
  def versions vs
    raise ArgumentError, "Invalid versions. Must be a positive integer." unless vs.is_a?(Fixnum) && vs > 0
    spawn :@versions, vs
  end

  # Returns an HBase::Scoped object with the specified batch limit
  # @param [Fixnum] b Sets the maximum number of values to fetch each time
  # @return [HBase::Scoped] HBase::Scoped object with the specified batch limit
  def batch b
    raise ArgumentError, "Invalid batch size. Must be a positive integer." unless b.is_a?(Fixnum) && b > 0
    spawn :@batch, b
  end

private
  # @param [HBase::Table] table
  def initialize table
    @table    = table
    @filters  = []
    @project  = []
    @prefixes = []
    @range    = nil
    @versions = nil
    @batch    = nil
    @caching  = nil
    @limit    = nil
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
            prefixes += [*val]
          when :range
            ranges += val.is_a?(Array) ? val : [val]
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

  def getify rowkey
    Get.new(Util.to_bytes rowkey).tap { |get|
      if @versions
        get.setMaxVersions @versions
      else
        get.setMaxVersions
      end

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
            BinaryComparator.new(Util.to_bytes range[0])) if range[0]

        filters <<
          RowFilter.new(
            CompareFilter::CompareOp::LESS,
            BinaryComparator.new(Util.to_bytes range[1])) if range[1]
      else
        raise ArgumentError, "Invalid range"
      end if range

      # Prefix filters
      filters += [*build_prefix_filter].compact

      # RowFilter must precede the others
      filters += @filters

      get.setFilter FilterList.new(filters) unless filters.empty?
    }
  end

  def filter_for cf, cq, val
    case val
    when Range
      min, max = [val.begin, val.end].map { |k| Util.to_bytes k }
      FilterList.new(FilterList::Operator::MUST_PASS_ALL, [
        SingleColumnValueFilter.new(
          cf, cq,
          CompareFilter::CompareOp::GREATER_OR_EQUAL, min),
        SingleColumnValueFilter.new(
          cf, cq,
          (val.exclude_end? ? CompareFilter::CompareOp::LESS :
                              CompareFilter::CompareOp::LESS_OR_EQUAL), max)
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
              raise ArgumentError, "Unknown operator: #{op}"
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
                SingleColumnValueFilter.new(cf, cq, operator, Util.to_bytes(vv))
              }
            )
          when Hash
            raise ArgumentError, "Hash predicate not supported"
          else
            SingleColumnValueFilter.new(cf, cq, operator, Util.to_bytes(v))
          end
        }
      )
    when Regexp
      SingleColumnValueFilter.new(
        cf, cq,
        CompareFilter::CompareOp::EQUAL,
        RegexStringComparator.new(val.to_s)
      )
    else
      SingleColumnValueFilter.new(
        cf, cq,
        CompareFilter::CompareOp::EQUAL,
        Util.to_bytes(val))
    end
  end

  def filtered_scan
    Scan.new.tap { |scan|
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
        scan.setStartRow Util.to_bytes range[0] if range[0]
        scan.setStopRow  Util.to_bytes range[1] if range[1]
      else
        # This shouldn't happen though.
        raise ArgumentError, "Invalid range"
      end if range

      scan.caching = @caching if @caching

      # Filters
      prefix_filter = [*build_prefix_filter].compact
      filters = prefix_filter + @filters
      filters += process_projection!(scan)

      scan.setFilter FilterList.new(filters) unless filters.empty?

      if @limit
        # setMaxResultSize not implemented in 0.92
        if scan.respond_to?(:setMaxResultSize)
          scan.setMaxResultSize(@limit)
        else
          raise NotImplementedError, 'Scan.setMaxResultSize not implemented'
        end
      end

      if @versions
        scan.setMaxVersions @versions
      else
        scan.setMaxVersions
      end

      # Batch
      scan.setBatch @batch if @batch
    }
  end

  def filtered_scan_minimum
    filtered_scan.tap do |scan|
      scan.cache_blocks = false

      if flist = scan.getFilter
        flist.addFilter KeyOnlyFilter.new
      else
        scan.setFilter FilterList.new(KeyOnlyFilter.new)
      end
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

    byte_arrays = @prefixes.map { |pref| ByteArray.new(pref) }.sort
    start = byte_arrays.first
    stop  = byte_arrays.last

    [start.java, stop.stopkey_bytes_for_prefix]
  end

  def parse_filter_input filters
    filters.map { |f|
      case f
      when Hash
        f.map { |col, val|
          cf, cq = Util.parse_column_name col

          case val
          when Array
            FilterList.new(FilterList::Operator::MUST_PASS_ONE,
              val.map { |v| filter_for cf, cq, v })
          else
            filter_for cf, cq, val
          end
        }.flatten
      when FilterBase, FilterList
        f
      else
        raise ArgumentError, "Unknown filter type"
      end
    }.flatten
  end
end#Scoped
end#HBase

