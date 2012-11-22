class HBase
# Scope of table scan
# @author Junegunn Choi <junegunn.c@gmail.com>
class Scoped
  include Enumerable

  # A clean HBase::Scoped object for the same table
  # @return [HBase::Scope] A clean HBase::Scoped object for the same table
  def unscope
    Scoped.send(:new, @table)
  end

  # Number of rows in the scope
  # @return [Fixnum, Bignum] The number of rows in the scope
  def count
    cnt = 0
    htable.getScanner(filtered_scan_minimum).each do
      cnt += 1
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
      prefixes  = [*key_range.last[:prefix]]
      key_range = key_range[0...-1]
    end

    if prefixes
      raise ArgumentError, "Invalid range" unless [0, 1, 2].include?(key_range.length)
    else
      raise ArgumentError, "Invalid range" unless [1, 2].include?(key_range.length)
    end

    spawn :@range, key_range[0].is_a?(Range) ? key_range[0] : key_range, :@prefixes, prefixes || []
  end

  # Returns an HBase::Scoped object with the filters added
  # @param [Array<Hash, FilterBase, FilterList>] filters
  # @return [HBase::Scoped] HBase::Scoped object also with the specified filters
  def filter *filters
    spawn :@filters, @filters + filters.map { |f|
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

  # Returns an HBase::Scoped object with the specified row number limit
  # @param [Fixnum] rows Sets the maximum number of rows to return from scan
  # @return [HBase::Scoped] HBase::Scoped object with the specified row number limit
  def limit rows
    spawn :@limit, rows
  end

  # Returns an HBase::Scoped object with the specified projection
  # @param [Array<String>] columns Array of column expressions
  # @return [HBase::Scoped] HBase::Scoped object with the specified projection
  def project *columns
    spawn :@project, @project + columns
  end

  # Returns an HBase::Scoped object with the specified version number limit.
  # If not set, all versions of each value are fetched by default.
  # @param [Fixnum] vs Sets the maximum number of versions
  # @return [HBase::Scoped] HBase::Scoped object with the version number limit
  def versions vs
    spawn :@versions, vs
  end

  # Returns an HBase::Scoped object with the specified batch limit
  # @param [Fixnum] b Sets the maximum number of values to fetch each time
  # @return [HBase::Scoped] HBase::Scoped object with the specified batch limit
  def batch b
    spawn :@batch, b
  end

private
  # @param [HBase::Table] table
  def initialize table
    @table    = table
    @filters  = []
    @project  = []
    @range    = []
    @prefixes = []
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
            raise ArgumentError, "Multiple :limit's" if limit
            raise ArgumentError, ":limit must be an integer" unless val.is_a?(Fixnum)
            limit = val
          when :offset
            raise ArgumentError, "Multiple :offset's" if offset
            raise ArgumentError, ":offset must be an integer" unless val.is_a?(Fixnum)
            offset = val
          else
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

      if @range
        case @range
        when Range
          filters <<
            RowFilter.new(
              CompareFilter::CompareOp::GREATER_OR_EQUAL,
              BinaryComparator.new(Util.to_bytes @range.begin))

          filters <<
            RowFilter.new(
              (@range.exclude_end? ?
                CompareFilter::CompareOp::LESS :
                CompareFilter::CompareOp::LESS_OR_EQUAL),
              BinaryComparator.new(Util.to_bytes @range.end))
        when Array
          filters <<
            RowFilter.new(
              CompareFilter::CompareOp::GREATER_OR_EQUAL,
              BinaryComparator.new(Util.to_bytes @range[0])) if @range[0]

          filters <<
            RowFilter.new(
              CompareFilter::CompareOp::LESS,
              BinaryComparator.new(Util.to_bytes @range[1])) if @range[1]
        else
          raise ArgumentError, "Invalid range"
        end
      end

      # Prefix filters
      filters += [*build_prefix_filter]

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
            when :ne, :!=
              CompareFilter::CompareOp::NOT_EQUAL
            else
              raise ArgumentError, "Unknown operator: #{op}"
            end
          case v
          when Array
            # XXX TODO Undocumented feature
            FilterList.new(
              case op
              when :ne, :!=
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
    else
      SingleColumnValueFilter.new(
        cf, cq,
        CompareFilter::CompareOp::EQUAL,
        Util.to_bytes(val))
    end
  end

  def filtered_scan
    Scan.new.tap { |scan|
      case @range
      when Range
        scan.setStartRow Util.to_bytes @range.begin

        if @range.exclude_end?
          scan.setStopRow Util.to_bytes @range.end
        else
          scan.setStopRow Util.append_0(Util.to_bytes @range.end)
        end
      when Array
        scan.setStartRow Util.to_bytes @range[0] if @range[0]
        scan.setStopRow  Util.to_bytes @range[1] if @range[1]
      else
        scan.setStartRow Util.to_bytes @range
      end

      scan.caching = @caching if @caching

      # Filters
      prefix_filter = [*build_prefix_filter]
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

      # A filter that will only return the first KV from each row
      # A filter that will only return the key component of each KV
      filters = [FirstKeyOnlyFilter.new, KeyOnlyFilter.new]
      if flist = scan.getFilter
        filters.each do |filter|
          flist.addFilter filter
        end
      else
        scan.setFilter FilterList.new(filters)
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
end#Scoped
end#HBase

