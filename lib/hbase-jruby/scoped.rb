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

  # @param [Fixnum] rows The number of rows to cache TODO
  def caching rows
    spawn :@caching, rows
  end

  # @overload range(start_key)
  #   Returns an HBase::Scoped object with the specified scan range
  #   @param [Object] start_key Start rowkey
  #   @return [HBase::Scoped] HBase::Scoped object with the range
  # @overload range(start_key, stop_key)
  #   Returns an HBase::Scoped object with the specified scan range
  #   @param [Object, nil] start_key Start rowkey. Can be nil.
  #   @param [Object] stop_key Stop rowkey (exclusive)
  #   @return [HBase::Scoped] HBase::Scoped object with the range
  # @overload range(start_stop_range)
  #   Returns an HBase::Scoped object with the specified scan range
  #   @param [Range] start_stop_range Rowkey scan range
  #   @return [HBase::Scoped] HBase::Scoped object with the range
  def range *key_range
    raise ArgumentError, "Invalid range" unless [1, 2].include?(key_range.length)
    spawn :@range, key_range.length == 1 ? key_range[0] : key_range
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
          when nil
            SingleColumnValueFilter.new(
              cf, cq,
              CompareFilter::CompareOp::EQUAL,
              BinaryComparator.new(nil))
          when Range
            min, max = [val.begin, val.end].map { |k| Util.to_bytes k }
            [
              SingleColumnValueFilter.new(
                cf, cq,
                CompareFilter::CompareOp::GREATER_OR_EQUAL, min),
              SingleColumnValueFilter.new(
                cf, cq,
                (val.exclude_end? ? CompareFilter::CompareOp::LESS :
                                    CompareFilter::CompareOp::LESS_OR_EQUAL), max)
            ]
          else
            SingleColumnValueFilter.new(
              cf, cq,
              CompareFilter::CompareOp::EQUAL,
              Util.to_bytes(val))
          end
        }.flatten
      when FilterBase
        f
      when FilterList
        f.getFilters.to_a
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

private
  # @param [HBase::Table] table
  def initialize table
    @table    = table
    @filters  = []
    @project  = []
    @range    = []
    @versions = nil
    @caching  = nil
    @limit    = nil
  end

  def spawn attr, val
    self.dup.tap do |obj|
      obj.instance_variable_set attr, val
    end
  end

  def htable
    @table.htable
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
      scan.setFilter FilterList.new(@filters)

      @project.each do |col|
        cf, cq = Util.parse_column_name col
        if cq
          scan.addColumn cf, cq
        else
          scan.addFamily cf
        end
      end

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
end#Scoped
end#HBase

