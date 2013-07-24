class HBase
class Table
  class CheckedOperation
    def initialize table, mutation, rowkey, cf, cq, val
      @table   = table
      @mutation = mutation
      @rowkey   = rowkey
      @cf       = cf
      @cq       = cq
      @val      = val
    end

    # @param [Hash] props
    def put props
      @table.htable.checkAndPut(
        @rowkey, @cf, @cq, @val, @mutation.put(@rowkey, props))
    end

    # @param [Object] *extra Optional delete specification. Column family, qualifier, and timestamps
    def delete *extra
      @table.htable.checkAndDelete(
        @rowkey, @cf, @cq, @val, @mutation.delete(@rowkey, *extra))
    end
  end
end
end

