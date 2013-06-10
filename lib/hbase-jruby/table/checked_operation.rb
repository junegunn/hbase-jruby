class HBase
class Table
  class CheckedOperation
    def initialize table, rowkey, cf, cq, val
      @table  = table
      @rowkey = rowkey
      @cf     = cf
      @cq     = cq
      @val    = val
    end

    # @param [Hash] props
    def put props
      @table.htable.checkAndPut @rowkey, @cf, @cq, @val, @table.send(:make_put, @rowkey, props)
    end

    # @param [Object] *extra Optional delete specification. Column family, qualifier, and timestamps
    def delete *extra
      @table.htable.checkAndDelete @rowkey, @cf, @cq, @val, @table.send(:make_delete, @rowkey, *extra)
    end
  end
end
end

