require 'java'
require 'open-uri'
require 'tempfile'
require 'erb'

# HBase connection
class HBase
  class << self
    # @private
    def import_java_classes!
      {
        HBase => %w[
          org.apache.hadoop.hbase.HBaseConfiguration
          org.apache.hadoop.hbase.client.HBaseAdmin
          org.apache.hadoop.hbase.client.HConnectionManager
          org.apache.hadoop.hbase.client.HTablePool
        ],
        HBase::Util => %w[
          java.nio.ByteBuffer
          org.apache.hadoop.hbase.KeyValue
          org.apache.hadoop.hbase.util.Bytes
        ],
        HBase::ByteArray => %w[
          java.util.Arrays
          org.apache.hadoop.hbase.util.Bytes
        ],
        HBase::Cell => %w[
          org.apache.hadoop.hbase.KeyValue
        ],
        HBase::Result => %w[
          org.apache.hadoop.hbase.util.Bytes
        ],
        HBase::Table => %w[
          org.apache.hadoop.hbase.HColumnDescriptor
          org.apache.hadoop.hbase.HTableDescriptor
          org.apache.hadoop.hbase.client.Append
          org.apache.hadoop.hbase.client.Delete
          org.apache.hadoop.hbase.client.Increment
          org.apache.hadoop.hbase.client.Put
          org.apache.hadoop.hbase.client.RowMutations
          org.apache.hadoop.hbase.io.hfile.Compression
          org.apache.hadoop.hbase.io.compress.Compression
        ], # hfile.Compression <= 0.94
        HBase::Scoped => %w[
          org.apache.hadoop.hbase.client.Get
          org.apache.hadoop.hbase.client.Scan
          org.apache.hadoop.hbase.filter.BinaryComparator
          org.apache.hadoop.hbase.filter.ColumnPaginationFilter
          org.apache.hadoop.hbase.filter.ColumnRangeFilter
          org.apache.hadoop.hbase.filter.CompareFilter
          org.apache.hadoop.hbase.filter.FilterBase
          org.apache.hadoop.hbase.filter.FilterList
          org.apache.hadoop.hbase.filter.KeyOnlyFilter
          org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
          org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter
          org.apache.hadoop.hbase.filter.PrefixFilter
          org.apache.hadoop.hbase.filter.RegexStringComparator
          org.apache.hadoop.hbase.filter.RowFilter
          org.apache.hadoop.hbase.filter.SingleColumnValueFilter
          org.apache.hadoop.hbase.filter.WhileMatchFilter
          org.apache.hadoop.hbase.client.coprocessor.AggregationClient
          org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter
        ]
      }.each do |base, list|
        base.class_eval do
          list.each do |klass|
            begin
              java_import klass
            rescue NameError
            end
          end
        end
      end
    end
  end#class << self
end
