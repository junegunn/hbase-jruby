require 'java'
require 'open-uri'
require 'tempfile'
require 'erb'

# HBase connection
class HBase
  # @private
  @mutex = Mutex.new

  # @private
  @deps = {
    self => %w[
      org.apache.hadoop.hbase.client.ConnectionFactory
      org.apache.hadoop.hbase.HBaseConfiguration
      org.apache.hadoop.hbase.client.HBaseAdmin
      org.apache.hadoop.hbase.client.HTablePool
      org.apache.hadoop.hbase.TableName
    ],
    Util => %w[
      java.nio.ByteBuffer
      org.apache.hadoop.hbase.KeyValue
      org.apache.hadoop.hbase.util.Bytes
    ],
    ByteArray => %w[
      java.util.Arrays
      org.apache.hadoop.hbase.util.Bytes
    ],
    Cell => %w[
      org.apache.hadoop.hbase.KeyValue
      org.apache.hadoop.hbase.CellUtil
    ],
    Result => %w[
      org.apache.hadoop.hbase.util.Bytes
    ],
    Table => %w[
      org.apache.hadoop.hbase.HColumnDescriptor
      org.apache.hadoop.hbase.HTableDescriptor
      org.apache.hadoop.hbase.client.Append
      org.apache.hadoop.hbase.client.Delete
      org.apache.hadoop.hbase.client.Increment
      org.apache.hadoop.hbase.client.Put
      org.apache.hadoop.hbase.client.RowMutations
    ] << %w[org.apache.hadoop.hbase.io.hfile.Compression
            org.apache.hadoop.hbase.io.compress.Compression],
    Scoped => %w[
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
  }

  class << self
    # Returns the version of the loaded client library
    # @return [String]
    def version
      org.apache.hadoop.hbase.util.VersionInfo.getVersion
    end

    # @private
    def import_java_classes!
      @mutex.synchronize do
        @deps.each do |base, list|
          base.class_eval do
            list.reject! do |classes|
              [*classes].find do |klass|
                begin
                  java_import klass
                  true
                rescue NameError
                  false
                end
              end
            end
          end
        end
        @deps.reject! { |k, v| v.empty? }

        self.instance_eval do
          def import_java_classes!
          end
        end if @deps.empty?
      end
      nil
    end
  end#class << self
end
