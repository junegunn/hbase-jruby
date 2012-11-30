require 'bigdecimal'

class HBase
module Util
  JAVA_BYTE_ARRAY_EMPTY = [].to_java(Java::byte)
  JAVA_BYTE_ARRAY_CLASS = JAVA_BYTE_ARRAY_EMPTY.java_class

  class << self
    def java_bytes? v
      v.respond_to?(:java_class) && v.java_class == JAVA_BYTE_ARRAY_CLASS
    end

    # Returns byte array representation of the Ruby object
    # @param [byte[]] v
    # @return [byte[]]
    def to_bytes v
      case v
      when String, ByteArray
        v.to_java_bytes
      when Fixnum
        Bytes.java_send :toBytes, [Java::long], v
      when Symbol
        v.to_s.to_java_bytes
      when Float
        Bytes.java_send :toBytes, [Java::double], v
      when true, false, ByteBuffer
        Bytes.to_bytes v
      when nil
        ''.to_java_bytes
      when Bignum
        raise ArgumentError, "Integer too large. Consider storing it as a BigDecimal."
      when BigDecimal
        Bytes.java_send :toBytes, [java.math.BigDecimal], v.to_java
      when java.math.BigDecimal
        Bytes.java_send :toBytes, [java.math.BigDecimal], v
      else
        if java_bytes?(v)
          v
        else
          raise ArgumentError.new("Don't know how to convert #{v.class} into Java bytes")
        end
      end
    end

    # Returns Ruby object decoded from the byte array according to the given type
    # @param [Symbol, Class] type Type to convert to
    # @param [byte[]] val Java byte array
    # @return [Object]
    def from_bytes type, val
      return nil if val.nil?

      case type
      when :string, :str
        Bytes.to_string val
      when :fixnum, :int, :integer
        Bytes.to_long val
      when :symbol, :sym
        Bytes.to_string(val).to_sym
      when :bigdecimal
        BigDecimal.new(Bytes.to_big_decimal(val).to_s)
      when :float, :double
        Bytes.to_double val
      when :boolean, :bool
        Bytes.to_boolean val
      when :raw
        val
      else
        raise ArgumentError, "Invalid type: #{type}"
      end
    end

    # Returns a byte array with a trailing '0' byte
    # @param [byte[]] v
    # @return [byte[]]
    def append_0 v
      baos = java.io.ByteArrayOutputStream.new
      baos.write v, 0, v.length
      baos.write 0
      baos.toByteArray
    end

    # Extracts a byte array pair of column family and column qualifier from the given object
    # @param [Object, Array, KeyValue] col
    def parse_column_name col
      case col
      when ColumnKey
        return col.cf.to_java_bytes, col.cq(:raw)
      when KeyValue
        return col.getFamily, col.getQualifier
      when Array
        return to_bytes(col[0]), to_bytes(col[1])
      when '', nil
        raise ArgumentError, "Column family not specified"
      else
        cf, cq = KeyValue.parseColumn(col.to_s.to_java_bytes)
        cq = JAVA_BYTE_ARRAY_EMPTY if cq.nil? && col.to_s[-1, 1] == ':'
        return cf, cq
      end
    end

    # Import Java classes (Prerequisite for classes in hbase-jruby)
    # @return [nil]
    def import_java_classes!
      imp = lambda { |base, classes|
        base.class_eval do
          classes.each do |klass|
            begin
              import klass
            rescue NameError => e
              warn e
            end
          end
        end
      }

      @@imported ||= begin
        imp.call HBase, %w[
          org.apache.hadoop.hbase.HBaseConfiguration
          org.apache.hadoop.hbase.client.HBaseAdmin
          org.apache.hadoop.hbase.client.HTablePool
          org.apache.hadoop.hbase.client.HConnectionManager
        ]

        imp.call HBase::Util, %w[
          java.nio.ByteBuffer
          org.apache.hadoop.hbase.util.Bytes
          org.apache.hadoop.hbase.KeyValue
        ]

        imp.call HBase::ByteArray, %w[
          java.util.Arrays
          org.apache.hadoop.hbase.util.Bytes
        ]

        imp.call HBase::Cell, %w[
          org.apache.hadoop.hbase.KeyValue
        ]

        imp.call HBase::Result, %w[
          org.apache.hadoop.hbase.util.Bytes
        ]

        imp.call HBase::ColumnKey, %w[
          java.util.Arrays
          org.apache.hadoop.hbase.util.Bytes
        ]

        imp.call HBase::Table, %w[
          org.apache.hadoop.hbase.HColumnDescriptor
          org.apache.hadoop.hbase.HTableDescriptor
          org.apache.hadoop.hbase.client.Delete
          org.apache.hadoop.hbase.client.Increment
          org.apache.hadoop.hbase.client.Put
          org.apache.hadoop.hbase.io.hfile.Compression
          org.apache.hadoop.hbase.regionserver.StoreFile
          org.apache.hadoop.hbase.Coprocessor
        ]

        imp.call HBase::Scoped, %w[
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
      end

      nil
    end
  end
end#Util
end#HBase

