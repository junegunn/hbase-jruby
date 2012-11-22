require 'bigdecimal'

class HBase
module Util
  JAVA_BYTE_ARRAY_EMPTY = [].to_java(Java::byte) 
  JAVA_BYTE_ARRAY_CLASS = JAVA_BYTE_ARRAY_EMPTY.java_class

  class << self
    # Returns byte array representation of the Ruby object
    # @param [byte[]] v
    # @return [byte[]]
    def to_bytes v
      case v
      when String
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
        Bytes.java_send :toBytes, [java.math.BigDecimal], java.math.BigDecimal.new(v.to_s)
      when BigDecimal
        Bytes.java_send :toBytes, [java.math.BigDecimal], v.to_java
      when java.math.BigDecimal
        Bytes.java_send :toBytes, [java.math.BigDecimal], v
      else
        if v.respond_to?(:java_class) && v.java_class == JAVA_BYTE_ARRAY_CLASS
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
      when :bignum, :bigint, :biginteger
        BigDecimal.new(Bytes.to_big_decimal(val).to_s).to_i
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
        cq = JAVA_BYTE_ARRAY_EMPTY if cq.nil? && col.to_s[-1] == ':'
        return cf, cq
      end
    end

    # @return [nil]
    def import_java_classes!
      @@imported ||= begin
        ::HBase.class_eval do
          import org.apache.hadoop.hbase.HBaseConfiguration
          import org.apache.hadoop.hbase.client.HBaseAdmin
          import org.apache.hadoop.hbase.client.HTablePool
          import org.apache.hadoop.hbase.client.HConnectionManager

          Util.module_eval do
            import java.nio.ByteBuffer
            import org.apache.hadoop.hbase.util.Bytes
            import org.apache.hadoop.hbase.KeyValue
          end

          Cell.class_eval do
            import org.apache.hadoop.hbase.KeyValue
          end

          ColumnKey.class_eval do
            import java.util.Arrays
            import org.apache.hadoop.hbase.util.Bytes
          end

          Table.class_eval do
            import org.apache.hadoop.hbase.HTableDescriptor
            import org.apache.hadoop.hbase.HColumnDescriptor

            import org.apache.hadoop.hbase.client.Put
            import org.apache.hadoop.hbase.client.Delete
            import org.apache.hadoop.hbase.client.Increment

            import org.apache.hadoop.hbase.io.hfile.Compression::Algorithm
            import org.apache.hadoop.hbase.regionserver.StoreFile::BloomType
          end

          Scoped.class_eval do
            import org.apache.hadoop.hbase.client.Get
            import org.apache.hadoop.hbase.client.Scan
            import org.apache.hadoop.hbase.filter.BinaryComparator
            import org.apache.hadoop.hbase.filter.ColumnPaginationFilter
            import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter
            import org.apache.hadoop.hbase.filter.ColumnRangeFilter
            import org.apache.hadoop.hbase.filter.CompareFilter
            import org.apache.hadoop.hbase.filter.FilterBase
            import org.apache.hadoop.hbase.filter.FilterList
            import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
            import org.apache.hadoop.hbase.filter.KeyOnlyFilter
            import org.apache.hadoop.hbase.filter.PrefixFilter
            import org.apache.hadoop.hbase.filter.RowFilter
            import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
          end
        end
      end
    end
  end
end#Util
end#HBase

