require 'bigdecimal'

class HBase
module Util
  JAVA_BYTE_ARRAY = 'byte[]'

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
      when Bignum
        Bytes.java_send :toBytes, [java.math.BigDecimal], java.math.BigDecimal.new(v.to_s)
      when BigDecimal
        Bytes.java_send :toBytes, [java.math.BigDecimal], v.to_java
      when true, false, java.math.BigDecimal, ByteBuffer
        Bytes.to_bytes v
      when nil
        ''.to_java_bytes
      else
        if v.respond_to?(:java_class) && v.java_class.simple_name == JAVA_BYTE_ARRAY
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
      when KeyValue
        return col.getFamily, col.getQualifier
      when Array
        return to_bytes(col[0]), to_bytes(col[1])
      else
        cf, cq = KeyValue.parseColumn(col.to_s.to_java_bytes)
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

          Table.class_eval do
            import org.apache.hadoop.hbase.HTableDescriptor
            import org.apache.hadoop.hbase.HColumnDescriptor

            import org.apache.hadoop.hbase.client.Put
            import org.apache.hadoop.hbase.client.Get
            import org.apache.hadoop.hbase.client.Delete
            import org.apache.hadoop.hbase.client.Increment

            import org.apache.hadoop.hbase.io.hfile.Compression::Algorithm
            import org.apache.hadoop.hbase.regionserver.StoreFile::BloomType
          end

          Scoped.class_eval do
            import org.apache.hadoop.hbase.client.Scan
            import org.apache.hadoop.hbase.filter.BinaryComparator
            import org.apache.hadoop.hbase.filter.CompareFilter
            import org.apache.hadoop.hbase.filter.FilterBase
            import org.apache.hadoop.hbase.filter.FilterList
            import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
            import org.apache.hadoop.hbase.filter.KeyOnlyFilter
            import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
          end
        end
      end
    end
  end

end#Util
end#HBase

