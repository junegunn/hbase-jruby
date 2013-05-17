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
      import_java_classes!

      case v
      when Array
        v.to_java(Java::byte)
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
      when Hash
        len = v.length
        raise ArgumentError, "Unknown value format" unless len == 1

        val = v.values.first
        raise ArgumentError, "Unknown value format" unless val.is_a?(Fixnum)

        case v.keys.first
        when :byte
          [val].to_java(Java::byte)
        when :int
          Bytes.java_send :toBytes, [Java::int], val
        when :short
          Bytes.java_send :toBytes, [Java::short], val
        when :long, :fixnum
          Bytes.java_send :toBytes, [Java::long], val
        else
          raise ArgumentError, "Invalid value format"
        end
      else
        if java_bytes?(v)
          v
        else
          raise ArgumentError.new("Don't know how to convert #{v.class} into Java bytes")
        end
      end
    end

    def to_typed_bytes type, val
      return Util.to_bytes val if type.nil?

      import_java_classes!
      case type
      when :string, :str, :symbol, :sym
        val.to_s.to_java_bytes
      when :byte
        [val].to_java(Java::byte)
      when :boolean, :bool
        Bytes.to_bytes val
      when :int
        Bytes.java_send :toBytes, [Java::int], val
      when :short
        Bytes.java_send :toBytes, [Java::short], val
      when :long, :fixnum
        Bytes.java_send :toBytes, [Java::long], val
      when :float, :double
        Bytes.java_send :toBytes, [Java::double], val
      when :bigdecimal
        case val
        when BigDecimal
          Bytes.java_send :toBytes, [java.math.BigDecimal], v.to_java
        when java.math.BigDecimal
          Bytes.java_send :toBytes, [java.math.BigDecimal], v
        else
          raise ArgumentError, "not BigDecimal"
        end
      when :raw
        val
      else
        raise ArgumentError, "invalid type: #{type}"
      end
    end

    # Returns Ruby object decoded from the byte array according to the given type
    # @param [Symbol, Class] type Type to convert to
    # @param [byte[]] val Java byte array
    # @return [Object]
    def from_bytes type, val
      return nil if val.nil?

      import_java_classes!
      case type
      when :string, :str
        Bytes.to_string val
      when :fixnum, :long
        Bytes.to_long val
      when :byte
        val.first
      when :int
        Bytes.to_int val
      when :short
        Bytes.to_short val
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

  private
    def import_java_classes!
      HBase.import_java_classes!
      if defined?(ByteBuffer) && defined?(KeyValue) && defined?(Bytes)
        self.instance_eval do
          def import_java_classes!
          end
        end
      end
    end
  end

private
  # @private
  def time_to_long ts
    case ts
    when Fixnum
      ts
    when Time
      (ts.to_f * 1000).to_i
    else
      raise ArgumentError, "Invalid time format"
    end
  end
end#Util
end#HBase

