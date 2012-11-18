class HBase
module Util
  class << self
    # Returns byte array representation of the Ruby object
    # @param [byte[]] v
    # @return [byte[]]
    def to_bytes v
      case v
      when Float
        Bytes.java_send :toBytes, [Java::double], v
      when Fixnum
        Bytes.java_send :toBytes, [Java::long], v
      when Bignum
        Bytes.java_send :toBytes, [java.math.BigDecimal], java.math.BigDecimal.new(v.to_s)
      else
        Bytes.to_bytes v
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

    def parse_column_name col
      case col
      when KeyValue
        return col.getFamily, col.getQualifier
      when Array
        return col[0], col[1]
      else
        cf, cq = KeyValue.parseColumn(col.to_s.to_java_bytes)
        return cf, cq
      end
    end
  end
end#Util
end#HBase

