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
  end
end#Util
end#HBase

