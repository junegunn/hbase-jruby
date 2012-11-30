class HBase
class << self
  # Shortcut method to HBase::ByteArray.new
  # @param [Object] value
  def ByteArray value
    ByteArray.new value
  end
end
# Boxed class for Java byte arrays
# @!attribute [r] java
#   @return [byte[]] Java byte array
class ByteArray
  attr_reader :java

  # @param [Object] value
  def initialize value
    @java = Util.to_bytes value
  end

  # Checks if the two byte arrays are the same
  # @param [HBase::ByteArray] other
  def eql? other
    Arrays.equals(@java, other.java)
  end
  alias == eql?

  # Compares two ByteArray objects
  # @param [HBase::ByteArray] other
  def <=> other
    Bytes.compareTo(@java, other.java)
  end

  # Concats two byte arrays
  # @param [HBase::ByteArray] other
  def + other
    ByteArray.new(Bytes.add @java, other.java)
  end

  # Returns the Java byte array
  # @return [byte[]]
  def to_java_bytes
    @java
  end

  # Returns the first byte array whose prefix doesn't match this byte array
  # @return [byte[]]
  def stopkey_bytes_for_prefix
    arr = @java.to_a
    csr = arr.length - 1
    arr[csr] += 1
    while csr >= 0 && arr[csr] > 127
      csr -= 1
      arr[csr] += 1
    end
    if csr < 0
      nil
    else
      arr[0..csr].to_java(Java::byte)
    end
  end

  # Returns a hash number for the byte array
  # @return [Fixnum]
  def hash
    Arrays.java_send(:hashCode, [Util::JAVA_BYTE_ARRAY_CLASS], @java)
  end
end#ByteArray
end#HBase

