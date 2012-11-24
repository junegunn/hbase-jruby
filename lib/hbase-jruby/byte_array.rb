class HBase
# @private
class ByteArray
  attr_reader :java

  def initialize value
    @java = Util.to_bytes value
  end

  def eql? other
    Arrays.equals(@java, other.java)
  end
  alias == eql?

  def <=> other
    Bytes.compareTo(@java, other.java)
  end

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

  def hash
    Arrays.java_send(:hashCode, [Util::JAVA_BYTE_ARRAY_CLASS], @java)
  end
end#ByteArray
end#HBase

