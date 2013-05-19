class HBase
class << self
  # Shortcut method to HBase::ByteArray.new
  # @param [*Object] values
  # @return [HBase::ByteArray]
  def ByteArray *values
    ByteArray.new(*values)
  end
end
# Boxed class for Java byte arrays
# @!attribute [r] java
#   @return [byte[]] The underlying native Java byte array
class ByteArray
  attr_reader :java
  alias to_java java
  alias to_java_bytes java

  include Enumerable

  # Shortcut method to HBase::ByteArray.new
  # @param [*Object] values
  # @return [HBase::ByteArray]
  def self.[] *values
    ByteArray.new(*values)
  end

  # Initializes ByteArray instance with the given objects,
  # each converted to its byte array representation
  # @param [*Object] values
  def initialize *values
    HBase.import_java_classes!
    if defined?(Bytes) && defined?(Arrays)
      ByteArray.class_eval do
        alias initialize initialize_
      end
    end
    initialize_(*values)
  end

  def each
    return enum_for(:each) unless block_given?
    @java.to_a.each { |byte| yield byte }
  end

  # Checks if the two byte arrays are the same
  # @param [HBase::ByteArray] other
  def eql? other
    other = other_as_byte_array other
    Arrays.equals(@java, other.java)
  end
  alias == eql?

  # Compares two ByteArray objects
  # @param [HBase::ByteArray] other
  def <=> other
    other = other_as_byte_array other
    Bytes.compareTo(@java, other.java)
  end

  # Concats two byte arrays
  # @param [Object] other
  def + other
    ByteArray.new(@java, other)
  end

  # Appends the byte array of the given object on to the end
  # @param [Object] other
  # @return [ByteArray] Modified self
  def << other
    @java = Bytes.add @java, Util.to_bytes(other)
    self
  end

  # Prepends the byte array of the given object to the front
  # @param [*Object] args Objects to prepend
  # @return [ByteArray] Modified self
  def unshift *args
    @java = (ByteArray.new(*args) + self).java
    self
  end

  # Returns the length of the byte array
  # @return [Fixnum]
  def length
    @java.length
  end

  # Slices the byte array
  # @param [Object] index
  # @return [ByteArray]
  def [] *index
    if index.length == 1 && index.first.is_a?(Fixnum)
      @java.to_a[*index]
    else
      ByteArray.new(@java.to_a[*index].to_java(Java::byte))
    end
  end

  # @param [Symbol] type
  # @return [Object]
  def decode type
    Util.from_bytes type, @java
  end

  # Returns the first element decoded as the given type
  # and removes the portion from the byte array.
  # For types of variable lengths, such as :string and :bigdecimal, byte size must be given.
  # @param [Symbol] type
  # @return [Object]
  def shift type, length = nil
    length =
      case type
      when :fixnum, :long, :float, :double
        8
      when :int
        4
      when :short
        2
      when :boolean, :bool, :byte
        1
      else
        length
      end
    raise ArgumentError.new("Byte length must be specified for type: #{type}") unless length
    raise ArgumentError.new("Not enough bytes for #{type}") if length > @java.length

    arr   = @java.to_a
    val   = arr[0, length].to_java(Java::byte)
    @java = arr[length..-1].to_java(Java::byte)

    Util.from_bytes type, val
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

private
  def initialize_ *values
    @java = values.inject(Util::JAVA_BYTE_ARRAY_EMPTY) { |sum, value|
      Bytes.add sum, Util.to_bytes(value)
    }
  end

  def other_as_byte_array other
    case other
    when ByteArray
      other
    else
      ByteArray[other]
    end
  end
end#ByteArray
end#HBase

