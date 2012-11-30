class HBase
class << self
  # Shortcut method to HBase::ColumnKey.new
  # @param [Object] cf Column family
  # @param [Object] cq Column qualifier
  def ColumnKey cf, cq
    ColumnKey.new cf, cq
  end
end
# Boxed class for column keys
# @!attribute [r] cf
#   @return [String] The column family
class ColumnKey
  attr_reader :cf
  alias family cf

  # Creates a ColumnKey object
  # @param [Object] cf Column family
  # @param [Object] cq Column qualifier
  def initialize cf, cq
    @cf = String.from_java_bytes Util.to_bytes(cf)
    @cq = Util.to_bytes(cq)
  end

  # Returns the column qualifer decoded as the given type
  # @param [Symbol] type
  def cq type = :string
    Util.from_bytes type, @cq
  end
  alias qualifier cq

  # Checks whether if the two ColumnKeys are equal
  # @param [Object] other
  def eql? other
    other = other_as_ck(other)
    @cf == other.cf && Arrays.equals(@cq, other.cq(:raw))
  end
  alias == eql?

  # Compares two ColumnKeys
  # @param [Object] other
  def <=> other
    other = other_as_ck(other)
    d = @cf <=> other.cf
    d != 0 ? d : Bytes.compareTo(@cq, other.cq(:raw))
  end

  # Returns a hash number for this ColumnKey
  # @return [Fixnum]
  def hash
    [@cf, Arrays.java_send(:hashCode, [Util::JAVA_BYTE_ARRAY_CLASS], @cq)].hash
  end

  # Returns String representation of the column key (Qualifier decoded as a String)
  # @return [String]
  def to_s
    [@cf, @cq.empty? ? nil : cq].compact.join(':')
  end

private
  def other_as_ck other
    case other
    when ColumnKey
      other
    else
      cf, cq = Util.parse_column_name(other)
      ColumnKey.new(cf, cq)
    end
  end
end#ColumnKey
end#HBase

