require 'json'

# Represents a row returned by HBase
class HBase
# @author Junegunn Choi <junegunn.c@gmail.com>
class Result
  # @return [String, byte[]]
  def rowkey
    @table.decode_rowkey @result.getRow
  end

  # Returns Hash representation of the row.
  # @param [Hash] schema Schema used to parse byte arrays
  # @return [Hash] Hash representation of the row
  def to_hash schema = {}
    {}.tap do |ret|
      @result.getNoVersionMap.each do |cf, cqmap|
        cqmap.each do |cq, val|
          col = cfcq cf, cq
          ret[col] = schema[col] ? decode(schema[col], val) : val
        end
      end
    end
  end

  # Returns Hash representation of the row.
  # Each column value again is represented as a Hash indexed by timestamp of each version.
  # @param [Hash] schema Schema used to parse byte arrays
  # @return [Hash<Hash>] Hash representation of the row
  def to_hash_with_versions schema = {}
    {}.tap do |ret|
      @result.getMap.each do |cf, cqmap|
        cqmap.each do |cq, tsmap|
          col = cfcq cf, cq
          ret[col] = Hash[
            tsmap.map { |ts, val|
              [ts, schema[col] ? decode(schema[col], val) : val]
            }]
        end
      end
    end
  end

  # Returns the column value as a byte array
  # @param [String, org.apache.hadoop.hbase.KeyValue] col
  # @return [byte[]] Byte array representation
  def bytes col
    cf, cq = Util.parse_column_name(col)
    @result.getValue cf, cq
  end

  # Returns the column value as a String
  # @param [String, org.apache.hadoop.hbase.KeyValue] col
  # @return [String]
  def string col
    decode :string, bytes(col)
  end
  alias str string

  # Returns the column value as a Fixnum
  # @param [String, org.apache.hadoop.hbase.KeyValue] col
  # @return [Fixnum]
  def fixnum col
    decode :fixnum, bytes(col)
  end
  alias integer fixnum
  alias int     fixnum

  # Returns the column value as a Bignum
  # @param [String, org.apache.hadoop.hbase.KeyValue] col
  # @return [Bignum]
  def bignum col
    decode :bignum, bytes(col)
  end
  alias biginteger bignum
  alias bigint     bignum

  # Returns the column value as a Float
  # @param [String, org.apache.hadoop.hbase.KeyValue] col
  # @return [Float]
  def float col
    decode :float, bytes(col)
  end
  alias double float

  # Returns the column value as a boolean value
  # @param [String, org.apache.hadoop.hbase.KeyValue] col
  # @return [true, false]
  def boolean col
    decode :boolean, bytes(col)
  end
  alias bool boolean

  # Returns the column value as a Ruby Object
  # @param [String, org.apache.hadoop.hbase.KeyValue] col
  # @return [Object]
  def json col
    decode :json, bytes(col)
  end

private
  # @param [HBase::Table] table
  # @param [org.apache.hadoop.hbase.client.Result] java_result
  def initialize table, java_result
    @table  = table
    @result = java_result
  end

  def cfcq cf, cq
    # FIXME: Only allows String names
    cf = String.from_java_bytes cf
    cq = String.from_java_bytes cq
    cq = nil if cq.empty?
    [cf, cq].compact.join(':')
  end

  def decode type, val
    case type
    when :string, :str
      Bytes.to_string val
    when :fixnum, :int, :integer
      Bytes.to_long val
    when :bignum, :bigint, :biginteger
      BigDecimal.new(Bytes.to_big_decimal(val).to_s).to_i
    when :float, :double
      Bytes.to_double val
    when :boolean, :bool
      Bytes.to_boolean val
    when :json
      JSON.parse Bytes.to_string(val)
    else
      raise Exception, "Invalid type: #{type}"
    end
  end
end#Result
end#HBase 

