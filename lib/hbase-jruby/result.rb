# Represents a row returned by HBase
class HBase
# @author Junegunn Choi <junegunn.c@gmail.com>
class Result
  # Returns the rowkey of the row
  # @param [Symbol] type The data type of the rowkey
  # @return [String, byte[]]
  def rowkey type = :string
    Util.from_bytes type, @result.getRow
  end

  # Returns Hash representation of the row.
  # @param [Hash] schema Schema used to parse byte arrays
  # @return [Hash] Hash representation of the row
  def to_hash schema = {}
    {}.tap do |ret|
      @result.getNoVersionMap.each do |cf, cqmap|
        cqmap.each do |cq, val|
          col = cfcq cf, cq
          ret[col] = schema[col] ? Util.from_bytes(schema[col], val) : val
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
              [ts, schema[col] ? Util.from_bytes(schema[col], val) : val]
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
    Util.from_bytes :string, bytes(col)
  end
  alias str string

  # Returns the column value as a Fixnum
  # @param [String, org.apache.hadoop.hbase.KeyValue] col
  # @return [Fixnum]
  def fixnum col
    Util.from_bytes :fixnum, bytes(col)
  end
  alias integer fixnum
  alias int     fixnum

  # Returns the column value as a Bignum
  # @param [String, org.apache.hadoop.hbase.KeyValue] col
  # @return [Bignum]
  def bignum col
    Util.from_bytes :bignum, bytes(col)
  end
  alias biginteger bignum
  alias bigint     bignum

  # Returns the column value as a Float
  # @param [String, org.apache.hadoop.hbase.KeyValue] col
  # @return [Float]
  def float col
    Util.from_bytes :float, bytes(col)
  end
  alias double float

  # Returns the column value as a boolean value
  # @param [String, org.apache.hadoop.hbase.KeyValue] col
  # @return [true, false]
  def boolean col
    Util.from_bytes :boolean, bytes(col)
  end
  alias bool boolean

private
  # @param [org.apache.hadoop.hbase.client.Result] java_result
  def initialize java_result
    @result = java_result
  end

  def cfcq cf, cq
    # FIXME: Only allows String names
    cf = String.from_java_bytes cf
    cq = String.from_java_bytes cq
    cq = nil if cq.empty?
    [cf, cq].compact.join(':')
  end
end#Result
end#HBase 

