require 'bigdecimal'

class HBase
# Represents a row returned by HBase
# @author Junegunn Choi <junegunn.c@gmail.com>
class Row
  include Enumerable

  # Returns the rowkey of the row
  # @param [Symbol] type The type of the rowkey
  #   Can be one of :string, :symbol, :fixnum, :float, :short, :int, :bigdecimal, :boolean and :raw.
  # @return [String, byte[]]
  def rowkey type = :string
    Util.from_bytes type, @result.getRow
  end

  # Enumerates through cells
  def each
    return enum_for(:each) unless block_given?
    @result.raw.each do |kv|
      yield Cell.new(@table, kv)
    end
  end

  def [] *col
    col = col.length == 1 ? col[0] : col
    cf, cq, type = @table.lookup_schema(col)
    if cf
      self.send type, [cf, cq]
    else
      self.raw col
    end
  end

  # Only supports string column qualifiers
  # @return [Hash]
  def to_h
    {}.tap do |ret|
      @result.getNoVersionMap.each do |cf, cqmap|
        cqmap.each do |cq, val|
          f, q, t = @table.lookup_schema(cq.to_s)
          name = t ? q : [cf.to_s.to_sym, ByteArray[cq]]
          ret[name] = Util.from_bytes(t, val)
        end
      end
    end
  end

  # @return [Hash]
  def to_H
    {}.tap do |ret|
      @result.getMap.each do |cf, cqmap|
        cqmap.each do |cq, tsmap|
          f, q, t = @table.lookup_schema(cq.to_s)
          name = t ? q : [cf.to_s.to_sym, ByteArray[cq]]

          ret[name] =
            Hash[
              tsmap.map { |ts, val|
                [ ts, Util.from_bytes(t, val) ]
              }
            ]
        end
      end
    end
  end

  # Returns column values as byte arrays
  # @overload raw(column)
  #   Returns the latest column value as a byte array
  #   @param [String, Array] col Column name as String or 2-element Array of family and qualifier
  #   @return [byte[]] Byte array representation of the latest value
  def raw col
    get_value col
  end

  # Returns all versions of column values as byte arrays in a Hash indexed by their timestamps
  # @overload raws(column)
  #   Returns all versions of column values as byte arrays in a Hash indexed by their timestamps
  #   @param [String, Array] col Column name as String or 2-element Array of family and qualifier
  #   @return [Hash<Fixnum, byte[]>]
  def raws col
    get_value col, true
  end

  # Returns column values as Strings
  # @overload string(column)
  #   Returns the latest column value as a String
  #   @param [String, Array] col Column name as String or 2-element Array of family and qualifier
  #   @return [String]
  def string col
    decode_value :string, col
  end
  alias str string

  # Returns all versions of column values as Strings in a Hash indexed by their timestamps
  # @overload strings(column)
  #   Returns all versions of column values as Strings in a Hash indexed by their timestamps
  #   @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  #   @return [Hash<Fixnum, String>]
  def strings col
    decode_value :string, col, true
  end
  alias strs strings

  # Returns column values as Symbols
  # @overload symbol(column)
  #   Returns the latest column value as a Symbol
  #   @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  #   @return [Symbol]
  def symbol col
    decode_value :symbol, col
  end
  alias sym symbol

  # Returns all versions of column values as Symbols in a Hash indexed by their timestamps
  # @overload symbols(column)
  #   Returns all versions of column values as Symbols in a Hash indexed by their timestamps
  #   @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  #   @return [Hash<Fixnum, Symbol>]
  def symbols col
    decode_value :symbol, col, true
  end
  alias syms symbols

  # Returns 1-byte column values as Fixnums
  # @overload byte(column)
  #   Returns the latest column value as a Fixnum
  #   @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  #   @return [Fixnum]
  def byte col
    decode_value :byte, col
  end

  # Returns all versions of 1-byte column values as Fixnums in a Hash indexed by their timestamps
  # @overload bytes(column)
  #   Returns all versions of column values as Fixnums in a Hash indexed by their timestamps
  #   @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  #   @return [Hash<Fixnum, Fixnum>]
  def bytes col
    decode_value :byte, col, true
  end

  # Returns 2-byte column values as Fixnums
  # @overload short(column)
  #   Returns the latest 2-byte column value as a Fixnum
  #   @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  #   @return [Fixnum]
  def short col
    decode_value :short, col
  end

  # Returns all versions of 2-byte column values as Fixnums in a Hash indexed by their timestamps
  # @overload shorts(column)
  #   Returns all versions of 2-byte column values as Fixnums in a Hash indexed by their timestamps
  #   @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  #   @return [Hash<Fixnum, Fixnum>]
  def shorts col
    decode_value :short, col, true
  end

  # Returns 4-byte column values as Fixnums
  # @overload int(column)
  #   Returns the latest 4-byte column value as a Fixnum
  #   @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  #   @return [Fixnum]
  def int col
    decode_value :int, col
  end

  # Returns all versions of 4-byte column values as Fixnums in a Hash indexed by their timestamps
  # @overload ints(column)
  #   Returns all versions of 4-byte column values as Fixnums in a Hash indexed by their timestamps
  #   @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  #   @return [Hash<Fixnum, Fixnum>]
  def ints col
    decode_value :int, col, true
  end

  # Returns 8-byte column values as Fixnums
  # @overload fixnum(column)
  #   Returns the latest 8-byte column value as a Fixnum
  #   @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  #   @return [Fixnum]
  def fixnum col
    decode_value :fixnum, col
  end
  alias long fixnum

  # Returns all versions of 8-byte column values as Fixnums in a Hash indexed by their timestamps
  # @overload fixnums(column)
  #   Returns all versions of 8-byte column values as Fixnums in a Hash indexed by their timestamps
  #   @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  #   @return [Hash<Fixnum, Fixnum>]
  def fixnums col
    decode_value :fixnum, col, true
  end
  alias longs fixnums

  # Returns column values as Bigdecimals
  # @overload bigdecimal(column)
  #   Returns the latest column value as a BigDecimal
  #   @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  #   @return [BigDecimal]
  def bigdecimal col
    decode_value :bigdecimal, col
  end

  # Returns all versions of column values as BigDecimals in a Hash indexed by their timestamps
  # @overload bigdecimals(column)
  #   Returns all versions of column values as BigDecimals in a Hash indexed by their timestamps
  #   @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  #   @return [Hash<Fixnum, BigDecimal>]
  def bigdecimals col
    decode_value :bigdecimal, col, true
  end

  # Returns column values as Floats
  # @overload float(column)
  #   Returns the latest column value as a Float
  #   @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  #   @return [Float]
  def float col
    decode_value :float, col
  end
  alias double float

  # Returns all versions of column values as Floats in a Hash indexed by their timestamps
  # @overload floats(column)
  #   Returns all versions of column values as Floats in a Hash indexed by their timestamps
  #   @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  #   @return [Hash<Fixnum, Float>]
  def floats col
    decode_value :float, col, true
  end
  alias doubles floats

  # Returns column values as Booleans
  # @overload boolean(column)
  #   Returns the latest column value as a boolean value
  #   @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  #   @return [true, false]
  def boolean col
    decode_value :boolean, col
  end
  alias bool boolean

  # Returns all versions of column values as Booleans in a Hash indexed by their timestamps
  # @overload booleans(column)
  #   Returns all versions of column values as boolean values in a Hash indexed by their timestamps
  #   @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  #   @return [Hash<Fixnum, true|false>]
  def booleans col
    decode_value :boolean, col, true
  end
  alias bools booleans

  # Compares two Result instances on their row keys
  def <=> other
    Bytes.compareTo(rowkey(:raw), other.rowkey(:raw))
  end

private
  def get_value col, with_versions = false
    cf, cq = Util.parse_column_name(col)
    if with_versions
      # Need to make it a Ruby hash:
      #   Prevents implicit conversion from ruby type to java type when updating the Hash
      Hash[ allmap.fetch(cf, {}).fetch(cq, {}) ]
    else
      @result.getValue cf, cq
    end
  end

  def decode_value type, col, with_versions = false
    v = get_value(col, with_versions)
    if with_versions
      v.each do |k, raw|
        v[k] = Util.from_bytes type, raw
      end
      v
    else
      Util.from_bytes type, v
    end
  end

  # @param [HBase::Table] table
  # @param [org.apache.hadoop.hbase.client.Result] java_result
  def initialize table, java_result
    @table  = table
    @result = java_result
    @allmap = nil
  end

  def allmap
    @allmap ||= @result.getMap
  end
end#Row
end#HBase

# For backward compatibility
HBase::Result = HBase::Row

