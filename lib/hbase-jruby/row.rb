require 'bigdecimal'

class HBase
# Represents a set of key-values returned by HBase
# @author Junegunn Choi <junegunn.c@gmail.com>
class Row
  include Enumerable

  # Returns if the returned row is empty
  # @return [Boolean]
  def empty?
    @result.empty?
  end

  # Returns the rowkey of the row
  # @param [Symbol] type The type of the rowkey
  #   Can be one of :string, :symbol, :fixnum, :float, :short, :int, :bigdecimal, :boolean and :raw.
  # @return [String, byte[]]
  def rowkey type = :raw
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
    HASH_TEMPLATE.clone.tap do |ret|
      @result.getNoVersionMap.each do |cf, cqmap|
        cf = Util.from_bytes :string, cf
        cqmap.each do |cq, val|
          cqs = Util.from_bytes(:string, cq) rescue nil
          f, q, t = @table.lookup_schema(cqs)
          t = nil if f != cf
          name = t ? q : [cf.to_sym, ByteArray[cq]]

          ret[name] = Util.from_bytes(t, val)
        end
      end
    end
  end
  alias to_hash to_h

  # @return [Hash]
  def to_H
    HASH_TEMPLATE.clone.tap do |ret|
      @result.getMap.each do |cf, cqmap|
        cf = Util.from_bytes :string, cf
        cqmap.each do |cq, tsmap|
          cqs = Util.from_bytes(:string, cq) rescue nil
          f, q, t = @table.lookup_schema(cqs)
          t = nil if f != cf
          name = t ? q : [cf.to_sym, ByteArray[cq]]

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
  alias to_hash_with_versions to_H

  # Returns the latest column value as a Java byte array
  # @param [String, Array] col Column name as String or 2-element Array of family and qualifier
  # @return [byte[]] Byte array representation of the latest value
  def raw col
    get_value col
  end

  # Returns all versions of column values as Java byte arrays in a Hash indexed by their timestamps
  # @param [String, Array] col Column name as String or 2-element Array of family and qualifier
  # @return [Hash<Fixnum, byte[]>]
  def raws col
    get_value col, true
  end

  # Returns the latest column value as a HBase::ByteArray instance
  # @param [String, Array] col Column name as String or 2-element Array of family and qualifier
  # @return [byte[]] Byte array representation of the latest value
  def byte_array col
    decode_value :byte_array, col
  end

  # Returns all versions of column values as HBase::ByteArray instances in a Hash indexed by their timestamps
  # @param [String, Array] col Column name as String or 2-element Array of family and qualifier
  # @return [byte[]] Byte array representation of the latest value
  def byte_arrays col
    decode_value :byte_array, col, true
  end

  # Returns the latest column value as a String
  # @param [String, Array] col Column name as String or 2-element Array of family and qualifier
  # @return [String]
  def string col
    decode_value :string, col
  end
  alias str string

  # Returns all versions of column values as Strings in a Hash indexed by their timestamps
  # @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  # @return [Hash<Fixnum, String>]
  def strings col
    decode_value :string, col, true
  end
  alias strs strings

  # Returns the latest column value as a Symbol
  # @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  # @return [Symbol]
  def symbol col
    decode_value :symbol, col
  end
  alias sym symbol

  # Returns all versions of column values as Symbols in a Hash indexed by their timestamps
  # @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  # @return [Hash<Fixnum, Symbol>]
  def symbols col
    decode_value :symbol, col, true
  end
  alias syms symbols

  # Returns the latest 1-byte column value as a Fixnum
  # @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  # @return [Fixnum]
  def byte col
    decode_value :byte, col
  end

  # Returns all versions of 1-byte column values as Fixnums in a Hash indexed by their timestamps
  # @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  # @return [Hash<Fixnum, Fixnum>]
  def bytes col
    decode_value :byte, col, true
  end

  # Returns the latest 2-byte column value as a Fixnum
  # @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  # @return [Fixnum]
  def short col
    decode_value :short, col
  end

  # Returns all versions of 2-byte column values as Fixnums in a Hash indexed by their timestamps
  # @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  # @return [Hash<Fixnum, Fixnum>]
  def shorts col
    decode_value :short, col, true
  end

  # Returns the latest 4-byte column value as a Fixnum
  # @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  # @return [Fixnum]
  def int col
    decode_value :int, col
  end

  # Returns all versions of 4-byte column values as Fixnums in a Hash indexed by their timestamps
  # @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  # @return [Hash<Fixnum, Fixnum>]
  def ints col
    decode_value :int, col, true
  end

  # Returns the latest 8-byte column value as a Fixnum
  # @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  # @return [Fixnum]
  def fixnum col
    decode_value :fixnum, col
  end
  alias long fixnum

  # Returns all versions of 8-byte column values as Fixnums in a Hash indexed by their timestamps
  # @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  # @return [Hash<Fixnum, Fixnum>]
  def fixnums col
    decode_value :fixnum, col, true
  end
  alias longs fixnums

  # Returns the latest column value as a BigDecimal
  # @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  # @return [BigDecimal]
  def bigdecimal col
    decode_value :bigdecimal, col
  end

  # Returns all versions of column values as BigDecimals in a Hash indexed by their timestamps
  # @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  # @return [Hash<Fixnum, BigDecimal>]
  def bigdecimals col
    decode_value :bigdecimal, col, true
  end

  # Returns the latest column value as a Float
  # @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  # @return [Float]
  def float col
    decode_value :float, col
  end
  alias double float

  # Returns all versions of column values as Floats in a Hash indexed by their timestamps
  # @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  # @return [Hash<Fixnum, Float>]
  def floats col
    decode_value :float, col, true
  end
  alias doubles floats

  # Returns the latest column value as a boolean value
  # @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  # @return [true, false]
  def boolean col
    decode_value :boolean, col
  end
  alias bool boolean

  # Returns all versions of column values as boolean values in a Hash indexed by their timestamps
  # @param [String, Array] column Column name as String or 2-element Array of family and qualifier
  # @return [Hash<Fixnum, true|false>]
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
    cf, cq, _ = @table.lookup_and_parse col, true
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

  HASH_TEMPLATE = {}.tap { |h|
    h.instance_eval do
      def [] key
        # %w[cf x]
        if key.is_a?(Array) && key.length == 2
          key = [key[0].to_sym, ByteArray[key[1]]]
        # %[cf:x]
        elsif key.is_a?(String) && key.index(':')
          cf, cq = key.split(':', 2)
          key = [cf.to_sym, ByteArray[cq]]
        end
        super key
      end
    end
  }

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

