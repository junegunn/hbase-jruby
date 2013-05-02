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
    if block_given?
      @result.raw.each do |kv|
        yield Cell.new(kv)
      end
    else
      self
    end
  end

  # Returns Hash representation of the row.
  # @param [Hash] schema Schema used to parse byte arrays (column family, qualifier and the value)
  # @return [Hash] Hash representation of the row indexed by ColumnKey
  def to_hash schema = {}
    schema = parse_schema schema

    HASH_TEMPLATE.clone.tap { |ret|
      @result.getNoVersionMap.each do |cf, cqmap|
        cqmap.each do |cq, val|
          name = ColumnKey.new(cf, cq)
          type = schema[name]
          ret[name] = type ? Util.from_bytes(type, val) : val
        end
      end
    }
  end

  # Returns Hash representation of the row.
  # Each column value again is represented as a Hash indexed by timestamp of each version.
  # @param [Hash] schema Schema used to parse byte arrays (column family, qualifier and the value)
  # @return [Hash<Hash>] Hash representation of the row indexed by ColumnKey
  def to_hash_with_versions schema = {}
    schema = parse_schema schema

    HASH_TEMPLATE.clone.tap { |ret|
      @result.getMap.each do |cf, cqmap|
        cqmap.each do |cq, tsmap|
          name = ColumnKey.new(cf, cq)
          type = schema[name]

          ret[name] =
            Hash[
              tsmap.map { |ts, val|
                [ ts,  type ? Util.from_bytes(type, val) : val ]
              }
            ]
        end
      end
    }
  end

  # Returns column values as byte arrays
  # @overload raw(column)
  #   Returns the latest column value as a byte array
  #   @param [String, HBase::ColumnKey] column "FAMILY:QUALIFIER" expression or ColumnKey object.
  #   @return [byte[]] Byte array representation of the latest value
  # @overload raw(columns)
  #   For each column specified,
  #   returns the latest column value as a byte array
  #   @param [<String|HBase::ColumnKey>] column "FAMILY:QUALIFIER" expression or ColumnKey object.
  #   @return [Array<byte[]>] Byte array representations of the latest values
  def raw cols
    ret = get_values [*cols]

    case cols
    when Array
      ret
    else
      ret.first
    end
  end

  # Returns all versions of column values as byte arrays in a Hash indexed by their timestamps
  # @overload raws(column)
  #   Returns all versions of column values as byte arrays in a Hash indexed by their timestamps
  #   @param [String, HBase::ColumnKey] column "FAMILY:QUALIFIER" expression or ColumnKey object.
  #   @return [Hash<Fixnum, byte[]>]
  # @overload raws(columns)
  #   For each column specified,
  #   returns all versions of column values as byte arrays in a Hash indexed by their timestamps
  #   @param [Array<String|HBase::ColumnKey>] columns Array of "FAMILY:QUALIFIER" expressions and ColumnKey objects.
  #   @return [Array<Hash<Fixnum, byte[]>>]
  def raws cols
    ret = get_values [*cols], true

    case cols
    when Array
      ret
    else
      ret.first
    end
  end

  # Returns column values as Strings
  # @overload string(column)
  #   Returns the latest column value as a String
  #   @param [String, HBase::ColumnKey] column "FAMILY:QUALIFIER" expression or ColumnKey object.
  #   @return [String]
  # @overload string(columns)
  #   For each column specified,
  #   returns the latest column value as a String
  #   @param [Array<String|HBase::ColumnKey>] columns Array of "FAMILY:QUALIFIER" expressions and ColumnKey objects.
  #   @return [Array<String>]
  def string cols
    decode_values :string, cols
  end
  alias str string

  # Returns all versions of column values as Strings in a Hash indexed by their timestamps
  # @overload strings(column)
  #   Returns all versions of column values as Strings in a Hash indexed by their timestamps
  #   @param [String, HBase::ColumnKey] column "FAMILY:QUALIFIER" expression or ColumnKey object.
  #   @return [Hash<Fixnum, String>]
  # @overload strings(columns)
  #   For each column specified,
  #   returns all versions of column values as Strings in a Hash indexed by their timestamps
  #   @param [Array<String|HBase::ColumnKey>] columns Array of "FAMILY:QUALIFIER" expressions and ColumnKey objects.
  #   @return [Array<Hash<Fixnum, String>>]
  def strings cols
    decode_values :string, cols, true
  end
  alias strs strings

  # Returns column values as Symbols
  # @overload symbol(column)
  #   Returns the latest column value as a Symbol
  #   @param [String, HBase::ColumnKey] column "FAMILY:QUALIFIER" expression or ColumnKey object.
  #   @return [Symbol]
  # @overload symbol(columns)
  #   For each column specified,
  #   returns the latest column values as a Symbol
  #   @param [Array<String|HBase::ColumnKey>] columns Array of "FAMILY:QUALIFIER" expressions and ColumnKey objects.
  #   @return [Array<Symbol>]
  def symbol cols
    decode_values :symbol, cols
  end
  alias sym symbol

  # Returns all versions of column values as Symbols in a Hash indexed by their timestamps
  # @overload symbols(column)
  #   Returns all versions of column values as Symbols in a Hash indexed by their timestamps
  #   @param [String, HBase::ColumnKey] column "FAMILY:QUALIFIER" expression or ColumnKey object.
  #   @return [Hash<Fixnum, Symbol>]
  # @overload symbols(columns)
  #   For each column specified,
  #   returns all versions of column values as Symbols in a Hash indexed by their timestamps
  #   @param [Array<String|HBase::ColumnKey>] columns Array of "FAMILY:QUALIFIER" expressions and ColumnKey objects.
  #   @return [Array<Hash<Fixnum, Symbol>>]
  def symbols cols
    decode_values :symbol, cols, true
  end
  alias syms symbols

  # Returns 1-byte column values as Fixnums
  # @overload byte(column)
  #   Returns the latest column value as a Fixnum
  #   @param [String, HBase::ColumnKey] column "FAMILY:QUALIFIER" expression or ColumnKey object.
  #   @return [Fixnum]
  # @overload byte(columns)
  #   For each column specified,
  #   returns the latest 1-byte column values as a Fixnum
  #   @param [Array<String|HBase::ColumnKey>] columns Array of "FAMILY:QUALIFIER" expressions and ColumnKey objects.
  #   @return [Array<Fixnum>]
  def byte cols
    decode_values :byte, cols
  end

  # Returns all versions of 1-byte column values as Fixnums in a Hash indexed by their timestamps
  # @overload bytes(column)
  #   Returns all versions of column values as Fixnums in a Hash indexed by their timestamps
  #   @param [String, HBase::ColumnKey] column "FAMILY:QUALIFIER" expression or ColumnKey object.
  #   @return [Hash<Fixnum, Fixnum>]
  # @overload bytes(columns)
  #   For each column specified,
  #   returns all versions of 1-byte column values as Fixnums in a Hash indexed by their timestamps
  #   @param [Array<String|HBase::ColumnKey>] columns Array of "FAMILY:QUALIFIER" expressions and ColumnKey objects.
  #   @return [Array<Hash<Fixnum, Fixnum>>]
  def bytes cols
    decode_values :byte, cols, true
  end

  # Returns 2-byte column values as Fixnums
  # @overload short(column)
  #   Returns the latest 2-byte column value as a Fixnum
  #   @param [String, HBase::ColumnKey] column "FAMILY:QUALIFIER" expression or ColumnKey object.
  #   @return [Fixnum]
  # @overload short(columns)
  #   For each column specified,
  #   returns the latest 2-byte column values as a Fixnum
  #   @param [Array<String|HBase::ColumnKey>] columns Array of "FAMILY:QUALIFIER" expressions and ColumnKey objects.
  #   @return [Array<Fixnum>]
  def short cols
    decode_values :short, cols
  end

  # Returns all versions of 2-byte column values as Fixnums in a Hash indexed by their timestamps
  # @overload shorts(column)
  #   Returns all versions of 2-byte column values as Fixnums in a Hash indexed by their timestamps
  #   @param [String, HBase::ColumnKey] column "FAMILY:QUALIFIER" expression or ColumnKey object.
  #   @return [Hash<Fixnum, Fixnum>]
  # @overload shorts(columns)
  #   For each column specified,
  #   returns all versions of 2-byte column values as Fixnums in a Hash indexed by their timestamps
  #   @param [Array<String|HBase::ColumnKey>] columns Array of "FAMILY:QUALIFIER" expressions and ColumnKey objects.
  #   @return [Array<Hash<Fixnum, Fixnum>>]
  def shorts cols
    decode_values :short, cols, true
  end

  # Returns 4-byte column values as Fixnums
  # @overload int(column)
  #   Returns the latest 4-byte column value as a Fixnum
  #   @param [String, HBase::ColumnKey] column "FAMILY:QUALIFIER" expression or ColumnKey object.
  #   @return [Fixnum]
  # @overload int(columns)
  #   For each column specified,
  #   returns the latest 4-byte column values as a Fixnum
  #   @param [Array<String|HBase::ColumnKey>] columns Array of "FAMILY:QUALIFIER" expressions and ColumnKey objects.
  #   @return [Array<Fixnum>]
  def int cols
    decode_values :int, cols
  end

  # Returns all versions of 4-byte column values as Fixnums in a Hash indexed by their timestamps
  # @overload ints(column)
  #   Returns all versions of 4-byte column values as Fixnums in a Hash indexed by their timestamps
  #   @param [String, HBase::ColumnKey] column "FAMILY:QUALIFIER" expression or ColumnKey object.
  #   @return [Hash<Fixnum, Fixnum>]
  # @overload ints(columns)
  #   For each column specified,
  #   returns all versions of 4-byte column values as Fixnums in a Hash indexed by their timestamps
  #   @param [Array<String|HBase::ColumnKey>] columns Array of "FAMILY:QUALIFIER" expressions and ColumnKey objects.
  #   @return [Array<Hash<Fixnum, Fixnum>>]
  def ints cols
    decode_values :int, cols, true
  end

  # Returns 8-byte column values as Fixnums
  # @overload fixnum(column)
  #   Returns the latest 8-byte column value as a Fixnum
  #   @param [String, HBase::ColumnKey] column "FAMILY:QUALIFIER" expression or ColumnKey object.
  #   @return [Fixnum]
  # @overload fixnum(columns)
  #   For each column specified,
  #   returns the latest 8-byte column values as a Fixnum
  #   @param [Array<String|HBase::ColumnKey>] columns Array of "FAMILY:QUALIFIER" expressions and ColumnKey objects.
  #   @return [Array<Fixnum>]
  def fixnum cols
    decode_values :fixnum, cols
  end
  alias long fixnum

  # Returns all versions of 8-byte column values as Fixnums in a Hash indexed by their timestamps
  # @overload fixnums(column)
  #   Returns all versions of 8-byte column values as Fixnums in a Hash indexed by their timestamps
  #   @param [String, HBase::ColumnKey] column "FAMILY:QUALIFIER" expression or ColumnKey object.
  #   @return [Hash<Fixnum, Fixnum>]
  # @overload fixnums(columns)
  #   For each column specified,
  #   returns all versions of 8-byte column values as Fixnums in a Hash indexed by their timestamps
  #   @param [Array<String|HBase::ColumnKey>] columns Array of "FAMILY:QUALIFIER" expressions and ColumnKey objects.
  #   @return [Array<Hash<Fixnum, Fixnum>>]
  def fixnums cols
    decode_values :fixnum, cols, true
  end
  alias longs fixnums

  # Returns column values as Bigdecimals
  # @overload bigdecimal(column)
  #   Returns the latest column value as a BigDecimal
  #   @param [String, HBase::ColumnKey] column "FAMILY:QUALIFIER" expression or ColumnKey object.
  #   @return [BigDecimal]
  # @overload bigdecimal(columns)
  #   For each column specified,
  #   returns the latest column values as a BigDecimal
  #   @param [Array<String|HBase::ColumnKey>] columns Array of "FAMILY:QUALIFIER" expressions and ColumnKey objects.
  #   @return [Array<BigDecimal>]
  def bigdecimal cols
    decode_values :bigdecimal, cols
  end

  # Returns all versions of column values as BigDecimals in a Hash indexed by their timestamps
  # @overload bigdecimals(column)
  #   Returns all versions of column values as BigDecimals in a Hash indexed by their timestamps
  #   @param [String, HBase::ColumnKey] column "FAMILY:QUALIFIER" expression or ColumnKey object.
  #   @return [Hash<Fixnum, BigDecimal>]
  # @overload bigdecimals(columns)
  #   For each column specified,
  #   returns all versions of column values as BigDecimals in a Hash indexed by their timestamps
  #   @param [Array<String|HBase::ColumnKey>] columns Array of "FAMILY:QUALIFIER" expressions and ColumnKey objects.
  #   @return [Array<Hash<Fixnum, BigDecimal>>]
  def bigdecimals cols
    decode_values :bigdecimal, cols, true
  end

  # Returns column values as Floats
  # @overload float(column)
  #   Returns the latest column value as a Float
  #   @param [String, HBase::ColumnKey] column "FAMILY:QUALIFIER" expression or ColumnKey object.
  #   @return [Float]
  # @overload float(columns)
  #   For each column specified,
  #   returns the latest column values as a Float
  #   @param [Array<String|HBase::ColumnKey>] columns Array of "FAMILY:QUALIFIER" expressions and ColumnKey objects.
  #   @return [Array<Float>]
  def float cols
    decode_values :float, cols
  end
  alias double float

  # Returns all versions of column values as Floats in a Hash indexed by their timestamps
  # @overload floats(column)
  #   Returns all versions of column values as Floats in a Hash indexed by their timestamps
  #   @param [String, HBase::ColumnKey] column "FAMILY:QUALIFIER" expression or ColumnKey object.
  #   @return [Hash<Fixnum, Float>]
  # @overload floats(columns)
  #   For each column specified,
  #   returns all versions of column values as Floats in a Hash indexed by their timestamps
  #   @param [Array<String|HBase::ColumnKey>] columns Array of "FAMILY:QUALIFIER" expressions and ColumnKey objects.
  #   @return [Array<Hash<Fixnum, Float>>]
  def floats cols
    decode_values :float, cols, true
  end
  alias doubles floats

  # Returns column values as Booleans
  # @overload boolean(column)
  #   Returns the latest column value as a boolean value
  #   @param [String, HBase::ColumnKey] column "FAMILY:QUALIFIER" expression or ColumnKey object.
  #   @return [true, false]
  # @overload boolean(columns)
  #   For each column specified,
  #   returns the latest column values as a boolean value
  #   @param [Array<String|HBase::ColumnKey>] columns Array of "FAMILY:QUALIFIER" expressions and ColumnKey objects.
  #   @return [Array<true|false>]
  def boolean cols
    decode_values :boolean, cols
  end
  alias bool boolean

  # Returns all versions of column values as Booleans in a Hash indexed by their timestamps
  # @overload booleans(column)
  #   Returns all versions of column values as boolean values in a Hash indexed by their timestamps
  #   @param [String, HBase::ColumnKey] column "FAMILY:QUALIFIER" expression or ColumnKey object.
  #   @return [Hash<Fixnum, true|false>]
  # @overload booleans(columns)
  #   For each column specified,
  #   returns all versions of column values as boolean values in a Hash indexed by their timestamps
  #   @param [Array<String|HBase::ColumnKey>] columns Array of "FAMILY:QUALIFIER" expressions and ColumnKey objects.
  #   @return [Array<Hash<Fixnum, true|false>>]
  def booleans cols
    decode_values :boolean, cols, true
  end
  alias bools booleans

  # Compares two Result instances on their row keys
  def <=> other
    Bytes.compareTo(rowkey(:raw), other.rowkey(:raw))
  end

private
  HASH_TEMPLATE = {}.tap { |h|
    h.instance_eval do
      def [] key
        ck =
          case key
          when ColumnKey
            key
          else
            cf, cq = Util.parse_column_name key
            ColumnKey.new(cf, cq)
          end
        super ck
      end
    end
  }

  def get_values cols, with_versions = false
    raise ArgumentError, "No column expressions specified" if cols.empty?
    cols.map { |col|
      cf, cq = Util.parse_column_name(col)
      if with_versions
        # Need to make it a Ruby hash:
        #   Prevents implicit conversion from ruby type to java type when updating the Hash
        Hash[ allmap.fetch(cf, {}).fetch(cq, {}) ]
      else
        @result.getValue cf, cq
      end
    }
  end

  def decode_values type, cols, with_versions = false
    ret = get_values([*cols], with_versions).map { |v|
      if with_versions
        v.each do |k, raw|
          v[k] = Util.from_bytes type, raw
        end
        v
      else
        Util.from_bytes type, v
      end
    }
    case cols
    when Array
      ret
    else
      ret.first
    end
  end

  # @param [org.apache.hadoop.hbase.client.Result] java_result
  def initialize java_result
    @result = java_result
    @allmap = nil
  end

  def allmap
    @allmap ||= @result.getMap
  end

  def parse_schema schema
    {}.tap { |ret|
      schema.each do |name, type|
        ck =
          case name
          when ColumnKey
            name
          else
            cf, cq = Util.parse_column_name(name)
            ColumnKey.new(cf, cq)
          end
        ret[ck] = type
      end
    }
  end
end#Row
end#HBase

# For backward compatibility
HBase::Result = HBase::Row

