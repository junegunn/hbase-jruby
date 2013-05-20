require 'forwardable'

class HBase
class Schema
  extend Forwardable
  def_delegators :@schema, :inspect, :to_s

  def initialize
    @schema = {}
    @lookup = {}
  end

  # [cq]
  # [cf:cq]
  # @param [Symbol] table
  # @param [Hash] definition
  def []= table, definition
    if definition.nil? || definition.empty?
      delete table
      return nil
    end

    unless definition.is_a?(Hash)
      raise ArgumentError, 'invalid schema definition: Hash required'
    end
    definition = definition.dup.freeze
    lookup     = empty_lookup_table

    definition.each do |cf, cols|
      unless [Symbol, String].any? { |k| cf.is_a? k }
        raise ArgumentError,
          "invalid schema: use String or Symbol for column family name"
      end

      # CF:CQ => Type shortcut
      cf = cf.to_s
      if cf.index(':')
        cf, q = key.to_s.split ':', 2
        cols = { q => cols }
      else
        raise ArgumentError, "invalid schema: expected Hash" unless cols.is_a?(Hash)
      end

      # Family => { Column => Type }
      cols.each do |cq, type|
        raise ArgumentError, "invalid schema" unless type.is_a?(Symbol)

        # Pattern
        case cq
        when Regexp
          lookup[:pattern][cq] = [cf, nil, type]
        # Exact
        when String, Symbol
          cq = cq.to_s
          cfcq = [cf, cq].join(':')
          [cq, cq.to_sym, cfcq].each do |key|
            lookup[:exact][key] = [cf, cq.to_sym, type]
          end
        else
          raise ArgumentError, "invalid schema"
        end
      end
    end

    table = table.to_sym
    @lookup[table] = lookup
    @schema[table] = definition
  end

  # @private
  # @param [Symbol] table
  # @return [Array] CF, CQ, Type. When not found, nil.
  def lookup table, col
    return nil unless lookup = @lookup[table]

    if match = lookup[:exact][col]
      return match
    elsif pair = lookup[:pattern].find { |k, v| col.to_s =~ k }
      return pair[1].dup.tap { |e| e[1] = col.to_sym }
    end
  end

  # @private
  # @param [Symbol] table
  def lookup_and_parse table, col
    cf, cq, type = lookup table, col
    cf, cq = Util.parse_column_name(cf ? [cf, cq] : col)
    return [cf, cq, type]
  end

  # Delete schema for the table
  # @param [Symbol] table
  def delete table
    table = table.to_sym
    @lookup.delete table
    @schema.delete table
    nil
  end

  # @return [Hash]
  def to_h
    @schema
  end

private
  def empty_lookup_table
    {
      :exact   => {},
      :pattern => {},
    }
  end

end
end

