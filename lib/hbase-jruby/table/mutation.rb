class HBase
class Table
# Generate single-row mutation objects
class Mutation
  include HBase::Util

  def initialize table
    @table = table
  end

  def put rowkey, props
    Put.new(Util.to_bytes rowkey).tap { |put|
      props.each do |col, val|
        next if val.nil?

        cf, cq, type = @table.lookup_and_parse col, true

        case val
        when Hash
          val.each do |t, v|
            case t
            # Timestamp / Ruby Time
            when Time, Fixnum
              put.add cf, cq, time_to_long(t), Util.to_typed_bytes(type, v)
            # Types: :byte, :short, :int, ...
            else
              put.add cf, cq, Util.to_typed_bytes(t, v)
            end unless v.nil?
          end
        else
          put.add cf, cq, Util.to_typed_bytes(type, val)
        end
      end
      raise ArgumentError, "no column to put" if put.empty?
    }
  end

  def delete rowkey, *extra
    Delete.new(Util.to_bytes rowkey).tap { |del|
      cf = cq = nil
      prcd = false

      prc = lambda do
        unless prcd
          if cq
            # Delete all versions
            del.deleteColumns cf, cq
          elsif cf
            del.deleteFamily cf
          end
        end
      end

      extra.each do |x|
        case x
        when Fixnum, Time
          if cq
            del.deleteColumn cf, cq, time_to_long(x)
            prcd = true
          else
            raise ArgumentError, 'qualifier not given'
          end
        else
          prc.call
          cf, cq, _ = @table.lookup_and_parse x, false
          prcd = false
        end
      end
      prc.call
    }
  end

  def increment rowkey, *spec
    if spec.first.is_a?(Hash)
      spec = spec.first
    else
      c, b = spec
      spec = { c => (b || 1) }
    end

    Increment.new(Util.to_bytes rowkey).tap { |inc|
      spec.each do |col, by|
        cf, cq, _ = @table.lookup_and_parse col, true
        inc.addColumn cf, cq, by
      end
    }
  end

  def append rowkey, spec
    Append.new(Util.to_bytes rowkey).tap { |apnd|
      spec.each do |col, val|
        cf, cq, _ = @table.lookup_and_parse col, true
        apnd.add(cf, cq, Util.to_bytes(val))
      end
    }
  end

  def mutate rowkey
    rm = Mutator.new(self, rowkey)
    yield rm
    org.apache.hadoop.hbase.client.RowMutations.new(Util.to_bytes rowkey).tap { |m|
      rm.mutations.each do |action|
        m.add action
      end
    }
  end

  class Mutator
    attr_reader :mutations

    def initialize mutation, rowkey
      @mutation  = mutation
      @rowkey    = rowkey
      @mutations = []
    end

    # @param [Hash] props Column values
    def put props
      @mutations << @mutation.put(@rowkey, props)
      self
    end

    def delete *args
      @mutations << @mutation.delete(@rowkey, *args)
      self
    end
  end#RowMutation
end#Mutation
end#Table
end#HBase
