class HBase
class Table
# Class used to register actions to perform in batch
class BatchAction
  attr_reader :actions
  attr_reader :types

  class BatchGetScoped
    # @see HBase::Scoped#get
    def get rowkey
      @callback.call @scoped.send(:getify, rowkey)
    end

    [:range, :project, :filter, :versions, :time_range, :at].each do |method|
      define_method(method) do |*args|
        BatchGetScoped.send(:new, @scoped.send(method, *args), @callback)
      end
    end

  private
    def initialize scoped, callback
      @scoped   = scoped
      @callback = callback
    end
  end

  # @see HBase::Table#put
  def put *args
    @actions << { :type => :put, :action => @mutation.put(*args) }
  end

  # @see HBase::Table#delete
  def delete *args
    @actions << { :type => :delete, :action => @mutation.delete(*args) }
  end

  # @see HBase::Table#append
  def append *args
    @actions << { :type => :append, :action => @mutation.append(*args) }
  end

  # @see HBase::Table#increment
  def increment *args
    @actions << { :type => :increment, :action => @mutation.increment(*args) }
  end

  # @see HBase::Table#mutate
  def mutate *args, &blk
    @actions << { :type => :mutate, :action => @mutation.mutate(*args, &blk) }
  end

  [:get, :range, :project, :filter, :versions, :time_range, :at].each do |method|
    define_method(method) do |*args|
      BatchGetScoped.send(:new, @table.scoped, proc { |get|
        @actions << { :type => :get, :action => get }
      }).send(method, *args)
    end
  end

private
  def initialize table, mutation
    @table    = table
    @mutation = mutation
    @actions  = []
  end
end#BatchAction
end#Table
end#HBase

