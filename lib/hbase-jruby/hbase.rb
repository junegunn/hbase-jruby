require 'java'
require 'set'
require 'thread'

# @author Junegunn Choi <junegunn.c@gmail.com>
# @!attribute [r] config
#   @return [org.apache.hadoop.conf.Configuration]
class HBase
  attr_reader :config, :schema

  include Admin

  # @overload HBase.log4j=(filename)
  #   Configure Log4j logging with the given file
  #   @param [String] filename Path to log4j.properties or log4j.xml file
  #   @return [nil]
  # @overload HBase.log4j=(hash)
  #   Configure Log4j logging with the given Hash
  #   @param [Hash] hash Log4j properties in Ruby Hash
  #   @return [nil]
  # @overload HBase.log4j=(props)
  #   Configure Log4j logging with the given Properties
  #   @param [java.util.Properties] props Properties object
  #   @return [nil]
  def self.log4j= arg
    if arg.is_a?(Hash)
      props = java.util.Properties.new
      arg.each do |k, v|
        props.setProperty k.to_s, v.to_s
      end
      org.apache.log4j.PropertyConfigurator.configure props
    else
      case File.extname(arg).downcase
      when '.xml'
        org.apache.log4j.xml.DOMConfigurator.configure arg
      else
        org.apache.log4j.PropertyConfigurator.configure arg
      end
    end
  end

  # Connects to HBase
  # @param [Hash] config A key-value pairs to build HBaseConfiguration from
  def initialize config = {}
    begin
      org.apache.hadoop.conf.Configuration
    rescue NameError
      raise NameError.new(
        "Required Java classes not loaded. Set up CLASSPATH or try `HBase.resolve_dependency!`")
    end

    HBase.import_java_classes!

    @config =
      case config
      when org.apache.hadoop.conf.Configuration
        config
      else
        HBaseConfiguration.create.tap do |hbcfg|
          config.each do |k, v|
            hbcfg.set k.to_s, v.to_s
          end
        end
      end
    @htable_pool = HTablePool.new @config, java.lang.Integer::MAX_VALUE
    @threads = Set.new
    @mutex   = Mutex.new
    @schema  = Schema.new
    @closed  = false
  end

  # Returns an HBaseAdmin object for administration
  # @yield [admin] An HBaseAdmin object
  # @yieldparam [org.apache.hadoop.hbase.client.HBaseAdmin] admin
  # @return [org.apache.hadoop.hbase.client.HBaseAdmin]
  def admin
    if block_given?
      with_admin { |admin| yield admin }
    else
      check_closed
      HBaseAdmin.new @config
    end
  end

  # Closes HTablePool and connection
  # @return [nil]
  def close
    @mutex.synchronize do
      unless @closed
        @closed = true
        close_table_pool
        HConnectionManager.deleteConnection(@config)
      end
    end
  end

  # Returns whether if the connection is closed
  # @return [Boolean]
  def closed?
    @closed
  end

  # Returns the list of HBase::Table instances
  # @return [Array<HBase::Table>]
  def tables
    check_closed
    table_names.map { |tn| table(tn) }
  end

  # Returns the list of table names
  # @return [Array<String>]
  def table_names
    check_closed
    with_admin { |admin| admin.list_tables.map(&:name_as_string) }
  end
  alias list table_names

  # Creates an HBase::Table instance for the specified name
  # @param [#to_s] table_name The name of the table
  # @return [HBase::Table]
  def table table_name
    check_closed

    ht = HBase::Table.send :new, self, @config, table_name

    if block_given?
      yield ht
    else
      ht
    end
  end
  alias [] table

  # Returns an Array of snapshot information
  # @return [Array<Hash>]
  def snapshots
    with_admin { |admin| admin.listSnapshots }.map { |sd|
      props = sd.getAllFields.map { |k, v|
        [k.name.to_sym, v.respond_to?(:name) ? v.name : v]
      }
      Hash[props]
    }
  end

  # @param [Hash] hash
  # @return [HBase::Schema]
  def schema= hash
    unless hash.is_a?(Hash)
      raise ArgumentError, "invalid schema: Hash required"
    end

    schema = Schema.new
    hash.each do |table, definition|
      schema[table] = definition
    end
    @schema = schema
 end

  # Reset underlying HTablePool
  # @return [nil]
  def reset_table_pool
    @mutex.synchronize do
      close_table_pool
      @htable_pool = HTablePool.new @config, java.lang.Integer::MAX_VALUE
    end
    nil
  end

private
  def register_thread t
    @mutex.synchronize do
      check_closed
      @threads << t
    end
  end

  def close_table_pool
    # Close all the HTable instances in the pool
    @htable_pool.close

    # Cleanup thread-local references
    @threads.each do |thr|
      thr[:hbase_jruby].delete self
    end
  end

  def get_htable name
    @htable_pool.get_table name
  end

  def check_closed
    raise RuntimeError, "Connection already closed" if closed?
  end
end

