require 'java'
require 'set'
require 'thread'

# @author Junegunn Choi <junegunn.c@gmail.com>
# @!attribute [r] config
#   @return [org.apache.hadoop.conf.Configuration]
class HBase
  attr_reader :config, :schema

  include Admin
  include HBase::Util

  # @overload HBase.log4j=(filename)
  #   Configure Log4j logging with the given file
  #   @param [String] filename Path to log4j.properties or log4j.xml file
  #   @return [String]
  # @overload HBase.log4j=(hash)
  #   Configure Log4j logging with the given Hash
  #   @param [Hash] hash Log4j properties in Ruby Hash
  #   @return [Hash]
  # @overload HBase.log4j=(props)
  #   Configure Log4j logging with the given Properties
  #   @param [java.util.Properties] props Properties object
  #   @return [java.util.Properties]
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
  # @overload initialize(zookeeper_quorum)
  #   @param [String] zookeeper_quorum hbase.zookeeper.quorum
  # @overload initialize(config)
  #   @param [Hash] config A key-value pairs to build HBaseConfiguration from
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
      when String
        HBaseConfiguration.create.tap do |hbcfg|
          hbcfg.set 'hbase.zookeeper.quorum', config
        end
      when org.apache.hadoop.conf.Configuration
        config
      else
        HBaseConfiguration.create.tap do |hbcfg|
          config.each do |k, v|
            hbcfg.set k.to_s, v.to_s
          end
        end
      end
    @connection = HConnectionManager.createConnection @config
    @htable_pool =
      if @connection.respond_to?(:getTable)
        nil
      else
        HTablePool.new @config, java.lang.Integer::MAX_VALUE
      end
    @mutex   = Mutex.new
    @schema  = Schema.new
    @closed  = false
  end

  # Returns if this instance is backed by an HTablePool which is deprecated
  # in the recent versions of HBase
  # @return [Boolean]
  def use_table_pool?
    !@htable_pool.nil?
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

  # Closes the connection, and clean up thread-local cache
  # @return [nil]
  def close
    @mutex.synchronize do
      unless @closed
        @closed = true
        @htable_pool.close if use_table_pool?
        @connection.close

        # To be deprecated
        begin
          HConnectionManager.deleteConnection(@config)
        rescue ArgumentError
          # HBase 0.92 or below
          HConnectionManager.deleteConnection(@config, true)
        end if use_table_pool?
      end
    end

    thread_local.delete self

    nil
  end

  # Returns whether if the connection is closed
  # @return [Boolean]
  def closed?
    @closed
  end

  # Returns the list of HBase::Table instances
  # @return [Array<HBase::Table>]
  def tables
    table_names.map { |tn| table(tn) }
  end

  # Returns the list of table names
  # @return [Array<String>]
  def table_names
    with_admin { |admin| admin.list_tables.map(&:name_as_string) }
  end
  alias list table_names

  # Creates an HBase::Table instance for the specified name
  # @param [#to_s] table_name The name of the table
  # @param [Hash] opts Options
  #   @option opts [Boolean] :cache Use thread-local cache (default: false)
  # @return [HBase::Table]
  def table table_name, opts = {}
    check_closed

    ht = HBase::Table.send :new, self, @config, table_name, opts[:cache]

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
  # @deprecated
  # @return [nil]
  def reset_table_pool
    raise RuntimeError, 'Not using table pool' unless use_table_pool?

    @mutex.synchronize do
      @htable_pool.close
      @htable_pool = HTablePool.new @config, java.lang.Integer::MAX_VALUE
    end
    nil
  end

private
  def get_htable name
    (@htable_pool || @connection).get_table name
  end

  def check_closed
    raise RuntimeError, "Connection already closed" if closed?
  end
end

