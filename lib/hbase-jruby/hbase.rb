require 'java'

# @author Junegunn Choi <junegunn.c@gmail.com>
# @!attribute [r] config
#   @return [org.apache.hadoop.conf.Configuration]
class HBase
  attr_reader :config

  include Admin

  # @overload HBase.log4j=(filename)
  #   Configure Log4j logging with the given file
  #   @param [String] filename Path to log4j.properties file
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
      arg = props
    end

    org.apache.log4j.PropertyConfigurator.configure arg
  end

  # Connects to HBase
  # @param [Hash] config A key-value pairs to build HBaseConfiguration from
  def initialize config = {}
    unless defined?(Java::OrgApacheHadoopConf::Configuration)
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
    @closed = false
  end

  # Returns an HBaseAdmin object for administration
  # @yield [admin] An HBaseAdmin object
  # @yieldparam [org.apache.hadoop.hbase.client.HBaseAdmin] admin
  # @return [org.apache.hadoop.hbase.client.HBaseAdmin]
  def admin
    check_closed
    if block_given?
      with_admin { |admin| yield admin }
    else
      HBaseAdmin.new @config
    end
  end

  # Closes HTablePool and connection
  # @return [nil]
  def close
    unless @closed
      @htable_pool.close
      HConnectionManager.deleteConnection(@config, true)
      @closed = true
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

    ht = HBase::Table.send :new, self, @config, @htable_pool, table_name

    if block_given?
      yield ht
    else
      ht
    end
  end
  alias [] table

private
  def check_closed
    raise RuntimeError, "Connection already closed" if closed?
  end
end

