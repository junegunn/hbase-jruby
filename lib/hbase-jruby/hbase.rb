require 'java'

# @author Junegunn Choi <junegunn.c@gmail.com>
# @!attribute [r] config
#   @return [org.apache.hadoop.conf.Configuration]
class HBase
  attr_reader :config

  include Admin

  # Connects to HBase
  # @param [Hash] config A key-value pairs to build HBaseConfiguration from
  def initialize config = {}
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
  end

  # Returns an HBaseAdmin object for administration
  # @yield [org.apache.hadoop.hbase.client.HBaseAdmin]
  # @return [org.apache.hadoop.hbase.client.HBaseAdmin]
  def admin
    if block_given?
      with_admin { |admin| yield admin }
    else
      HBaseAdmin.new @config
    end
  end

  # Closes HTablePool and connection
  # @return [nil]
  def close
    @htable_pool.close
    HConnectionManager.deleteConnection(@config, true)
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

  # Creates HBase::Table instance for the specified name
  # @param [#to_s] table_name The name of the table
  # @return [HBase::Table]
  def table table_name
    ht = HBase::Table.send :new, @config, @htable_pool, table_name

    if block_given?
      begin
        yield ht
      ensure
        ht.close
      end
    else
      ht
    end
  end
end

