require 'java'

# @author Junegunn Choi <junegunn.c@gmail.com>
# @!attribute [r] admin
#   @return [org.apache.hadoop.hbase.client.HBaseAdmin]
# @!attribute [r] config
#   @return [org.apache.hadoop.hbase.HBaseConfiguration]
class HBase
  attr_reader :admin
  attr_reader :config

  include Util

  # Connects to HBase
  # @param [Hash] config A key-value pairs to build HBaseConfiguration from
  def initialize config = {}
    Util.import_java_classes!

    @config =
      case config
      when HBaseConfiguration
        config
      else
        HBaseConfiguration.create.tap do |hbcfg|
          config.each do |k, v|
            hbcfg.set k.to_s, v.to_s
          end
        end
      end
    @admin = HBaseAdmin.new @config
    @htable_pool = HTablePool.new @config, java.lang.Integer::MAX_VALUE
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
    @admin.list_tables.map(&:name_as_string)
  end

  # Creates HBase::Table instance for the specified name
  # @param [String, Symbol] table_name
  # @return [HBase::Table]
  def table table_name
    ht = HBase::Table.send :new, @admin, @htable_pool, table_name

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

