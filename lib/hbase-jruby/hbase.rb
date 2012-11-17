require 'java'

# @author Junegunn Choi <junegunn.c@gmail.com>
# @!attribute [r] admin
#   @return [org.apache.hadoop.hbase.client.HBaseAdmin]
# @!attribute [r] config
#   @return [org.apache.hadoop.hbase.HBaseConfiguration]
class HBase
  attr_reader :admin
  attr_reader :config

  # Connects to HBase
  # @param [Hash] config A key-value pairs to build HBaseConfiguration from
  def initialize config = {}
    @@imported ||= begin
      import org.apache.hadoop.hbase.HBaseConfiguration
      import org.apache.hadoop.hbase.HColumnDescriptor
      import org.apache.hadoop.hbase.HTableDescriptor
      import org.apache.hadoop.hbase.KeyValue
      import org.apache.hadoop.hbase.client.Delete
      import org.apache.hadoop.hbase.client.Get
      import org.apache.hadoop.hbase.client.HBaseAdmin
      import org.apache.hadoop.hbase.client.HConnectionManager
      import org.apache.hadoop.hbase.client.HTablePool
      import org.apache.hadoop.hbase.client.Increment
      import org.apache.hadoop.hbase.client.Put
      import org.apache.hadoop.hbase.client.Scan
      import org.apache.hadoop.hbase.filter.BinaryComparator
      import org.apache.hadoop.hbase.filter.CompareFilter
      import org.apache.hadoop.hbase.filter.FilterBase
      import org.apache.hadoop.hbase.filter.FilterList
      import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
      import org.apache.hadoop.hbase.filter.KeyOnlyFilter
      import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
      import org.apache.hadoop.hbase.io.hfile.Compression::Algorithm
      import org.apache.hadoop.hbase.regionserver.StoreFile::BloomType
      import org.apache.hadoop.hbase.util.Bytes
    end

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
  # @param [Hash] opts
  # @option opts [true, false] :string_rowkey (true) Stringify rowkey
  # @return [HBase::Table]
  def table table_name, opts = {}
    opts = { :string_rowkey => true }.merge opts
    ht = HBase::Table.send :new, @admin, @htable_pool, table_name, opts[:string_rowkey]

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

