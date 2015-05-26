unless RUBY_PLATFORM =~ /java/
  raise LoadError, 'Only supports JRuby'
end

class HBase
  @@log4j = nil
end

require 'hbase-jruby/version'
require 'hbase-jruby/util'
require 'hbase-jruby/byte_array'
require 'hbase-jruby/cell'
require 'hbase-jruby/admin'
require 'hbase-jruby/scoped/aggregation'
require 'hbase-jruby/scoped'
require 'hbase-jruby/schema'
require 'hbase-jruby/batch_exception'
require 'hbase-jruby/table'
require 'hbase-jruby/table/mutation'
require 'hbase-jruby/table/batch_action'
require 'hbase-jruby/table/admin'
require 'hbase-jruby/table/inspection'
require 'hbase-jruby/table/checked_operation'
require 'hbase-jruby/row'
require 'hbase-jruby/hbase'
require 'hbase-jruby/dependency'

