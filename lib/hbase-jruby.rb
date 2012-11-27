unless RUBY_PLATFORM =~ /java/
  raise LoadError, 'Only supports JRuby'
end

require "hbase-jruby/version"
require "hbase-jruby/dependency"
require "hbase-jruby/util"
require "hbase-jruby/byte_array"
require "hbase-jruby/column_key"
require "hbase-jruby/cell"
require "hbase-jruby/admin"
require "hbase-jruby/scoped/aggregation"
require "hbase-jruby/scoped"
require "hbase-jruby/table"
require "hbase-jruby/result"
require 'hbase-jruby/hbase'

