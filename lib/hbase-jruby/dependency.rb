require 'java'
require 'open-uri'
require 'tempfile'

# HBase connection
class HBase
  class << self
    # Resolve Hadoop and HBase dependency with Maven or hbase command (Experimental)
    # @param [String] dist Distribution version or path to pom.xml file
    # @param [true, false] verbose Verbose output
    # @return [Array<String>] Loaded JAR files
    def resolve_dependency! dist, verbose = false
      silencer = verbose ? '' : '> /dev/null'
      tempfiles = []
      jars =
        if dist == :hbase
          # Check for hbase executable
          hbase = `which hbase`
          raise RuntimeError, "Cannot find executable `hbase`" if hbase.empty?
          `hbase classpath`.split(':')
        else
          # Check for Maven executable
          mvn = `which mvn`
          raise RuntimeError, "Cannot find executable `mvn`" if mvn.empty?

          if File.exists?(dist)
            path = dist
          else
            path = File.expand_path("../pom/pom.xml", __FILE__)
            profile = "-P #{dist}"
          end

          # Download dependent JAR files and build classpath string
          tempfiles << tf = Tempfile.new('hbase-jruby-classpath')
          tf.close(false)
          system "mvn org.apache.maven.plugins:maven-dependency-plugin:2.5.1:resolve org.apache.maven.plugins:maven-dependency-plugin:2.5.1:build-classpath -Dsilent=true -Dmdep.outputFile=#{tf.path} #{profile} -f #{path} #{silencer}"

          raise RuntimeError.new("Error occurred. Set verbose parameter to see the log.") unless $?.exitstatus == 0

          output = File.read(tf.path)
          raise ArgumentError.new("Invalid profile: #{dist}") if output.empty?
          File.read(tf.path).split(':')
        end

      # Load jars
      jars_loaded = jars.select { |jar|
        File.exists?(jar) &&
        File.extname(jar) == '.jar' &&
        require(jar)
      }

      # Try importing Java classes again
      not_found = HBase.import_java_classes!
      if verbose && !not_found.empty?
        warn "Java classes not found: #{not_found.join(', ')}"
      end

      return jars_loaded
    ensure
      tempfiles.each { |tempfile| tempfile.unlink rescue nil }
    end

    # Import Java classes (Prerequisite for classes in hbase-jruby)
    # @return [Array<String>] List of Java classes not found
    def import_java_classes!
      imp = lambda { |hash|
        hash.map { |base, classes|
          base.class_eval do
            classes.map { |klass|
              begin
                import klass
                nil
              rescue NameError => e
                klass
              end
            }.compact
          end
        }.flatten
      }

      imp.call(
        HBase => %w[
          org.apache.hadoop.hbase.HBaseConfiguration
          org.apache.hadoop.hbase.client.HBaseAdmin
          org.apache.hadoop.hbase.client.HConnectionManager
          org.apache.hadoop.hbase.client.HTablePool
        ],
        HBase::Util => %w[
          java.nio.ByteBuffer
          org.apache.hadoop.hbase.KeyValue
          org.apache.hadoop.hbase.util.Bytes
        ],
        HBase::ByteArray => %w[
          java.util.Arrays
          org.apache.hadoop.hbase.util.Bytes
        ],
        HBase::Cell => %w[
          org.apache.hadoop.hbase.KeyValue
        ],
        HBase::Result => %w[
          org.apache.hadoop.hbase.util.Bytes
        ],
        HBase::ColumnKey => %w[
          java.util.Arrays
          org.apache.hadoop.hbase.util.Bytes
        ],
        HBase::Table => %w[
          org.apache.hadoop.hbase.HColumnDescriptor
          org.apache.hadoop.hbase.HTableDescriptor
          org.apache.hadoop.hbase.client.Delete
          org.apache.hadoop.hbase.client.Increment
          org.apache.hadoop.hbase.client.Put
          org.apache.hadoop.hbase.io.hfile.Compression
          org.apache.hadoop.hbase.regionserver.StoreFile
        ],
        HBase::Scoped => %w[
          org.apache.hadoop.hbase.client.Get
          org.apache.hadoop.hbase.client.Scan
          org.apache.hadoop.hbase.filter.BinaryComparator
          org.apache.hadoop.hbase.filter.ColumnPaginationFilter
          org.apache.hadoop.hbase.filter.ColumnRangeFilter
          org.apache.hadoop.hbase.filter.CompareFilter
          org.apache.hadoop.hbase.filter.FilterBase
          org.apache.hadoop.hbase.filter.FilterList
          org.apache.hadoop.hbase.filter.KeyOnlyFilter
          org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
          org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter
          org.apache.hadoop.hbase.filter.PrefixFilter
          org.apache.hadoop.hbase.filter.RegexStringComparator
          org.apache.hadoop.hbase.filter.RowFilter
          org.apache.hadoop.hbase.filter.SingleColumnValueFilter
          org.apache.hadoop.hbase.filter.WhileMatchFilter
          org.apache.hadoop.hbase.client.coprocessor.AggregationClient
          org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter
        ]).tap { |not_found|

        if not_found.empty?
          self.instance_eval do
            def import_java_classes!
              []
            end
          end
        end
      }
    end
  end#class << self
end
