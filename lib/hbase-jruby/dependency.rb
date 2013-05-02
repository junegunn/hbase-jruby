require 'java'
require 'open-uri'
require 'tempfile'
require 'erb'

# HBase connection
class HBase

  # @private
  # https://github.com/apache/hbase/tags
  # http://search.maven.org/#search%7Cgav%7C1%7Cg%3A%22org.apache.hbase%22%20AND%20a%3A%22hbase%22
  # https://ccp.cloudera.com/display/SUPPORT/CDH+Downloads
  SUPPORTED_PROFILES = {
    # Prefix => Latest known version
    'cdh4.2' => 'cdh4.2.1',
    'cdh4.1' => 'cdh4.1.4',
    'cdh3'   => 'cdh3u6',
    '0.95'   => '0.95.0',
    '0.94'   => '0.94.6.1',
    '0.92'   => '0.92.2',
  }

  class << self
    # @overload resolve_dependency!(dist, options)
    #   Resolve Hadoop and HBase dependency with a predefined Maven profile
    #   @param [String] dist HBase distribution: cdh4.2, cdh4.1, cdh3, 0.94, 0.92, local
    #   @param [Hash] options Options
    #   @option options [Boolean] :verbose Enable verbose output
    #   @return [Array<String>] Loaded JAR files
    # @overload resolve_dependency!(pom_path, options)
    #   Resolve Hadoop and HBase dependency with the given Maven POM file
    #   @param [String] pom_path Path to POM file
    #   @param [Hash] options Options
    #   @option options [Boolean] :verbose Enable verbose output
    #   @option options [String] :profile Maven profile
    #   @return [Array<String>] Loaded JAR files
    def resolve_dependency! dist, options = {}
      # Backward-compatibility
      options = { :verbose => options } if [true, false].include?(options)
      options = { :verbose => false }.merge(options)

      dist    = dist.to_s
      verbose = options[:verbose]

      silencer = verbose ? '' : '> /dev/null'
      tempfiles = []

      jars =
        if %w[hbase local].include?(dist)
          # Check for hbase executable
          hbase = `which hbase`
          raise RuntimeError, "Cannot find `hbase` executable" if hbase.empty?
          `hbase classpath`.split(':')
        else
          # Check for Maven executable
          mvn = `which mvn`
          raise RuntimeError, "Cannot find `mvn` executable" if mvn.empty?

          # POM file path given (with optional profile)
          if File.exists?(dist)
            path = dist
            profile = options[:profile] && "-P #{options[:profile]}"
          # Predefined dependencies
          else
            matched_profiles = SUPPORTED_PROFILES.keys.select { |pf| dist.start_with? pf }
            if matched_profiles.length != 1
              raise ArgumentError, "Invalid profile: #{dist}"
            end
            matched_profile = matched_profiles.first
            profiles = SUPPORTED_PROFILES.dup
            profiles[matched_profile] = dist if dist != matched_profile
            tempfiles << tf = Tempfile.new('hbase-jruby-pom')
            erb = ERB.new(File.read File.expand_path("../pom/pom.xml.erb", __FILE__))
            tf << erb.result(binding)
            tf.close(false)
            path = tf.path
            profile = "-P #{matched_profile}"
          end

          # Download dependent JAR files and build classpath string
          tempfiles << tf = Tempfile.new('hbase-jruby-classpath')
          tf.close(false)
          system "mvn org.apache.maven.plugins:maven-dependency-plugin:2.5.1:resolve org.apache.maven.plugins:maven-dependency-plugin:2.5.1:build-classpath -Dsilent=true -Dmdep.outputFile=#{tf.path} #{profile} -f #{path} #{silencer}"

          raise RuntimeError.new("Error occurred. Set verbose option to see the log.") unless $?.exitstatus == 0

          if File.read(tf.path).empty?
            desc =
              if options[:profile]
                "#{dist} (#{options[:profile]})"
              else
                dist
              end
            raise ArgumentError.new("Invalid profile: #{desc}")
          end
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
    # @return [Array<String>] List of Java classes *NOT* found
    def import_java_classes!
      imp = lambda { |hash|
        hash.map { |base, classes|
          base.class_eval do
            classes.map { |klass|
              begin
                java_import klass
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
