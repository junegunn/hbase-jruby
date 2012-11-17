require 'java'
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

          path = [
            dist.to_s,
            File.expand_path("../pom/#{dist.downcase}.xml", __FILE__)
          ].select { |f| File.exists? f }.first

          # TODO: remote fetch from github head
          raise ArgumentError, "Invalid distribution: #{dist}" unless path

          # Download dependent JAR files and build classpath string
          tf = Tempfile.new 'hbase-jruby-classpath'
          tf.close(false) # Unlink later
          system "mvn org.apache.maven.plugins:maven-dependency-plugin:2.5.1:resolve org.apache.maven.plugins:maven-dependency-plugin:2.5.1:build-classpath -Dsilent=true -Dmdep.outputFile=#{tf.path} -f #{path} #{silencer}"
          File.read(tf.path).split(':')
        end

      # Load jars
      jars.select { |jar| File.exists?(jar) && File.extname(jar) == '.jar' }.select do |jar|
        require jar
      end
    ensure
      tf.unlink if tf
    end
  end
end
