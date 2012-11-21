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

          distname = dist.downcase.sub(/\.xml$/, '')
          path = [
            File.expand_path("../pom/#{distname}.xml", __FILE__),
            dist.to_s,
          ].select { |f| File.exists? f }.first

          # Try github head
          unless path
            begin
              xml = open("https://raw.github.com/junegunn/hbase-jruby/master/lib/hbase-jruby/pom/#{distname}.xml").read
              tempfiles << tf = Tempfile.new("#{distname}.xml")
              tf.close(false)
              path = tf.path
              File.open(path, 'w') do |f|
                f << xml
              end
            rescue OpenURI::HTTPError => e
              # No such distribution anywhere
            end
          end

          raise ArgumentError, "Invalid distribution: #{dist}" unless path

          # Download dependent JAR files and build classpath string
          tempfiles << tf = Tempfile.new('hbase-jruby-classpath')
          tf.close(false)
          system "mvn org.apache.maven.plugins:maven-dependency-plugin:2.5.1:resolve org.apache.maven.plugins:maven-dependency-plugin:2.5.1:build-classpath -Dsilent=true -Dmdep.outputFile=#{tf.path} -f #{path} #{silencer}"

          raise RuntimeError.new("Error occurred. Set verbose parameter to see the log.") unless $?.exitstatus == 0

          File.read(tf.path).split(':')
        end

      # Load jars
      jars.select { |jar| File.exists?(jar) && File.extname(jar) == '.jar' }.select do |jar|
        require jar
      end
    ensure
      Util.import_java_classes!
      tempfiles.each { |tempfile| tempfile.unlink rescue nil }
    end
  end
end
