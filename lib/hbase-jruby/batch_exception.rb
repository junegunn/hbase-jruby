class HBase
class BatchException < RuntimeError
  attr_reader :java_exception, :results

  def initialize x, results
    super x.to_s
    @java_exception = x
    @results = results
  end
end
end

