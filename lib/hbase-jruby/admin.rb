require 'thread'

class HBase
module Admin
private
  def with_admin
    (@admin_mutex ||= Mutex.new).synchronize do
      begin
        admin = HBaseAdmin.new(@config)
        yield admin
      ensure
        admin.close if admin
      end
    end
  end

  def wait_async_admin admin
    while true
      pair  = admin.getAlterStatus(@name.to_java_bytes)
      yet   = pair.getFirst
      total = pair.getSecond

      break if yet == 0
      sleep 1
    end
  end
end#Admin
end#HBase

