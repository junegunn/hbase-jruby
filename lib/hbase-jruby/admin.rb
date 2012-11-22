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
end#Admin
end#HBase

