class HBase
module Admin
private
  def with_admin
    begin
      admin = HBaseAdmin.new(@config)
      yield admin
    ensure
      admin.close if admin
    end
  end
end#Admin
end#HBase

