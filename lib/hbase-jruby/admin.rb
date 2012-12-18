class HBase
# @private
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

  def wait_async_admin admin, &block
    prev_yet = nil
    while true
      pair  = admin.getAlterStatus(@name.to_java_bytes)
      yet   = pair.getFirst
      total = pair.getSecond

      if block && yet != prev_yet
        block.call (total - yet), total
        prev_yet = yet
      end

      break if yet == 0
      sleep 1
    end
  end
end#Admin
end#HBase

