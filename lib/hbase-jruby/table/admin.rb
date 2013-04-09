class HBase
class Table
  # Checks if the table of the name exists
  # @return [true, false] Whether table exists
  def exists?
    with_admin { |admin| admin.tableExists @name }
  end

  # Checks if the table is enabled
  # @return [true, false] Whether table is enabled
  def enabled?
    with_admin { |admin| admin.isTableEnabled(@name) }
  end

  # Checks if the table is disabled
  # @return [true, false] Whether table is disabled
  def disabled?
    !enabled?
  end

  # Creates the table
  # @overload create!(column_family_name, props = {})
  #   Create the table with one column family of the given name
  #   @param [#to_s] The name of the column family
  #   @param [Hash] props Table properties
  #   @return [nil]
  # @overload create!(column_family_hash, props = {})
  #   Create the table with the specified column families
  #   @param [Hash] Column family properties
  #   @param [Hash] props Table properties
  #   @return [nil]
  #   @example
  #     table.create!(
  #       # Column family with default options
  #       :cf1 => {},
  #       # Another column family with custom properties
  #       :cf2 => {
  #         :blockcache         => true,
  #         :blocksize          => 128 * 1024,
  #         :bloomfilter        => :row,
  #         :compression        => :snappy,
  #         :in_memory          => true,
  #         :keep_deleted_cells => true,
  #         :min_versions       => 2,
  #         :replication_scope  => 0,
  #         :ttl                => 100,
  #         :versions           => 5
  #       }
  #     )
  # @overload create!(table_descriptor)
  #   Create the table with the given HTableDescriptor
  #   @param [org.apache.hadoop.hbase.HTableDescriptor] Table descriptor
  #   @return [nil]
  def create! desc, props = {}
    splits =
      if props[:splits]
        raise ArgumentError, ":splits property must be an Array" if !props[:splits].is_a?(Array)
        props[:splits].map { |e| Util.to_bytes(e).to_a }.to_java(Java::byte[])
      end

    todo = nil
    with_admin do |admin|
      raise RuntimeError, 'Table already exists' if admin.tableExists(@name)

      case desc
      when HTableDescriptor
        patch_table_descriptor! desc, props
        admin.createTable(*[desc, splits].compact)
      when Symbol, String
        todo = lambda { create!({desc => {}}, props) }
      when Hash
        htd = HTableDescriptor.new(@name.to_java_bytes)
        patch_table_descriptor! htd, props
        desc.each do |name, opts|
          htd.addFamily hcd(name, opts)
        end

        admin.createTable(*[htd, splits].compact)
      else
        raise ArgumentError, 'Invalid table description'
      end
    end
    todo.call if todo # Avoids mutex relocking
  end

  # Alters the table (synchronous)
  # @param [Hash] props Table properties
  # @return [nil]
  # @yield [progress, total]
  # @yieldparam [Fixnum] progress Number of regions updated
  # @yieldparam [Fixnum] total Total number of regions
  # @example
  #   table.alter!(
  #     :max_filesize       => 512 * 1024 ** 2,
  #     :memstore_flushsize =>  64 * 1024 ** 2,
  #     :readonly           => false,
  #     :deferred_log_flush => true
  #   )
  def alter! props, &block
    _alter props, true, &block
  end

  # Alters the table (asynchronous)
  # @see HBase::Table#alter!
  def alter props
    _alter props, false
  end

  # Adds the column family (synchronous)
  # @param [#to_s] name The name of the column family
  # @param [Hash] opts Column family properties
  # @return [nil]
  # @yield [progress, total]
  # @yieldparam [Fixnum] progress Number of regions updated
  # @yieldparam [Fixnum] total Total number of regions
  def add_family! name, opts, &block
    _add_family name, opts, true, &block
  end

  # Adds the column family (asynchronous)
  # @see HBase::Table#add_family!
  def add_family name, opts
    _add_family name, opts, false
  end

  # Alters the column family
  # @param [#to_s] name The name of the column family
  # @param [Hash] opts Column family properties
  # @return [nil]
  # @yield [progress, total]
  # @yieldparam [Fixnum] progress Number of regions updated
  # @yieldparam [Fixnum] total Total number of regions
  def alter_family! name, opts, &block
    _alter_family name, opts, true, &block
  end

  # Alters the column family (asynchronous)
  # @see HBase::Table#alter_family!
  def alter_family name, opts
    _alter_family name, opts, false
  end

  # Removes the column family
  # @param [#to_s] name The name of the column family
  # @return [nil]
  # @yield [progress, total]
  # @yieldparam [Fixnum] progress Number of regions updated
  # @yieldparam [Fixnum] total Total number of regions
  def delete_family! name, &block
    _delete_family name, true, &block
  end

  # Removes the column family (asynchronous)
  # @see HBase::Table#delete_family!
  def delete_family name
    _delete_family name, false
  end

  # Adds the table coprocessor to the table
  # @param [String] class_name Full class name of the coprocessor
  # @param [Hash] props Coprocessor properties
  # @option props [String] path The path of the JAR file
  # @option props [Fixnum] priority Coprocessor priority
  # @option props [Hash<#to_s, #to_s>] params Arbitrary key-value parameter pairs passed into the coprocessor
  # @yield [progress, total]
  # @yieldparam [Fixnum] progress Number of regions updated
  # @yieldparam [Fixnum] total Total number of regions
  def add_coprocessor! class_name, props = {}, &block
    _add_coprocessor class_name, props, true, &block
  end

  # Adds the table coprocessor to the table (asynchronous)
  def add_coprocessor class_name, props = {}
    _add_coprocessor class_name, props, false
  end

  # Removes the coprocessor from the table.
  # @param [String] class_name Full class name of the coprocessor
  # @return [nil]
  # @yield [progress, total]
  # @yieldparam [Fixnum] progress Number of regions updated
  # @yieldparam [Fixnum] total Total number of regions
  def remove_coprocessor! class_name, &block
    _remove_coprocessor class_name, true, &block
  end

  # Removes the coprocessor from the table (asynchronous)
  # @see HBase::Table#remove_coprocessor!
  def remove_coprocessor class_name
    _remove_coprocessor class_name, false
  end

  # Return if the table has the coprocessor of the given class name
  # @param [String] class_name Full class name of the coprocessor
  # @return [true, false]
  def has_coprocessor? class_name
    descriptor.hasCoprocessor(class_name)
  end

  # Splits the table region on the given split point (asynchronous)
  # @param [*Object] split_keys
  # @return [nil]
  def split! *split_keys
    _split split_keys, false
  end

  # Enables the table
  # @return [nil]
  def enable!
    with_admin do |admin|
      admin.enableTable @name unless admin.isTableEnabled(@name)
    end
  end

  # Disables the table
  # @return [nil]
  def disable!
    with_admin do |admin|
      admin.disableTable @name if admin.isTableEnabled(@name)
    end
  end

  # Truncates the table by dropping it and recreating it.
  # @return [nil]
  def truncate!
    htd = htable.get_table_descriptor
    drop!
    create! htd
  end

  # Drops the table
  # @return [nil]
  def drop!
    with_admin do |admin|
      raise RuntimeError, 'Table does not exist' unless admin.tableExists @name

      admin.disableTable @name if admin.isTableEnabled(@name)
      admin.deleteTable  @name
      close
    end
  end

private
  COLUMN_PROPERTIES = {
    :blockcache            => { :set => :setBlockCacheEnabled,         :get => :isBlockCacheEnabled },
    :blocksize             => { :set => :setBlocksize,                 :get => :getBlocksize },
    :bloomfilter           => { :set => :setBloomFilterType,           :get => :getBloomFilterType },
    :cache_blooms_on_write => { :set => :setCacheBloomsOnWrite,        :get => :shouldCacheBloomsOnWrite },
    :cache_data_on_write   => { :set => :setCacheDataOnWrite,          :get => :shouldCacheDataOnWrite },
    :cache_index_on_write  => { :set => :setCacheIndexesOnWrite,       :get => :shouldCacheIndexesOnWrite },
    :compression           => { :set => :setCompressionType,           :get => :getCompressionType },
    :compression_compact   => { :set => :setCompactionCompressionType, :get => :getCompactionCompression },
    :data_block_encoding   => { :set => :setDataBlockEncoding,         :get => :getDataBlockEncoding },
    :encode_on_disk        => { :set => :setEncodeOnDisk,              :get => nil },
    :evict_blocks_on_close => { :set => :setEvictBlocksOnClose,        :get => :shouldEvictBlocksOnClose },
    :in_memory             => { :set => :setInMemory,                  :get => :isInMemory },
    :keep_deleted_cells    => { :set => :setKeepDeletedCells,          :get => :getKeepDeletedCells },
    :min_versions          => { :set => :setMinVersions,               :get => :getMinVersions },
    :replication_scope     => { :set => :setScope,                     :get => :getScope },
    :ttl                   => { :set => :setTimeToLive,                :get => :getTimeToLive },
    :versions              => { :set => :setMaxVersions,               :get => :getMaxVersions },
  }

  TABLE_PROPERTIES = {
    :max_filesize       => { :get => :getMaxFileSize,       :set => :setMaxFileSize },
    :readonly           => { :get => :isReadOnly,           :set => :setReadOnly },
    :memstore_flushsize => { :get => :getMemStoreFlushSize, :set => :setMemStoreFlushSize },
    :deferred_log_flush => { :get => :isDeferredLogFlush,   :set => :setDeferredLogFlush },
  }

  MAX_SPLIT_WAIT = 30

  def while_disabled admin
    begin
      admin.disableTable @name if admin.isTableEnabled(@name)
      yield
    ensure
      admin.enableTable @name
    end
  end

  def hcd name, opts
    HColumnDescriptor.new(name.to_s).tap do |hcd|
      opts.each do |key, val|
        method = COLUMN_PROPERTIES[key] && COLUMN_PROPERTIES[key][:set]
        if method
          hcd.send method,
            ({
              :bloomfilter => proc { |v|
                enum =
                  if defined?(org.apache.hadoop.hbase.regionserver.StoreFile::BloomType)
                    org.apache.hadoop.hbase.regionserver.StoreFile::BloomType
                  else
                    # 0.95 or later
                    org.apache.hadoop.hbase.regionserver.BloomType
                  end
                const_shortcut enum, v, "Invalid bloom filter type"
              },
              :compression => proc { |v|
                const_shortcut Compression::Algorithm, v, "Invalid compression algorithm"
              },
              :compression_compact => proc { |v|
                const_shortcut Compression::Algorithm, v, "Invalid compression algorithm"
              },
              :data_block_encoding => proc { |v|
                const_shortcut org.apache.hadoop.hbase.io.encoding.DataBlockEncoding, v, "Invalid data block encoding algorithm"
              }
            }[key] || proc { |a| a }).call(val)
        elsif key.is_a?(String)
          hcd.setValue key, val.to_s
        else
          raise ArgumentError, "Invalid property: #{key}"
        end
      end#opts
    end
  end

  def const_shortcut base, v, message
    # Match by constant value
    # - const_get doesn't work with symbols in 1.8 compatibility mode
    if base.constants.map { |c| base.const_get c }.any? { |cv| v == cv }
      v
    # Match by constant name (uppercase)
    elsif (e = base.valueOf(vs = v.to_s.upcase) rescue nil)
      e
    else
      raise ArgumentError, [message, v.to_s].join(': ')
    end
  end

  def patch_table_descriptor! htd, props
    props.each do |key, value|
      next if key == :splits

      if method = TABLE_PROPERTIES[key] && TABLE_PROPERTIES[key][:set]
        htd.send method, value
      elsif key.is_a?(String)
        htd.setValue key, value.to_s
      else
        raise ArgumentError, "Invalid table property: #{key}" unless method
      end
    end
    htd
  end

  def _alter props, bang, &block
    raise ArgumentError, ":split not supported" if props[:splits]
    with_admin do |admin|
      htd = admin.get_table_descriptor(@name.to_java_bytes)
      patch_table_descriptor! htd, props
      while_disabled(admin) do
        admin.modifyTable @name.to_java_bytes, htd
        wait_async_admin(admin, &block) if bang
      end
    end
  end

  def _add_family name, opts, bang, &block
    with_admin do |admin|
      while_disabled(admin) do
        admin.addColumn @name, hcd(name.to_s, opts)
        wait_async_admin(admin, &block) if bang
      end
    end
  end

  def _alter_family name, opts, bang, &block
    with_admin do |admin|
      while_disabled(admin) do
        admin.modifyColumn @name, hcd(name.to_s, opts)
        wait_async_admin(admin, &block) if bang
      end
    end
  end

  def _delete_family name, bang, &block
    with_admin do |admin|
      while_disabled(admin) do
        admin.deleteColumn @name, name.to_s
        wait_async_admin(admin, &block) if bang
      end
    end
  end

  def _add_coprocessor class_name, props, bang, &block
    with_admin do |admin|
      while_disabled(admin) do

        htd = admin.get_table_descriptor(@name.to_java_bytes)
        if props.empty?
          htd.addCoprocessor class_name
        else
          path, priority, params = props.values_at :path, :priority, :params
          params = Hash[ params.map { |k, v| [k.to_s, v.to_s] } ]
          htd.addCoprocessor class_name, path, priority || Coprocessor::PRIORITY_USER, params
        end
        admin.modifyTable @name.to_java_bytes, htd
        wait_async_admin(admin, &block) if bang
      end
    end
  end

  def _remove_coprocessor name, bang, &block
    unless HTableDescriptor.respond_to?(:removeCoprocessor)
      raise NotImplementedError, "org.apache.hadoop.hbase.HTableDescriptor.removeCoprocessor not implemented"
    end
    with_admin do |admin|
      while_disabled(admin) do
        htd = admin.get_table_descriptor(@name.to_java_bytes)
        htd.removeCoprocessor name
        admin.modifyTable @name.to_java_bytes, htd
        wait_async_admin(admin, &block) if bang
      end
    end
  end

  def _split split_keys, bang, &block
    with_admin do |admin|
      split_keys.each do |sk|
        wait_until_all_regions_online admin
        admin.split(@name.to_java_bytes, Util.to_bytes(sk))

        if bang
          wait_async_admin(admin, &block)
          wait_until_all_regions_online admin
        end
      end
    end
  end

  def wait_until_all_regions_online admin
    # FIXME: progress reporting
    cnt = 0
    while !_regions(admin).map { |r| r[:online] }.all? { |e| e }
      raise RuntimeError, "Not all regions are online" if cnt >= MAX_SPLIT_WAIT
      cnt += 1
      sleep 1
    end
  end
end#Table
end#HBase

