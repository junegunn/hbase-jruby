class HBase
class Table
  # Returns a read-only org.apache.hadoop.hbase.HTableDescriptor object
  # @return [org.apache.hadoop.hbase.client.UnmodifyableHTableDescriptor]
  def descriptor
    htable.get_table_descriptor
  end

  # Returns table properties
  # @return [Hash]
  def properties
    desc = descriptor
    parse_raw_map(descriptor.values).tap { |props|
      TABLE_PROPERTIES.each do |prop, gs|
        get = gs[:get]
        if get && desc.respond_to?(get)
          props.delete(prop.to_s.upcase)
          props[prop] = parse_property desc.send get
        end
      end

      # deferred_log_flush is deprecated in 0.96
      if props.has_key?(:durability) && props.has_key?(:deferred_log_flush)
        props.delete :deferred_log_flush
      end
    }
  end

  # Returns raw String-to-String map of table properties
  # @return [Hash]
  def raw_properties
    parse_raw_map descriptor.values
  end

  # Returns properties of column families indexed by family name
  # @return [Hash]
  def families
    {}.tap { |ret|
      descriptor.families.each do |family|
        name = family.name_as_string
        ret[name] =
          parse_raw_map(family.values).tap { |props|
            COLUMN_PROPERTIES.each do |prop, gs|
              get = gs[:get]
              if get && family.respond_to?(get)
                props.delete(prop.to_s.upcase)
                props[prop] = parse_property family.send get
              end
            end
          }
      end
    }
  end

  # Returns raw String-to-String map of column family properties indexed by name
  # @return [Hash]
  def raw_families
    {}.tap { |ret|
      descriptor.families.each do |family|
        name = family.name_as_string
        ret[name] = parse_raw_map family.values
      end
    }
  end

  # Returns region information
  # @return [Hash]
  def regions
    with_admin do |admin|
      _regions admin
    end
  end

  # Returns a printable version of the table description
  # @return [String] Table description
  def inspect
    if exists?
      descriptor.toStringCustomizedValues
    else
      # FIXME
      "{NAME => '#{@name}'}"
    end
  end

private
  def _regions admin
    admin.getTableRegions(@name.to_java_bytes).map { |ri|
      {}.tap { |r|
        r[:name]      = ri.region_name
        r[:id]        = ri.region_id
        r[:start_key] = nil_if_empty ri.start_key
        r[:end_key]   = nil_if_empty ri.end_key
        if ri.respond_to?(:is_root_region)
          r[:root]      = ri.is_root_region
        end
        r[:meta]      = ri.is_meta_region
        r[:online]    = !ri.is_offline
      }
    }
  end

  def nil_if_empty v
    v.empty? ? nil : v
  end

  def parse_property v
    if v.is_a?(java.lang.Enum)
      v.to_s
    else
      v
    end
  end

  def parse_raw_map m
    Hash[
      m.keys.map { |e| Util.from_bytes :string, e.get }.zip(
        m.values.map { |e| Util.from_bytes :string, e.get }
      )
    ]
  end
end#Table
end#HBase
