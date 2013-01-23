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
    {}.tap { |props|
      desc = descriptor
      TABLE_PROPERTIES.each do |prop, gs|
        get = gs[:get]
        if get && desc.respond_to?(get)
          props[prop] = parse_property desc.send get
        end
      end
    }
  end

  # Returns properties of column families
  # @return [Hash]
  def families
    {}.tap { |ret|
      descriptor.families.each do |family|
        name = family.name_as_string
        ret[name] = {}.tap { |props|
          COLUMN_PROPERTIES.each do |prop, gs|
            get = gs[:get]
            if get && family.respond_to?(get)
              props[prop] = parse_property family.send get
            end
          end
        }
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
      properties.to_s
    else
      {}.to_s
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
        r[:root]      = ri.is_root_region
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
end#Table
end#HBase
