# Oracle can be very moody when it comes to Spatial tables. There are some common
# problems you may encounter when creating indexes as follows.
#
# MULTIPLE ENTRIES IN SDO_INDEX_METADATA
#
#  If you receive the following error when creating a spatial index:
#
#   ERROR at line 1:
#   ORA-29855: error occurred in the execution of ODCIINDEXCREATE routine
#   ORA-13249: internal error in Spatial index: [mdidxrbd]
#   ORA-13249: Multiple entries in sdo_index_metadata table
#   ORA-06512: at "MDSYS.SDO_INDEX_METHOD_10I", line 10
#
#  Then you should execute the following queries as a SYS user:
#
#   select * from all_sdo_geom_metadata;           -- Should have entries for each of your spatial columns
#   select * from all_sdo_index_metadata;          -- Should have no entries referring to your user and table
#   select * from mdsys.sdo_index_metadata_table;  -- Should have no entries referring to your user, table, and index
#
#  If you have orphaned entries of index type: MDIDX_INIT
#  Then index creation failed for an internal reason, most likely a lack of available space.
#  Try granting your user unlimited space in USER and SYSTEM spaces.
#
#  Otherwise, try running this query as a SYS user:
#    delete from mdsys.sdo_index_metadata_table;
#
#  Then try deleteing all tables that start with MDRT_
#
# DATA CARTRIDGE ERROR / SEQUENCE DOESN'T EXIST
#
#  OCIError: ORA-29856: error occurred in the execution of ODCIINDEXDROP routine
#  ORA-13249: Error in Spatial index: cannot drop sequence ARES_TEST.MDRS_68EB0$
#  ORA-13249: Stmt-Execute Failure: DROP SEQUENCE ARES_TEST.MDRS_68EB0$
#  ORA-29400: data cartridge error
#  ORA-02289: sequence does not exist
#  ORA-06512: at "MDSYS.SDO_INDEX_METHOD_10I", line 27: DROP INDEX index_aow_geoms_on_geom
#
# If you solve other problems related to creating Oracle Spatial tables and indexes, please
# document them here.
#



require 'active_record/connection_adapters/abstract_adapter'
require 'active_record/connection_adapters/oracle_enhanced_connection'
require 'active_record/connection_adapters/oracle_enhanced_adapter'
require 'spatial_adapter/schema_definitions'

module ActiveRecord
  module ConnectionAdapters

    class SpatialOracleColumn < OracleEnhancedColumn
      include SpatialColumn
      def self.string_to_geometry(string)
        return string unless string.is_a?(String)
        GeoRuby::SimpleFeatures::Geometry.from_hex_ewkb(string) rescue nil
      end
      def self.create_simplified(name,default,null = true)
        new(name,default,"geometry",null,nil,nil,nil)
      end
    end

    class SpatialOracleEnhancedColumn < SpatialOracleColumn; end

    module SpatialTableDefinition
      attr_accessor :column_comments, :create_sequence

      def geometry(*args)                                             # This method allows you to
        options = args.extract_options!                               # create_table :spatial_datas do |t|
        column_names = args                                           #   t.geometry :geom
        column_names.each { |name| column(name, :geometry, options) } # end
      end                                                             #

      def spatial_columns
        columns.select { |c| @base.geometry_data_types.include?(c.type.to_sym) }
      end

      def primary_key(*args)
        self.create_sequence = true
        super(*args)
      end

      def column(name, type, options = {})
        if options[:comment]
          self.column_comments ||= {}
          self.column_comments[name] = options[:comment]
        end
        super(name, type, options)
      end
    end
  end
end

module OracleSpatialAdapter
  include SpatialAdapter
  include ActiveRecord::ConnectionAdapters

  @@do_not_prefetch_primary_key ||= {} # Because OracleEnhancedAdapter requires it. Don't hate.

  # TODO: Add :point, :line_string, :polygon, :geometry_collection, :multi_point, :multi_line_string, and :multi_polygon
  def geometry_data_types
    { :geometry => { :name => "SDO_GEOMETRY"} }
  end

  def native_database_types_with_spatial
    native_database_types_without_spatial.merge( geometry_data_types )
  end

  def columns_without_cache_with_spatial(table_name, name = nil) #:nodoc:
    ignored_columns = ignored_table_columns(table_name)
    (owner, desc_table_name, db_link) = @connection.describe(table_name)

    @@do_not_prefetch_primary_key[table_name] =  has_primary_key_trigger?(table_name, owner, desc_table_name, db_link)

    table_cols_sql  = get_table_cols_sql(db_link,owner,desc_table_name)
    table_cols_meta = select_all(table_cols_sql, name)
    table_cols_meta = table_cols_meta.delete_if { |row| ignored_columns && ignored_columns.include?(row['name'].downcase) }
    metadata_to_columns(table_cols_meta, desc_table_name)
  end

  def add_index_with_spatial(table_name,column_name,options = {})
    return add_index_without_spatial(table_name, column_name, options) unless options[:spatial]
    unless spatially_indexed_on?(table_name, column_name)
      sql = "CREATE INDEX #{index_name(table_name, column_name)} ON #{table_name}(#{Array(column_name).join(", ")}) INDEXTYPE IS MDSYS.SPATIAL_INDEX"
      begin
        execute sql
      rescue Exception => e
        self.error("Failed to create spatial index: #{e.message}")
        if indexes(table_name).select { |i| i.spatial }.select { |si| si.columns.include?(column_name) }.empty?
          self.error("Cleaning up botched spatial index...")
          execute drop_index_sql(table_name, column_name) rescue nil
          execute delete_sdo_index_metadata_sql(table_name, column_name) rescue nil
          self.error("To diagnose the issue, try running the following command in SQLPlus:")
          self.error(sql)
        end
      end
    end
  end

  def create_table_with_spatial(name, options = {})
    table_definition = ActiveRecord::ConnectionAdapters::TableDefinition.new(self)
    table_definition.primary_key(options[:primary_key] || 'id') unless options[:id] == false

    yield table_definition if block_given?

    drop_table(name) rescue nil if options[:force]
    execute create_table_sql(name, table_definition, :temporary =>  options[:temporary], :options => options[:options])
    create_sequence_and_trigger(name, options) if options[:id] != false || table_definition.create_sequence
    add_table_comment name, options[:comment]
    ensure_spatial_column_metadata_exists(name, table_definition)
    add_column_comments(table_definition)
  end

  def indexes_with_spatial(table_name, name = nil)
    result = select_all(index_sql(table_name)).uniq
    return index_list_to_definitions(result)
  end

  def spatially_indexed_on?(table_name, columns)
    spatials = indexes(table_name).select { |i| i.spatial }
    columns.inject(false) do |is_indexed, column_name|
      is_indexed || !spatials.select { |si| si.columns.include?(column_name.to_s) }.empty?
    end
  end

  def is_spatial_column?(table_name, column_name, sql_type = nil)
    geometry_data_types.values.map { |t| t[:name] }.include?(sql_type) || column_spatial_info(table_name)[column_name].not_blank?
  end

  def column_spatial_info(table_name)
    meta = execute "SELECT * FROM USER_SDO_GEOM_METADATA WHERE table_name = '#{table_name.to_s.upcase}'"
    raw_geom_infos = {}
    meta.fetch { |sdo| raw_geom_infos[sdo[1]] = RawGeomInfo.new('SDO_GEOMETRY',sdo[3], sdo[2]) }      # RawGeomInfo arguments: Struct.new(:type,:srid,:dimension,:with_z,:with_m)
    raw_geom_infos.each_value { |v| v.convert! }
  end

  def delete_sdo_index_metadata(table_name, column_name)
    execute delete_sdo_index_metadata_sql(table_name, column_name)
  end

  def delete_sdo_index_metadata_sql(table_name, column_name)
    "DELETE FROM user_sdo_index_metadata where SDO_INDEX_NAME='#{index_name(table_name, column_name)}'"
  end

  def drop_index_sql(table_name, column_name)
    "DROP INDEX #{index_name(table_name, column_name)}"
  end

  def metadata_to_columns(table_cols_meta, table_name)
    table_cols_meta.map do |row|
      name                                  = oracle_downcase(row['name'])
      limit, scale, sql_type, data_default  = row['limit'], row['scale'], row['sql_type'], row['data_default']
      nullable                              = row['nullable'] == 'Y'

      sql_type << "(#{(limit || 38).to_i}" + ((scale = scale.to_i) > 0 ? ",#{scale})" : ")") if limit || scale

      clean_up_odd_default_spacing!(data_default)

      klass = is_spatial_column?(table_name, name, sql_type) ? SpatialOracleEnhancedColumn : OracleEnhancedColumn
      klass.new(name, data_default, sql_type, nullable, table_name, get_type_for_column(table_name, name))
    end
  end

  def clean_up_odd_default_spacing!(string)
    return unless string
    string.sub!(/^(.*?)\s*$/, '\1')
    string.sub!(/^'(.*)'$/, '\1')
    string.delete!(string) if string =~ /^(null|empty_[bc]lob\(\))$/i
  end

  def get_table_cols_sql(db_link,owner, table_name)
   str = <<-SQL
      select column_name as name, data_type as sql_type, data_default, nullable,
             decode(data_type, 'NUMBER', data_precision,
                               'FLOAT', data_precision,
                               'VARCHAR2', decode(char_used, 'C', char_length, data_length),
                               'CHAR', decode(char_used, 'C', char_length, data_length),
                                null) as limit,
             decode(data_type, 'NUMBER', data_scale, null) as scale
        from all_tab_columns#{db_link}
       where owner      = '#{owner}'
         and table_name = '#{table_name}'
       order by column_id
    SQL
  end

  def add_column_comments(table_definition)
    column_comments = table_definition.column_comments
    column_comments ||= {}
    column_comments.each { |column_name, comment| add_comment name, column_name, comment }
  end

  def ensure_spatial_column_metadata_exists(table_name, table_definition)
    existing_metadata = column_spatial_info(table_name)
    table_definition.spatial_columns.each do |column|
      execute sdo_metadata_sql(table_name, column.name) unless existing_metadata[column.name.to_s.upcase]
    end
  end

  def drop_table_with_spatial(table_name, options = {})
    column_spatial_info(table_name).keys do |spatial_column|
      execute delete_sdo_metadata_sql(table_name, spatial_column)
    end
    drop_table_without_spatial(table_name, options)
  end

  # TODO: Support user-defined dimension limits and tolerance in the migrations
  def sdo_metadata_sql(table_name, column_name, srid = "NULL", *dimensions)
    sql = <<-SQL
      INSERT INTO USER_SDO_GEOM_METADATA (TABLE_NAME, COLUMN_NAME, DIMINFO, SRID)
      VALUES ('#{table_name.to_s.upcase}', '#{column_name.to_s.upcase}',
          MDSYS.SDO_DIM_ARRAY(
              MDSYS.SDO_DIM_ELEMENT('X', -179.999783489, 180.000258016, 0.000000050),
              MDSYS.SDO_DIM_ELEMENT('Y', -89.999828389, 83.633810934, 0.000000050)
          ),
          NULL)
    SQL
  end

  def delete_sdo_metadata_sql(table_name, column_name)
    sql = <<-SQL
      DELETE FROM USER_SDO_GEOM_METADATA WHERE table_name = '#{table_name.to_s.upcase}' AND column_name = '#{column_name.to_s.upcase}'
    SQL
  end

  def create_table_sql(name, table_definition, options)
    sql = <<-SQL
      CREATE #{'TEMPORARY' if options[:temporary]} TABLE #{name} (
         #{table_definition.to_sql}
      ) #{options[:options]}
    SQL
  end

  def index_list_to_definitions(result)
    current_index = nil
    indexes = []
    result.each do |row|
      if current_index != row['index_name']
        indexes << IndexDefinition.new(row['table_name'], row['index_name'], row_describes_unique_index?(row), [], row_describes_spatial_index?(row))
        current_index = row['index_name']
      end
      indexes.last.columns << (row['column_expression'].nil? ? row['column_name'] : row['column_expression'].gsub('"','').downcase)
    end
    indexes
  end

  def row_tablespace_name(index_row)
    index_row['tablespace_name'] == default_tablespace ? nil : index_row['tablespace_name']
  end

  def row_describes_unique_index?(index_row)
    index_row['uniqueness'] == "UNIQUE"
  end

  def row_describes_spatial_index?(index_row)
    index_row['ityp_name'] == 'SPATIAL_INDEX' 
  end

  def index_sql(table_name)
    (owner, table_name, db_link) = @connection.describe(table_name)
    <<-SQL
        SELECT
          lower(i.table_name) as table_name,
          lower(i.index_name) as index_name,
          i.uniqueness,
          lower(i.tablespace_name) as tablespace_name,
          lower(c.column_name) as column_name,
          e.column_expression as column_expression,
          lower(i.index_type) as index_type,
          i.ityp_name as ityp_name
        FROM user_indexes#{db_link} i
        JOIN user_ind_columns#{db_link} c on c.index_name = i.index_name
        LEFT OUTER JOIN user_ind_expressions#{db_link} e on e.index_name = i.index_name and e.column_position = c.column_position
        LEFT OUTER JOIN user_sdo_geom_metadata sdo on sdo.column_name = C.COLUMN_NAME
        WHERE lower(i.table_name) = '#{table_name.to_s.downcase}'
        AND NOT EXISTS (SELECT uc.index_name FROM user_constraints uc WHERE uc.index_name = i.index_name AND uc.constraint_type = 'P')
        ORDER BY i.index_name, c.column_position
      SQL
  end

  def error(message)
    RAILS_DEFAULT_LOGGER.error(message)
    puts "ERROR! #{message}"
  end
end

ActiveRecord::SchemaDumper.class_eval do

  def indexes(table, stream)
    indexes = @connection.indexes(table)
    indexes.each do |index|
      stream.print "  add_index #{index.table.inspect}, #{index.columns.inspect}, :name => #{index.name.inspect}"
      stream.print ", :unique => true" if index.unique
      stream.print ", :spatial => true " if index.spatial
      stream.puts
    end

    stream.puts unless indexes.empty?
  end
end

module ActiveRecord
  module ConnectionAdapters

    OracleEnhancedAdapter.class_eval do
      include OracleSpatialAdapter
      alias_method_chain :native_database_types, :spatial
      alias_method_chain :columns_without_cache, :spatial
      alias_method_chain :indexes, :spatial
      alias_method_chain :create_table, :spatial
      alias_method_chain :add_index, :spatial
    end

    TableDefinition.class_eval { include SpatialTableDefinition }

  end
end
