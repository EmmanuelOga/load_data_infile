require 'erb'

module LoadDataInfile
  module MySql

    # Deletes all rows in table very fast, but without calling +destroy+ method
    # nor any hooks.
    def truncate_table(table = quoted_table_name)
      connection.execute("TRUNCATE TABLE #{table}")
    end

    # Disables key updates for model table
    def disable_keys(table = quoted_table_name)
      connection.execute("ALTER TABLE #{table} DISABLE KEYS")
    end

    # Enables key updates for model table
    def enable_keys(table = quoted_table_name)
      connection.execute("ALTER TABLE #{table} ENABLE KEYS")
    end

    # Disables keys, yields block, enables keys.
    def with_keys_disabled(table = quoted_table_name)
      disable_keys(table)
      yield
    ensure
      enable_keys(table)
    end

    # Load csv from a file using MySql's LOAD DATA INFILE
    # For details see: http://dev.mysql.com/doc/refman/5.1/en/load-data.html
    #
    # Options:
    #
    # path                   :: CSV file path
    #
    # charset                :: [OPTIONAL] Charset
    # columns                :: [OPTIONAL] Array of columns. Tries to use all columns if not provided. Use @dummy as column name to ignore a column. E.G.: (column_a, @column_b, @dummy)
    # concurrent             :: [OPTIONAL] True or false
    # enclosed_by            :: [OPTIONAL] Character
    # escaped_by             :: [OPTIONAL] Character
    # ignore                 :: [OPTIONAL] Number, If provided, skips that number of lines.
    # lines_starting_by      :: [OPTIONAL] Character
    # lines_terminated_by    :: [OPTIONAL] Character
    # local                  :: [OPTIONAL] true or fase. Defaults to true
    # low_priority           :: [OPTIONAL] true or false
    # mappings               :: [OPTIONAL] An array to map column values according to the mysql manual. E.G.: { :column_a => "TRIM(@column_b)"}
    # on_duplicates          :: [OPTIONAL] Action to perform when a duplicate row is found. Can be IGNORE or REPLACE
    # optionally_enclosed_by :: [OPTIONAL] Character
    # table                  :: [OPTIONAL] Table name. Defaults to quoted_table_name (won't work if used from an abstract class, e.g. ActiveRecord::Base')
    # terminated_by          :: [OPTIONAL] Character
    def load_data_infile(options = {})
      c = Context.new

      if options[:low_priority]
        c.low_priority_or_concurrent = :LOW_PRIORITY
      elsif options[:concurrent]
        c.low_priority_or_concurrent = :CONCURRENT
      end

      c.local = :LOCAL if !options.member?(:local) || options[:local]

      c.file_name = quote_value options[:path]

      c.replace_or_ignore = options[:on_duplicates] if options[:on_duplicates] # REPLACE or IGNORE

      c.table_name = options[:table] ? "`#{ options[:table] }`" : quoted_table_name

      c.charset = "CHARACTER SET #{quote_value options[:charset]}" if options[:charset]

      if options[:terminated_by] || options[:enclosed_by] || options[:optionally_enclosed_by] || options[:escaped_by]
        c.fields_definitions = " FIELDS " # or COLUMNS
        c.fields_definitions << " TERMINATED          BY #{ quote_value options[:terminated_by] } "          if options[:terminated_by]
        c.fields_definitions << " ENCLOSED            BY #{ quote_value options[:enclosed_by] } "            if options[:enclosed_by]
        c.fields_definitions << " OPTIONALLY ENCLOSED BY #{ quote_value options[:optionally_enclosed_by] } " if options[:optionally_enclosed_by]
        c.fields_definitions << " ESCAPED             BY #{ quote_value options[:escaped_by] } "             if options[:escaped_by]
      end

      if options[:lines_terminated_by] || options[:lines_starting_by]
        c.lines_defitions = " LINES "
        c.lines_defitions << " STARTING BY   #{quote_value options[:lines_starting_by]} "   if options[:lines_starting_by]
        c.lines_defitions << " TERMINATED BY #{quote_value options[:lines_terminated_by]} " if options[:lines_terminated_by]
      end

      c.ignores = "IGNORE #{options[:ignore]} LINES" if options[:ignore]

      c.columns = " (#{options[:columns].join(", ")}) " if options[:columns]

      if options[:mappings] && options[:mappings].length > 0
        s = options[:mappings].map{|column, mapping| "#{column} = #{mapping}" }.join(",")
        c.mappings = "SET #{s}"
      end

      connection.execute(ERB.new(LOAD_DATA_INFILE_SQL).result(c.binding).gsub(/^\s*\n/, ""))
    end

    class Context < OpenStruct
      public :binding
    end

    LOAD_DATA_INFILE_SQL = <<-SQL
      LOAD DATA <%= low_priority_or_concurrent %> <%= local %> INFILE <%= file_name %>
          <%= replace_or_ignore %>
          INTO TABLE <%= table_name %>
          <%= charset %>
          <%= fields_definitions %>
          <%= lines_defitions %>
          <%= ignores %>
          <%= columns %>
          <%= mappings %> ;
    SQL
  end
end
