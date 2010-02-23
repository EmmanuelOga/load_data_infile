require 'erb'

module LoadDataInfile
  module MySql

    # Deletes all rows in table very fast, but without calling +destroy+ method
    # nor any hooks.
    def truncate_table
      connection.execute("TRUNCATE TABLE #{quoted_table_name}")
    end

    # Disables key updates for model table
    def disable_keys
      connection.execute("ALTER TABLE #{quoted_table_name} DISABLE KEYS")
    end

    # Enables key updates for model table
    def enable_keys
      connection.execute("ALTER TABLE #{quoted_table_name} ENABLE KEYS")
    end

    # Disables keys, yields block, enables keys.
    def with_keys_disabled
      disable_keys
      yield
      enable_keys
    end

    def load_data_infile(options = {})
      c = Context.new

      if options[:low_priority]
        c.low_priority_or_concurrent = :LOW_PRIORITY
      elsif options[:concurrent]
        c.low_priority_or_concurrent = :CONCURRENT
      end

      c.local = :LOCAL if !options.member?(:local) || options[:local]

      c.file_name = options[:path]

      c.replace_or_ignore = options[:on_duplicates] if options[:on_duplicates] # REPLACE or IGNORE

      c.table_name = options[:table] ? "`#{ options[:table] }`" : quoted_table_name

      c.charset = "CHARACTER SET #{options[:charset]}" if options[:charset]

      if options[:terminated_by] || options[:enclosed_by] || options[:optionally_enclosed_by] || options[:escaped_by]
        c.fields_definitions = " FIELDS " # or COLUMNS
        c.fields_definitions << " TERMINATED          BY '#{ options[:terminated_by] }' "          if options[:terminated_by]
        c.fields_definitions << " ENCLOSED            BY '#{ options[:enclosed_by] }' "            if options[:enclosed_by]
        c.fields_definitions << " OPTIONALLY ENCLOSED BY '#{ options[:optionally_enclosed_by] }' " if options[:optionally_enclosed_by]
        c.fields_definitions << " ESCAPED             BY '#{ options[:escaped_by] }' "             if options[:escaped_by]
      end

      if options[:lines_terminated_by] || options[:lines_starting_by]
        c.lines_defitions = " LINES "
        c.lines_defitions << " STARTING BY '#{options[:lines_starting_by]}' "     if options[:lines_starting_by]
        c.lines_defitions << " TERMINATED BY '#{options[:lines_terminated_by]}' " if options[:lines_terminated_by]
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
      LOAD DATA <%= low_priority_or_concurrent %> <%= local %> INFILE '<%= file_name %>'
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
