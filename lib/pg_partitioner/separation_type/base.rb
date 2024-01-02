module PgPartitioner
  module SeparationType
    module Base
      def drop_table(table_name)
        sql = "DROP TABLE IF EXISTS #{table_name};"
        execute_sql(sql)
      end

      def drop_partitioning_trigger_sql
        sql = "DROP TRIGGER #{table_name}_insert ON #{table_name};
         DROP FUNCTION #{table_name}_insert_trigger();
         DROP TRIGGER #{table_name}_after_insert ON #{table_name};
         DROP FUNCTION #{table_name}_delete_trigger();"

        execute_sql(sql)
      end

      def create_partition_indexes(partition_table_name)
        custom_indexes = partition_table_indexes.presence
        return unless custom_indexes

        custom_indexes.each { |custom_index| create_custom_index(partition_table_name, custom_index) }
      end

      def create_partition_named_indexes(partition_table_name)
        custom_indexes = partition_table_named_indexes.presence
        return unless custom_indexes

        custom_indexes.map do |name, custom_index|
          index_name = "index_#{partition_table_name}_#{name}"
          create_custom_named_index(partition_table_name, custom_index, index_name)
        end
      end

      def create_partition_unique_indexes(partition_table_name)
        custom_unique_indexes = partition_table_unique_indexes.presence
        return unless custom_unique_indexes

        custom_unique_indexes.each { |custom_index| create_custom_index(partition_table_name, custom_index, true) }
      end

      def create_partition_named_unique_indexes(partition_table_name)
        custom_indexes = partition_table_named_unique_indexes.presence
        return unless custom_indexes

        custom_indexes.map do |name, custom_index|
          index_name = "index_#{partition_table_name}_#{name}"
          create_custom_named_index(partition_table_name, custom_index, index_name, true)
        end
      end

      def name_of_partition_table(date = Date.today, type:)
        case type
        when :month
          date.strftime("#{table_name}_y%Ym%m")
        when :quater
          "#{table_name}_y#{date.year}q#{(((date.month - 1) / 3) + 1).to_i}"
        when :week
          if date.cweek < 10
            "#{table_name}_y#{date.year}w0#{date.cweek}"
          else
            "#{table_name}_y#{date.year}w#{date.cweek}"
          end
        end
      end
    end
  end
end
