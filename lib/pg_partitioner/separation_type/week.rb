module PgPartitioner
  module SeparationType
    module Week
      def create_current_week_table
        create_week_table(Date.today)
      end

      def create_next_week_table
        create_week_table(Date.today.next_week)
      end

      def drop_old_week_table
        table_name = name_of_partition_table(Date.today.prev_week.prev_week,
                                             type: :week)
        drop_table(table_name)
      end

      def create_week_table(date = Date.today)
        date_start = date.at_beginning_of_week
        date_end = date.at_beginning_of_week.next_week
        partition_table_name = name_of_partition_table(date, type: :week)
        return 'Already exists' if connection.table_exists? partition_table_name

        sql = "CREATE TABLE IF NOT EXISTS #{partition_table_name} (
               CHECK ( #{parting_column} >= DATE('#{date_start}') AND #{parting_column} < DATE('#{date_end}') )
               ) INHERITS (#{table_name});"
        execute_sql(sql)
        sql = "ALTER TABLE #{partition_table_name} ADD PRIMARY KEY (id);"
        execute_sql(sql)

        disable_autovacuum(partition_table_name)
        create_partition_indexes(partition_table_name)
        create_partition_named_indexes(partition_table_name)
        create_partition_unique_indexes(partition_table_name)
        create_partition_named_unique_indexes(partition_table_name)
      end

      def create_partitioning_by_week_triggers
        sql = "CREATE OR REPLACE FUNCTION #{table_name}_insert_trigger() RETURNS trigger AS
    $$
           DECLARE
             curY varchar(4);
             curW varchar(2);
             tbl varchar(121);
           BEGIN
              select cast(DATE_PART('year', new.#{parting_column}) as varchar) into curY;
              select lpad(cast(DATE_PART('week', new.#{parting_column}) as varchar), 2, '0') into curW;
              tbl := '#{table_name}_y' || curY || 'w' || curW;
              EXECUTE format('INSERT into %I values ($1.*);', tbl) USING NEW;
              return NEW;
           END;
           $$
         LANGUAGE plpgsql;

         CREATE TRIGGER #{table_name}_insert
         BEFORE INSERT ON #{table_name}
         FOR EACH ROW
         EXECUTE PROCEDURE #{table_name}_insert_trigger();

         -- Trigger function to delete from the master table after the insert
         CREATE OR REPLACE FUNCTION #{table_name}_delete_trigger() RETURNS trigger
             AS $$
         DECLARE
             r #{table_name}%rowtype;
         BEGIN
             DELETE FROM ONLY #{table_name} where id = new.id returning * into r;
             RETURN r;
         end;
         $$
         LANGUAGE plpgsql;

         CREATE TRIGGER #{table_name}_after_insert
         AFTER INSERT ON #{table_name}
         FOR EACH ROW
         EXECUTE PROCEDURE #{table_name}_delete_trigger();"

        execute_sql(sql)
      end
    end
  end
end
