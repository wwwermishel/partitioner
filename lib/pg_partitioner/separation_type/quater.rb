module PgPartitioner
  module SeparationType
    module Quater
      def create_current_quater_table
        create_quater_table(Date.today)
      end

      def create_next_quater_table
        create_quater_table(Date.today.next_quarter)
      end

      def create_quater_table(date = Date.today)
        date_start = date.at_beginning_of_quarter
        date_end = date.end_of_quarter.next_month.at_beginning_of_month
        partition_table_name = name_of_partition_table(date, type: :quater)
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

      def create_partitioning_by_quater_triggers
        sql = "CREATE OR REPLACE FUNCTION #{table_name}_insert_trigger() RETURNS trigger AS
    $$
           DECLARE
             curY varchar(4);
             curQ varchar(1);
             tbl varchar(121);
           BEGIN
              select cast(DATE_PART('year', new.#{parting_column}) as varchar) into curY;
              select lpad(cast(DATE_PART('quarter', new.#{parting_column}) as varchar), 2, '0') into curQ;
              tbl := '#{table_name}_y' || curY || 'q' || curQ;
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
