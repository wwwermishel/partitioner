require 'pg_partitioner/version'
require 'pg_partitioner/separation_type/base'
require 'pg_partitioner/separation_type/week'
require 'pg_partitioner/separation_type/month'
require 'pg_partitioner/separation_type/quater'

module PgPartitioner
  def self.extended(base)
    base.extend(
      PgPartitioner::SeparationType::Base,
      PgPartitioner::SeparationType::Week,
      PgPartitioner::SeparationType::Month,
      PgPartitioner::SeparationType::Quater
    )
  end

  # Template method
  # Column which will determine partition for row (must be date or datetime type). Default value is :created_at
  def parting_column
    :created_at
  end

  # Template method
  def partition_table_indexes; end

  def partition_table_named_indexes; end

  # Template method
  def partition_table_unique_indexes; end

  # Template method
  def partition_table_named_unique_indexes; end

  private

  def execute_sql(sql_string)
    connection.execute(sql_string)
  end

  def create_custom_index(table_name, index_fields, is_unique = false)
    ActiveRecord::Migration.add_index table_name, index_fields, unique: is_unique
  end

  def create_custom_named_index(table_name, index_fields, name, is_unique = false)
    ActiveRecord::Migration.add_index table_name, index_fields, name: name, unique: is_unique
  end
end
