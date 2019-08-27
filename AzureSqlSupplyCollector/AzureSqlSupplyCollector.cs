using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using S2.BlackSwan.SupplyCollector;
using S2.BlackSwan.SupplyCollector.Models;

namespace AzureSqlSupplyCollector
{
    public class AzureSqlSupplyCollector : SupplyCollectorBase
    {
        public override List<string> DataStoreTypes()
        {
            return (new[] { "Azure SQL" }).ToList();
        }

        public string BuildConnectionString(string user, string password, string database, string host)
        {
            var builder = new SqlConnectionStringBuilder();
            builder.DataSource = host;
            builder.InitialCatalog = database;
            builder.UserID = user;
            builder.Password = password;
            return builder.ConnectionString;
        }

        private DataType ConvertDataType(string dbDataType)
        {
            if ("bigint".Equals(dbDataType))
            {
                return DataType.Decimal;
            }
            else if ("int".Equals(dbDataType))
            {
                return DataType.Long;
            }
            else if ("smallint".Equals(dbDataType))
            {
                return DataType.Short;
            }
            else if ("bit".Equals(dbDataType))
            {
                return DataType.Boolean;
            }
            else if ("decimal".Equals(dbDataType))
            {
                return DataType.Decimal;
            }
            else if ("numeric".Equals(dbDataType))
            {
                return DataType.Decimal;
            }
            else if ("tinyint".Equals(dbDataType))
            {
                return DataType.Byte;
            }
            else if ("money".Equals(dbDataType))
            {
                return DataType.Double;
            }
            else if ("smallmoney".Equals(dbDataType))
            {
                return DataType.Double;
            }
            else if ("char".Equals(dbDataType))
            {
                return DataType.Char;
            }
            else if ("varchar".Equals(dbDataType))
            {
                return DataType.String;
            }
            else if ("text".Equals(dbDataType))
            {
                return DataType.String;
            }
            else if ("nchar".Equals(dbDataType))
            {
                return DataType.Char;
            }
            else if ("nvarchar".Equals(dbDataType))
            {
                return DataType.String;
            }
            else if ("ntext".Equals(dbDataType))
            {
                return DataType.String;
            }
            else if ("float".Equals(dbDataType))
            {
                return DataType.Double;
            }
            else if ("real".Equals(dbDataType))
            {
                return DataType.Double;
            }
            else if ("date".Equals(dbDataType))
            {
                return DataType.DateTime;
            }
            else if ("datetime2".Equals(dbDataType))
            {
                return DataType.DateTime;
            }
            else if ("datetimeoffset".Equals(dbDataType))
            {
                return DataType.DateTime;
            }
            else if ("smalldatetime".Equals(dbDataType))
            {
                return DataType.DateTime;
            }
            else if ("datetime".Equals(dbDataType))
            {
                return DataType.DateTime;
            }
            else if ("time".Equals(dbDataType))
            {
                return DataType.DateTime;
            }

            return DataType.Unknown;
        }

        public override List<string> CollectSample(DataEntity dataEntity, int sampleSize) {
            var result = new List<string>();
            using (var conn = new SqlConnection(dataEntity.Container.ConnectionString)) {
                conn.Open();

                int rows;
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = $"SELECT COUNT(*) FROM {dataEntity.Collection.Name}";
                    rows = (int)cmd.ExecuteScalar();
                }

                int sampleRowsPct = rows == 0 ? 0 : (int)(sampleSize * 100.0 / rows);
                sampleRowsPct += 10;
                if (sampleRowsPct > 100)
                    sampleRowsPct = 100;

                using (var cmd = conn.CreateCommand()) {
                    cmd.CommandText = $"SELECT TOP {sampleSize} {dataEntity.Name} FROM {dataEntity.Collection.Name} tablesample({sampleRowsPct} percent)";

                    using (var reader = cmd.ExecuteReader()) {
                        while (reader.Read()) {
                            var val = reader[0];
                            if (val is DBNull) {
                                result.Add(null);
                            }
                            else {
                                result.Add(val.ToString());
                            }
                        }
                    }
                }
            }

            return result;
        }

        public override List<DataCollectionMetrics> GetDataCollectionMetrics(DataContainer container) {
            var metrics = new List<DataCollectionMetrics>();

            using (var conn = new SqlConnection(container.ConnectionString))
            {
                conn.Open();

                using (var cmd = conn.CreateCommand()) {
                    cmd.CommandText =
                        "SELECT \n" +
                        "t.NAME AS TableName,\n" +
                        "s.Name AS SchemaName,\n" +
                        "p.rows AS RowCounts,\n" +
                        "SUM(a.total_pages) * 8 AS TotalSpaceKB,\n" +
                        "SUM(a.used_pages) *8 AS UsedSpaceKB,\n" +
                        "(SUM(a.total_pages) - SUM(a.used_pages)) *8 AS UnusedSpaceKB\n" +
                        "FROM\n" +
                        "sys.tables t\n" +
                        "INNER JOIN\n" +
                        "sys.indexes i ON t.OBJECT_ID = i.object_id\n" +
                        "INNER JOIN\n" +
                        "sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id\n" +
                        "INNER JOIN\n" +
                        "sys.allocation_units a ON p.partition_id = a.container_id\n" +
                        "LEFT OUTER JOIN\n" +
                        "sys.schemas s ON t.schema_id = s.schema_id\n" +
                        "WHERE\n" +
                        "t.NAME NOT LIKE 'dt%'\n" +
                        "AND t.is_ms_shipped = 0\n" +
                        "AND i.OBJECT_ID > 255\n" +
                        "GROUP BY\n" +
                        "t.Name, s.Name, p.Rows\n" +
                        "ORDER BY \n" +
                        "t.Name";

                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            int column = 0;

                            var table = reader.GetString(column++);
                            var schema = reader.GetString(column++);
                            var rows = reader.GetInt64(column++);
                            var totalSizeKb = reader.GetInt64(column++);
                            var usedSizeKb = reader.GetInt64(column++);
                            var unusedSizeKb = reader.GetInt64(column++);
                            

                            metrics.Add(new DataCollectionMetrics()
                            {
                                Schema = schema,
                                Name = table,
                                RowCount = rows,
                                TotalSpaceKB = totalSizeKb,
                                UnUsedSpaceKB = unusedSizeKb,
                                UsedSpaceKB = usedSizeKb,
                            });
                        }
                    }
                }
            }

            return metrics;
        }


        public override (List<DataCollection>, List<DataEntity>) GetSchema(DataContainer container)
        {
            var collections = new List<DataCollection>();
            var entities = new List<DataEntity>();

            using (var conn = new SqlConnection(container.ConnectionString))
            {
                conn.Open();

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText =
                        "select c.table_schema, c.table_name, c.column_name, c.data_type, c.column_default, \n" +
                        "(select count(*)\n" +
                        "   from information_schema.constraint_column_usage ccu\n" +
                        "   join information_schema.table_constraints tc on ccu.constraint_name = tc.constraint_name and ccu.constraint_schema = tc.constraint_schema and tc.constraint_type = 'PRIMARY KEY'\n" +
                        "   where ccu.table_schema = c.table_schema and ccu.table_name = c.table_name and ccu.column_name = c.column_name\n" +
                        ") as is_primary,\n" +
                        "(select count(*)\n" +
                        "   from information_schema.constraint_column_usage ccu\n" +
                        "   join information_schema.table_constraints tc on ccu.constraint_name = tc.constraint_name and ccu.constraint_schema = tc.constraint_schema and tc.constraint_type = 'UNIQUE'\n" +
                        "   where ccu.table_schema = c.table_schema and ccu.table_name = c.table_name and ccu.column_name = c.column_name\n" +
                        ") as is_unique,\n" +
                        "(select count(*)\n" +
                        "   from information_schema.key_column_usage kcu\n" +
                        "   join information_schema.table_constraints tc on kcu.constraint_name = tc.constraint_name and kcu.constraint_schema = tc.constraint_schema and tc.constraint_type = 'FOREIGN KEY'\n" +
                        "   where kcu.table_schema = c.table_schema and kcu.table_name = c.table_name and kcu.column_name = c.column_name\n" +
                        ") as is_ref\n" +
                        "from information_schema.columns c\n" +
                        "where c.table_schema not in ('sys')\n" +
                        "order by table_schema, table_name, ordinal_position";

                    DataCollection collection = null;
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            int column = 0;

                            var schema = reader.GetDbString(column++);
                            var table = reader.GetDbString(column++);
                            var columnName = reader.GetDbString(column++);
                            var dataType = reader.GetDbString(column++);
                            var columnDef = reader.GetDbString(column++);
                            var isPrimary = reader.GetInt32(column++) > 0;
                            var isUnique = reader.GetInt32(column++) > 0;
                            var isRef = reader.GetInt32(column++) > 0;

                            if (collection == null || !collection.Schema.Equals(schema) ||
                                !collection.Name.Equals(table))
                            {

                                collection = new DataCollection(container, table)
                                {
                                    Schema = schema
                                };
                                collections.Add(collection);
                            }

                            entities.Add(new DataEntity(columnName, ConvertDataType(dataType), dataType, container,
                                collection)
                            {
                                IsAutoNumber = !String.IsNullOrEmpty(columnDef) &&
                                               columnDef.StartsWith("nextval(",
                                                   StringComparison.InvariantCultureIgnoreCase),
                                IsComputed = !String.IsNullOrEmpty(columnDef),
                                IsForeignKey = isRef,
                                IsIndexed = isPrimary || isRef,
                                IsPrimaryKey = isPrimary,
                                IsUniqueKey = isUnique
                            });
                        }
                    }
                }
            }

            return (collections, entities);
        }

        public override bool TestConnection(DataContainer container)
        {
            try
            {
                using (var conn = new SqlConnection(container.ConnectionString))
                {
                    conn.Open();
                }

                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }


    }

    internal static class DbDataReaderExtensions
    {
        internal static string GetDbString(this DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
                return null;
            return reader.GetString(ordinal);
        }
    }

}
