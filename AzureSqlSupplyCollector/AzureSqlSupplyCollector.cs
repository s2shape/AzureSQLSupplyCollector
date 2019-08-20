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

        public string BuildConnectionString(string user, string password, string database, string host,
            int port = 5439)
        {
            /*var builder = new NpgsqlConnectionStringBuilder();
            builder.Host = host;
            builder.Port = port;
            builder.Database = database;
            builder.Username = user;
            builder.Password = password;
            builder.ServerCompatibilityMode = ServerCompatibilityMode.Redshift;
            return builder.ConnectionString;*/
        }

        private DataType ConvertDataType(string dbDataType)
        {
            if ("integer".Equals(dbDataType))
            {
                return DataType.Long;
            }
            else if ("smallint".Equals(dbDataType))
            {
                return DataType.Short;
            }
            else if ("boolean".Equals(dbDataType))
            {
                return DataType.Boolean;
            }
            else if ("character".Equals(dbDataType))
            {
                return DataType.Char;
            }
            else if ("character varying".Equals(dbDataType))
            {
                return DataType.String;
            }
            else if ("text".Equals(dbDataType))
            {
                return DataType.String;
            }
            else if ("double precision".Equals(dbDataType))
            {
                return DataType.Double;
            }
            else if ("real".Equals(dbDataType))
            {
                return DataType.Double;
            }
            else if ("numeric".Equals(dbDataType))
            {
                return DataType.Decimal;
            }
            else if ("date".Equals(dbDataType))
            {
                return DataType.DateTime;
            }
            else if ("timestamp without time zone".Equals(dbDataType))
            {
                return DataType.DateTime;
            }
            else if ("timestamp with time zone".Equals(dbDataType))
            {
                return DataType.DateTime;
            }
            else if ("datetime".Equals(dbDataType))
            {
                return DataType.DateTime;
            }

            return DataType.Unknown;
        }

        public override List<string> CollectSample(DataEntity dataEntity, int sampleSize) {
            var result = new List<string>();
            using (var conn = new SqlConnection(dataEntity.Container.ConnectionString)) {
                conn.Open();

                using (var cmd = conn.CreateCommand()) {
                    cmd.CommandText = $"SELECT {dataEntity.Name} FROM {dataEntity.Collection.Name} LIMIT {sampleSize}";

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
                        "where c.table_schema not in ('pg_catalog', 'information_schema')\n" +
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
                            var isPrimary = reader.GetInt64(column++) > 0;
                            var isUnique = reader.GetInt64(column++) > 0;
                            var isRef = reader.GetInt64(column++) > 0;

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
