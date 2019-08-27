using System;
using System.Collections.Generic;
using System.Linq;
using S2.BlackSwan.SupplyCollector.Models;
using Xunit;

namespace AzureSqlSupplyCollectorTests
{
    public class AzureSqlSupplyCollectorTests : IClassFixture<LaunchSettingsFixture>
    {
        private readonly AzureSqlSupplyCollector.AzureSqlSupplyCollector _instance;
        public readonly DataContainer _container;
        private LaunchSettingsFixture _fixture;

        public AzureSqlSupplyCollectorTests(LaunchSettingsFixture fixture)
        {
            _fixture = fixture;
            _instance = new AzureSqlSupplyCollector.AzureSqlSupplyCollector();
            _container = new DataContainer()
            {
                ConnectionString = _instance.BuildConnectionString(
                    Environment.GetEnvironmentVariable("AZURE_SQL_USER"),
                    Environment.GetEnvironmentVariable("AZURE_SQL_PASSWORD"),
                    Environment.GetEnvironmentVariable("AZURE_SQL_DATABASE"),
                    Environment.GetEnvironmentVariable("AZURE_SQL_HOST")
                    )
            };
        }

        [Fact]
        public void DataStoreTypesTest()
        {
            var result = _instance.DataStoreTypes();
            Assert.Contains("Azure SQL", result);
        }

        [Fact]
        public void TestConnectionTest()
        {
            var result = _instance.TestConnection(_container);
            Assert.True(result);
        }

        [Fact]
        public void GetDataCollectionMetricsTest()
        {
            var metrics = new DataCollectionMetrics[] {
                new DataCollectionMetrics()
                    {Name = "test_data_types", RowCount = 1, TotalSpaceKB = 144},
                new DataCollectionMetrics()
                    {Name = "test_field_names", RowCount = 1, TotalSpaceKB = 72},
                new DataCollectionMetrics()
                    {Name = "test_index", RowCount = 7, TotalSpaceKB = 144},
                new DataCollectionMetrics()
                    {Name = "test_index_ref", RowCount = 2, TotalSpaceKB = 72}
            };

            var result = _instance.GetDataCollectionMetrics(_container);
            Assert.Equal(metrics.Length, result.Count);

            foreach (var metric in metrics)
            {
                var resultMetric = result.Find(x => x.Name.Equals(metric.Name));
                Assert.NotNull(resultMetric);

                Assert.Equal(metric.RowCount, resultMetric.RowCount);
                Assert.Equal(metric.TotalSpaceKB, resultMetric.TotalSpaceKB);
            }
        }

        [Fact]
        public void GetTableNamesTest()
        {
            var (tables, elements) = _instance.GetSchema(_container);
            Assert.Equal(4, tables.Count);
            Assert.Equal(35, elements.Count);

            var tableNames = new string[] { "test_data_types", "test_field_names", "test_index", "test_index_ref" };
            foreach (var tableName in tableNames)
            {
                var table = tables.Find(x => x.Name.Equals(tableName));
                Assert.NotNull(table);
            }
        }

        [Fact]
        public void DataTypesTest()
        {
            var (tables, elements) = _instance.GetSchema(_container);

            var dataTypes = new Dictionary<string, string>() {
                {"id_field", "int"},
                {"bigint_field", "bigint"},
                {"numeric_field", "numeric"},
                {"bit_field", "bit"},
                {"smallint_field", "smallint"},
                {"decimal_field", "decimal"},
                {"smallmoney_field", "smallmoney"},
                {"int_field", "int"},
                {"tinyint_field", "tinyint"},
                {"money_field", "money"},
                {"float_field", "float"},
                {"real_field", "real"},
                {"date_field", "date"},
                {"datetimeoffset_field", "datetimeoffset"},
                {"datetime2_field", "datetime2"},
                {"smalldatetime_field", "smalldatetime"},
                {"datetime_field", "datetime"},
                {"time_field", "time"},
                {"char_field", "char"},
                {"varchar_field", "varchar"},
                {"text_field", "text"},
                {"nchar_field", "nchar"},
                {"nvarchar_field", "nvarchar"},
                {"ntext_field", "ntext"}
            };

            var columns = elements.Where(x => x.Collection.Name.Equals("test_data_types")).ToArray();
            Assert.Equal(24, columns.Length);

            foreach (var column in columns)
            {
                Assert.Contains(column.Name, (IDictionary<string, string>)dataTypes);
                Assert.Equal(dataTypes[column.Name], column.DbDataType);
            }
        }

        [Fact]
        public void SpecialFieldNamesTest()
        {
            var (tables, elements) = _instance.GetSchema(_container);

            var fieldNames = new string[] { "id", "low_case", "UPCASE", "CamelCase", "Table", "array", "SELECT" };

            var columns = elements.Where(x => x.Collection.Name.Equals("test_field_names")).ToArray();
            Assert.Equal(fieldNames.Length, columns.Length);

            foreach (var column in columns)
            {
                Assert.Contains(column.Name, fieldNames);
            }
        }

        [Fact]
        public void AttributesTest()
        {
            var (tables, elements) = _instance.GetSchema(_container);

            var idFields = elements.Where(x => x.Name.Equals("id")).ToArray();
            Assert.Equal(3, idFields.Length);

            foreach (var idField in idFields)
            {
                Assert.Equal(DataType.Long, idField.DataType);
                Assert.True(idField.IsPrimaryKey);
            }

            var uniqueField = elements.Find(x => x.Name.Equals("name"));
            Assert.True(uniqueField.IsUniqueKey);

            var refField = elements.Find(x => x.Name.Equals("index_id"));
            Assert.True(refField.IsForeignKey);

            foreach (var column in elements)
            {
                if (column.Name.Equals("id") || column.Name.Equals("name") || column.Name.Equals("index_id") || column.Name.Equals("id_field"))
                {
                    continue;
                }

                Assert.False(column.IsPrimaryKey);
                Assert.False(column.IsAutoNumber);
                Assert.False(column.IsForeignKey);
                Assert.False(column.IsIndexed);
            }
        }

        [Fact]
        public void CollectSampleTest()
        {
            var entity = new DataEntity("name", DataType.String, "varchar", _container,
                new DataCollection(_container, "test_index"));

            var samples = _instance.CollectSample(entity, 2);
            Assert.Equal(2, samples.Count);

            samples = _instance.CollectSample(entity, 7);
            Assert.Equal(7, samples.Count);
            Assert.Contains("Wednesday", samples);
        }

    }
}
