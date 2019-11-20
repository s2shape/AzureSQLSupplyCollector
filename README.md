# AzureSqlSupplyCollector
A supply collector designed to connect to Azure SQL

## Build
Run `dotnet build`

## Tests
Run `./run-tests.sh`

## Known issues
- SqlClient doesn't correctly work via reflection. Unable to use load tests, but loader and xunit tests work.
  See https://github.com/dotnet/corefx/issues/24229
