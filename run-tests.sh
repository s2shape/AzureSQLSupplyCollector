#!/bin/bash

docker run -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=<YourStrong@Passw0rd>" -p 1433:1433 --name sql1 -d mcr.microsoft.com/mssql/server:2017-latest
sleep 20
docker cp AzureSqlSupplyCollectorLoader/tests/data.sql sql1:/data.sql
docker exec sql1 /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P "<YourStrong@Passw0rd>" -Q "create database TestDb;"
docker exec sql1 /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P "<YourStrong@Passw0rd>" -i /data.sql

export AZURE_SQL_HOST=localhost
export AZURE_SQL_USER=sa
export AZURE_SQL_DATABASE=TestDb
export AZURE_SQL_PASSWORD=YourStrong@Passw0rd

dotnet build
dotnet test
docker stop sql1
docker rm sql1
