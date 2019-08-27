#!/bin/bash

docker run -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=<YourStrong@Passw0rd>" -p 1433:1433 --name sql1 -d mcr.microsoft.com/mssql/server:2017-latest
sleep 20
docker cp AzureSqlSupplyCollectorTests/tests/data.sql sql1:/data.sql
docker exec sql1 /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P "<YourStrong@Passw0rd>" -i /data.sql

echo { > AzureSqlSupplyCollectorTests/Properties/launchSettings.json
echo   \"profiles\": { >> AzureSqlSupplyCollectorTests/Properties/launchSettings.json
echo     \"AzureSqlSupplyCollectorTests\": { >> AzureSqlSupplyCollectorTests/Properties/launchSettings.json
echo       \"commandName\": \"Project\", >> AzureSqlSupplyCollectorTests/Properties/launchSettings.json
echo       \"environmentVariables\": { >> AzureSqlSupplyCollectorTests/Properties/launchSettings.json
echo         \"AZURE_SQL_HOST\": \"localhost\", >> AzureSqlSupplyCollectorTests/Properties/launchSettings.json
echo         \"AZURE_SQL_USER\": \"sa\", >> AzureSqlSupplyCollectorTests/Properties/launchSettings.json
echo         \"AZURE_SQL_DATABASE\": \"TestDb\", >> AzureSqlSupplyCollectorTests/Properties/launchSettings.json
echo         \"AZURE_SQL_PASSWORD\": \"\<YourStrong@Passw0rd\>\" >> AzureSqlSupplyCollectorTests/Properties/launchSettings.json
echo       } >> AzureSqlSupplyCollectorTests/Properties/launchSettings.json
echo     } >> AzureSqlSupplyCollectorTests/Properties/launchSettings.json
echo   } >> AzureSqlSupplyCollectorTests/Properties/launchSettings.json
echo } >> AzureSqlSupplyCollectorTests/Properties/launchSettings.json

dotnet build
dotnet test
docker stop sql1
docker rm sql1
