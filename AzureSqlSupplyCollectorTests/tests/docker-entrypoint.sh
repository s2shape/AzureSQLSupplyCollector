#!/bin/bash
cleanup ()
{
kill -s SIGTERM $!
exit 0
}

trap cleanup SIGINT
trap cleanup SIGQUIT
trap cleanup SIGTSTP

echo "Starting SQL server..."
/opt/mssql/bin/sqlservr &
sleep 30

echo "Executing init scripts"
ls -al /docker-entrypoint-initdb.d
for f in /docker-entrypoint-initdb.d/*; do case "$f" in *.sh) echo "$0: running $f"; . "$f" ;; *.sql) echo "$0: running $f" && until /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P "$SA_PASSWORD" -i "$f"; do >&2 echo "Sql server is unavailable - sleeping"; sleep 2; done & ;; *) echo "$0: ignoring $f" ;; esac done
echo "Startup finished"

fg

for (( ; ; ))
do
   sleep 1
done
