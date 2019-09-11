FROM mcr.microsoft.com/mssql/server:2017-latest
MAINTAINER burilovmv@gmail.com

ENV ACCEPT_EULA Y
ENV SA_PASSWORD <YourStrong@Passw0rd>

ADD data.sql /docker-entrypoint-initdb.d/
ADD docker-entrypoint.sh /
RUN ls -la *.sh; chmod +x *.sh; ls -la *.sh

EXPOSE 10000/tcp

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["/opt/mssql/bin/sqlservr"]
