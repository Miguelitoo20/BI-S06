FROM mcr.microsoft.com/mssql/server:2022-latest

# Variables de entorno para configuración
ENV ACCEPT_EULA=Y
ENV SA_PASSWORD=SecurePass2024!

EXPOSE 1433

CMD /opt/mssql/bin/sqlservr