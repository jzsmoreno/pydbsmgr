# Use the official Microsoft SQL Server image as the base image
FROM mcr.microsoft.com/mssql/server:2022-latest

# Set environment variables for non-interactive installation
ENV DEBIAN_FRONTEND=noninteractive

# Set the password for the SQL Server 'sa' user
ENV SA_PASSWORD=vSk60DcYRU

# Set the ACCEPT_EULA environment variable to accept the EULA
ENV ACCEPT_EULA=Y

# Expose SQL Server port
EXPOSE 1433

# Start SQL Server
CMD ["/opt/mssql/bin/sqlservr"]

