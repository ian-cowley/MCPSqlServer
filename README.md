# SQL Server MCP Server for Windsurf IDE

A standalone MCP (Model Context Protocol) server written in C# that provides SQL Server integration capabilities as an addon to Windsurf IDE.

## Features

- SQL Server connectivity
- Database schema exploration
- Table and view inspection
- Column metadata retrieval
- Stored procedure enumeration
- SQL query execution
- Stored procedure execution
- Debug mode for troubleshooting
- Configurable logging path

## Prerequisites

- .NET 9.0 SDK or higher
- SQL Server instance (local or remote)
- SQL Server client tools

## Setup

1. Build the project:
```cmd
dotnet build
```

2. Configure the application:
   - Copy `appsettings.example.json` to `appsettings.json`
   - Update the connection string and other settings in `appsettings.json` with your SQL Server details
   ```json
   {
     "ConnectionStrings": {
       "DefaultConnection": "Server=your-server;Database=master;User ID=your-username;Password=your-password;TrustServerCertificate=True"
     },
     "LogPath": "C:\\Path\\To\\Your\\LogDirectory\\",
     "DebugMode": "false"
   }
   ```

3. Configure the MCP server in Windsurf:
   - Copy the contents of `windsurf_mcp_config.json` to your Windsurf MCP configuration file (typically located at `~/.codeium/windsurf/mcp_config.json`)
   - Update the path to point to your built executable:
   ```json
   {
     "mcpServers": {
       "sqlMcpService": {
         "command": "path/to/your/MCPSqlServer.exe",
         "args": [],
         "description": "SQL Server MCP Service"
       }
     }
   }
   ```

4. Set up GitHub integration:
   - Create a new GitHub repository for your project
   - Initialize a new Git repository in your project directory using `git init`
   - Add your GitHub repository as a remote using `git remote add origin <repository-url>`
   - Push your changes to the remote repository using `git push -u origin master`

## Configuration Options

The `appsettings.json` file contains the following configuration options:

- `ConnectionStrings:DefaultConnection`: The SQL Server connection string
- `LogPath`: Directory where log files will be stored
- `DebugMode`: Set to "true" to enable detailed debug logging

## Publishing

You can publish the application as a self-contained executable:

```cmd
dotnet publish -c Release -r win-x64 --self-contained true -p:PublishSingleFile=true
```

This will create a single executable file that includes all dependencies.

## Protocol

The MCP server communicates through standard input/output using a JSON-based protocol.

### Request Format

```json
{
  "id": "request-id",
  "action": "action-name",
  "parameters": {
    "param1": "value1",
    "param2": "value2"
  }
}
```

### Response Format

```json
{
  "success": true,
  "data": {
    "key": "value"
  }
}
```

Or in case of error:

```json
{
  "success": false,
  "error": {
    "code": "error_code",
    "message": "Error description"
  }
}
```

## Supported Actions

### 1. Connect to a SQL Server

```json
{
  "action": "connect",
  "parameters": {
    "connectionString": "Data Source=server;Initial Catalog=master;Integrated Security=True;"
  }
}
```

### 2. List Databases

```json
{
  "action": "get_databases",
  "parameters": {}
}
```

### 3. List Tables in a Database

```json
{
  "action": "get_tables",
  "parameters": {
    "database": "AdventureWorks"
  }
}
```

### 4. Get Table Columns

```json
{
  "action": "get_columns",
  "parameters": {
    "database": "AdventureWorks",
    "schema": "Person",
    "table": "Person"
  }
}
```

### 5. List Stored Procedures

```json
{
  "action": "get_procedures",
  "parameters": {
    "database": "AdventureWorks"
  }
}
```

### 6. Execute Database Query

```json
{
  "action": "execute_database_query",
  "parameters": {
    "database": "AdventureWorks",
    "query": "SELECT TOP 10 * FROM Person.Person",
    "parameters": {
      "param1": "value1"
    }
  }
}
```

### 7. Execute System Query

```json
{
  "action": "execute_system_query",
  "parameters": {
    "query": "SELECT name FROM sys.databases",
    "parameters": {
      "param1": "value1"
    }
  }
}
```

### 8. Execute Stored Procedure

```json
{
  "action": "execute_procedure",
  "parameters": {
    "database": "AdventureWorks",
    "schema": "dbo",
    "procedure": "uspGetEmployeeManagers",
    "parameters": {
      "BusinessEntityID": 5
    }
  }
}
```

## Integration with Windsurf

This MCP server can be used from Windsurf IDE to:

1. Browse database schemas
2. Execute SQL queries and view results
3. Get code completion for table and column names
4. Run stored procedures
5. Generate SQL code snippets
6. Analyze database structures

## Code Structure

The application is organized into the following components:

- `Program.cs`: Main entry point and request handling
- `JsonRpcHandler.cs`: Handles JSON-RPC protocol and dispatches requests
- Configuration is loaded from `appsettings.json`
- Logs are written to files specified by the `LogPath` setting

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Error Handling

Common error codes:

- `invalid_request`: Malformed JSON or missing required fields
- `connection_failed`: Failed to connect to SQL Server
- `missing_parameter`: Required parameter not provided
- `query_execution_error`: Error executing a SQL query
- `database_not_found`: Specified database does not exist
- `table_not_found`: Specified table does not exist
