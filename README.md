# SQL Server MCP Server

A standalone MCP (Model Context Protocol) server written in C# that provides SQL Server integration capabilities for any MCP-compliant IDE or client.

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

3. Configure the MCP server in your IDE / client:
   - Add the server configuration to your IDE's MCP config file (e.g., Windsurf, Claude Desktop config, or Cline/Cursor settings).
   - Point the command to the path of your built executable:
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

The MCP server communicates through standard input/output using the standard JSON-RPC 2.0 Model Context Protocol.

### Request Format (tools/call)

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "tool-name",
    "arguments": {
      "param1": "value1",
      "param2": "value2"
    }
  }
}
```

### Response Format

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "content": [
      {
        "type": "text",
        "text": "JSON-serialized tool output"
      }
    ]
  }
}
```

Or in case of error:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "error": {
    "code": -32602,
    "message": "Error description"
  }
}
```

## Supported Tools

### 1. List Databases (`get_databases`)

List all available non-system SQL Server databases.

Request:
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "get_databases",
    "arguments": {}
  }
}
```

### 2. List Tables in a Database (`get_tables`)

List all tables and views in a specified database.

Request:
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "get_tables",
    "arguments": {
      "database": "AdventureWorks",
      "schema": "dbo"
    }
  }
}
```

### 3. Get Table Columns (`get_columns`)

List all columns and column metadata in a specified table.

Request:
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "get_columns",
    "arguments": {
      "database": "AdventureWorks",
      "schema": "Person",
      "table": "Person"
    }
  }
}
```

### 4. List Stored Procedures (`get_procedures`)

List all stored procedures in a specified database.

Request:
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "get_procedures",
    "arguments": {
      "database": "AdventureWorks",
      "schema": "dbo"
    }
  }
}
```

### 5. Get Stored Procedure Definition (`get_procedure_definition`)

Get the definition of a stored procedure.

Request:
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "get_procedure_definition",
    "arguments": {
      "database": "AdventureWorks",
      "schema": "dbo",
      "name": "uspGetEmployeeManagers"
    }
  }
}
```

### 6. Execute Database Query (`execute_database_query`)

Execute a SQL query in the context of a specific database.

Request:
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "execute_database_query",
    "arguments": {
      "database": "AdventureWorks",
      "query": "SELECT TOP 10 * FROM Person.Person"
    }
  }
}
```

### 7. Execute System Query (`execute_system_query`)

Execute a SQL query at the server instance level (no database context required).

Request:
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "execute_system_query",
    "arguments": {
      "query": "SELECT name FROM sys.databases"
    }
  }
}
```

### 8. Execute Stored Procedure (`execute_procedure`)

Execute a stored procedure.

Request:
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "execute_procedure",
    "arguments": {
      "database": "AdventureWorks",
      "schema": "dbo",
      "procedure": "uspGetEmployeeManagers",
      "parameters": {
        "BusinessEntityID": 5
      }
    }
  }
}
```

## IDE / Client Integration

This MCP server can be used from any MCP-compliant IDE or client (e.g. Windsurf, Cursor, Cline, Claude Desktop) to:

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
