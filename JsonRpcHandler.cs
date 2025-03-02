using MCPSqlServer.Models;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Text.Json;

namespace MCPSqlServer;

public class JsonRpcHandler
{
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly string _connectionString;
    private readonly StreamWriter _writeLogWriter;
    private readonly bool _debugMode;

    public JsonRpcHandler(JsonSerializerOptions jsonOptions, string connectionString, StreamWriter writeLogWriter, bool debugMode = false)
    {
        _jsonOptions = jsonOptions;
        _connectionString = connectionString;
        _writeLogWriter = writeLogWriter;
        _debugMode = debugMode;
    }

    public async Task ProcessRequestAsync(string requestJson)
    {
        try
        {
            if (_debugMode)
            {
                await _writeLogWriter.WriteLineAsync($"[{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff}] REQUEST: {requestJson}");
            }

            // Parse the JSON request
            JsonRpcRequest? request = JsonSerializer.Deserialize<JsonRpcRequest>(requestJson, _jsonOptions);

            if (request == null || request.JsonRpc != "2.0")
            {
                await SendErrorResponseAsync(request?.Id, JsonRpcErrorCodes.InvalidRequest, "Invalid JSON-RPC 2.0 request");
                return;
            }

            switch (request.Method)
            {
                case "initialize":
                    await HandleInitializeAsync(request);
                    break;
                case "notifications/initialized":
                    await HandleNotificationsInitializedAsync(request);
                    break;
                case "tools/list":
                    await HandleToolsListAsync(request);
                    break;
                case "tools/call":
                    if (request.Params == null || !request.Params.TryGetValue("name", out JsonElement toolName))
                    {
                        await SendErrorResponseAsync(request.Id, JsonRpcErrorCodes.InvalidParams, "Tool name is required", isMcpToolCall: false);
                        return;
                    }

                    switch (toolName.GetString())
                    {
                        case "get_databases":
                            await HandleGetDatabasesAsync(request);
                            break;
                        case "get_tables":
                            await HandleGetTablesAsync(request);
                            break;
                        case "get_columns":
                            await HandleGetColumnsAsync(request);
                            break;
                        case "get_procedures":
                            await HandleGetProceduresAsync(request);
                            break;
                        case "get_procedure_definition":
                            await HandleGetProcedureDefinitionAsync(request);
                            break;
                        case "execute_database_query":
                            await HandleExecuteDatabaseQueryAsync(request);
                            break;
                        case "execute_system_query":
                            await HandleExecuteSystemQueryAsync(request);
                            break;
                        case "execute_procedure":
                            await HandleExecuteProcedureAsync(request);
                            break;
                        default:
                            await SendErrorResponseAsync(request.Id, JsonRpcErrorCodes.MethodNotFound, $"Unknown tool: {toolName}", isMcpToolCall: false);
                            break;
                    }
                    break;
                default:
                    await SendErrorResponseAsync(request.Id, JsonRpcErrorCodes.MethodNotFound, $"Unknown method: {request.Method}", isMcpToolCall: false);
                    break;
            }
        }
        catch (JsonException ex)
        {
            await SendErrorResponseAsync(null, JsonRpcErrorCodes.ParseError, ex.Message);
        }
        catch (Exception ex)
        {
            await SendErrorResponseAsync(null, JsonRpcErrorCodes.InternalError, ex.Message);
        }
    }

    private async Task HandleInitializeAsync(JsonRpcRequest request)
    {
        var capabilities = new
        {
            name = "SQL Server MCP",
            version = "1.0.0",
            capabilities = new
            {
                databases = true,
                tables = true,
                columns = true,
                procedures = true,
                queryExecution = true
            }
        };

        await SendSuccessResponseAsync(request.Id, capabilities, isMcpToolCall: false);
    }

    private async Task HandleNotificationsInitializedAsync(JsonRpcRequest request)
    {
        // This is a notification method, so we just acknowledge it with an empty object
        await SendSuccessResponseAsync(request.Id, new { }, isMcpToolCall: false);
    }

    private async Task HandleToolsListAsync(JsonRpcRequest request)
    {
        var tools = new object[]
        {
            new
            {
                name = "get_databases",
                description = "List all available SQL Server databases, response is in jsonrpc 2.0 format",
                parameters = new { },
                returns = new {
                    type = "object, that contains a list of databases"
                }
            },
            new
            {
                name = "get_tables",
                description = "List all tables in a specified database",
                parameters = new
                {
                    database = "Database name",
                    schema = "Optional schema name, defaults to 'dbo'"
                },
                required = new[] { "database" }
            },
            new
            {
                name = "get_columns",
                description = "List all columns in a specified table",
                parameters = new
                {
                    database = "Database name",
                    schema = "Optional schema name, defaults to 'dbo'",
                    table = "Table name"
                },
                required = new[] { "database", "table" }
            },
            new
            {
                name = "get_procedures",
                description = "List all stored procedures in a specified database",
                parameters = new
                {
                    database = "Database name",
                    schema = "Optional schema name, defaults to 'dbo'"
                },
                required = new[] { "database" }
            },
            new
            {
                name = "get_procedure_definition",
                description = "Get the definition of a stored procedure",
                parameters = new
                {
                    database = "Database name",
                    schema = "Schema name",
                    name = "Procedure name"
                },
                required = new[] { "database", "schema", "name" }
            },
            new
            {
                name = "execute_procedure",
                description = "Execute a stored procedure",
                parameters = new
                {
                    database = "Database name",
                    procedure = "Procedure name",
                    parameters = "Dictionary of parameter names and values"
                },
                required = new[] { "database", "procedure", "parameters" }
            },
            new
            {
                name = "execute_database_query",
                description = "Execute a SQL query in the context of a specific database",
                parameters = new
                {
                    database = "Database name",
                    query = "SQL query to execute"
                },
                required = new[] { "database", "query" }
            },
            new
            {
                name = "execute_system_query",
                description = "Execute a SQL query at the server instance level (no database context required)",
                parameters = new
                {
                    query = "SQL query to execute"
                },
                required = new[] { "query" }
            }
        };

        await SendSuccessResponseAsync(request.Id, new { tools }, isMcpToolCall: false);
    }

    private async Task HandleGetDatabasesAsync(JsonRpcRequest request)
    {
        using SqlConnection connection = new(_connectionString);
        await connection.OpenAsync();

        List<string> databases = [];

        using SqlCommand command = new(
            "SELECT name FROM sys.databases WHERE database_id > 4", // Exclude system databases
            connection);

        using SqlDataReader reader = await command.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            databases.Add(reader.GetString(0));
        }

        await SendSuccessResponseAsync(request.Id, new { databases }, isMcpToolCall: true);
    }

    private async Task HandleGetTablesAsync(JsonRpcRequest request)
    {
        if (request.Params == null ||
            !request.Params.TryGetValue("arguments", out JsonElement args) ||
            !args.TryGetProperty("database", out JsonElement database))
        {
            await SendErrorResponseAsync(request.Id, JsonRpcErrorCodes.InvalidParams, "Database name is required", isMcpToolCall: true);
            return;
        }

        using SqlConnection connection = new(_connectionString);
        await connection.OpenAsync();

        // Switch to the specified database
        using (SqlCommand useDbCommand = new($"USE [{database}]", connection))
        {
            await useDbCommand.ExecuteNonQueryAsync();
        }

        List<TableInfo> tables = [];

        using SqlCommand command = new(
            @"SELECT t.TABLE_SCHEMA, t.TABLE_NAME, t.TABLE_TYPE 
              FROM INFORMATION_SCHEMA.TABLES t
              ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME",
            connection);

        using SqlDataReader reader = await command.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            tables.Add(new TableInfo
            {
                Schema = reader.GetString(0),
                Name = reader.GetString(1),
                Type = reader.GetString(2)
            });
        }

        await SendSuccessResponseAsync(request.Id, new { tables }, isMcpToolCall: true);
    }

    private async Task HandleGetColumnsAsync(JsonRpcRequest request)
    {
        if (request.Params == null
            || !request.Params.TryGetValue("arguments", out JsonElement args)
            || !args.TryGetProperty("database", out JsonElement database)
            || !args.TryGetProperty("table", out JsonElement table)
        )
        {
            await SendErrorResponseAsync(request.Id, JsonRpcErrorCodes.InvalidParams, "Database and Table Parameters are required", isMcpToolCall: true);
            return;
        }

        string schema = "dbo";
        if (request.Params.TryGetValue("schema", out JsonElement schemaParam))
        {
            schema = schemaParam.ToString();
        }

        using SqlConnection connection = new(_connectionString);
        await connection.OpenAsync();

        // Switch to the specified database
        using (SqlCommand useDbCommand = new($"USE [{database}]", connection))
        {
            await useDbCommand.ExecuteNonQueryAsync();
        }

        List<ColumnInfo> columns = [];

        using SqlCommand command = new(
            @"SELECT 
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.NUMERIC_PRECISION,
                c.NUMERIC_SCALE,
                c.IS_NULLABLE,
                COLUMNPROPERTY(OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME)), c.COLUMN_NAME, 'IsIdentity') as IS_IDENTITY,
                CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END as IS_PRIMARY_KEY
            FROM INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN (
                SELECT ku.TABLE_CATALOG, ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                    ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                    AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
            ) pk 
                ON c.TABLE_CATALOG = pk.TABLE_CATALOG
                AND c.TABLE_SCHEMA = pk.TABLE_SCHEMA
                AND c.TABLE_NAME = pk.TABLE_NAME
                AND c.COLUMN_NAME = pk.COLUMN_NAME
            WHERE c.TABLE_SCHEMA = @schema AND c.TABLE_NAME = @table
            ORDER BY c.ORDINAL_POSITION",
            connection);

        command.Parameters.AddWithValue("@schema", schema);
        command.Parameters.AddWithValue("@table", table.ToString());

        using SqlDataReader reader = await command.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            columns.Add(new ColumnInfo
            {
                Name = reader.GetString(0),
                DataType = reader.GetString(1),
                MaxLength = !reader.IsDBNull(2) ? reader.GetInt32(2) : null,
                Precision = !reader.IsDBNull(3) ? reader.GetByte(3) : null,
                Scale = !reader.IsDBNull(4) ? reader.GetInt32(4) : null,
                IsNullable = reader.GetString(5) == "YES",
                IsIdentity = !reader.IsDBNull(6) && reader.GetInt32(6) == 1,
                IsPrimaryKey = reader.GetInt32(7) == 1
            });
        }

        await SendSuccessResponseAsync(request.Id, new { columns }, isMcpToolCall: true);
    }

    private async Task HandleGetProceduresAsync(JsonRpcRequest request)
    {
        if (request.Params == null
            || !request.Params.TryGetValue("arguments", out JsonElement args)
            || !args.TryGetProperty("database", out JsonElement database))
        {
            await SendErrorResponseAsync(request.Id, JsonRpcErrorCodes.InvalidParams, "Database name is required", isMcpToolCall: true);
            return;
        }

        using SqlConnection connection = new(_connectionString);
        await connection.OpenAsync();

        // Switch to the specified database
        using (SqlCommand useDbCommand = new($"USE [{database}]", connection))
        {
            await useDbCommand.ExecuteNonQueryAsync();
        }

        List<ProcedureInfo> procedures = [];

        using SqlCommand command = new(
            @"SELECT 
                SCHEMA_NAME(p.schema_id) as [Schema],
                p.name as [Name]
            FROM sys.procedures p
            ORDER BY [Schema], [Name]",
            connection);

        using SqlDataReader reader = await command.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            procedures.Add(new ProcedureInfo
            {
                Schema = reader.GetString(0),
                Name = reader.GetString(1)
            });
        }

        await SendSuccessResponseAsync(request.Id, new { procedures }, isMcpToolCall: true);
    }

    private async Task HandleGetProcedureDefinitionAsync(JsonRpcRequest request)
    {
        if (request.Params == null
            || !request.Params.TryGetValue("arguments", out JsonElement args)
            || !args.TryGetProperty("database", out JsonElement database)
            || !args.TryGetProperty("schema", out JsonElement schema)
            || !args.TryGetProperty("name", out JsonElement name))
        {
            await SendErrorResponseAsync(request.Id, JsonRpcErrorCodes.InvalidParams, "Database, schema and procedure name are required", isMcpToolCall: true);
            return;
        }

        using SqlConnection connection = new(_connectionString);
        await connection.OpenAsync();

        // Switch to the specified database
        using (SqlCommand useDbCommand = new($"USE [{database}]", connection))
        {
            await useDbCommand.ExecuteNonQueryAsync();
        }

        using SqlCommand command = new(
            @"SELECT 
                pm.definition as [Definition]
            FROM sys.procedures p
            INNER JOIN sys.sql_modules pm ON p.object_id = pm.object_id
            WHERE SCHEMA_NAME(p.schema_id) = @schema AND p.name = @name",
            connection);

        command.Parameters.AddWithValue("@schema", schema.ToString());
        command.Parameters.AddWithValue("@name", name.ToString());

        using SqlDataReader reader = await command.ExecuteReaderAsync();

        if (!await reader.ReadAsync())
        {
            await SendErrorResponseAsync(request.Id, JsonRpcErrorCodes.InvalidParams, "Procedure not found", isMcpToolCall: true);
            return;
        }

        string definition = reader.GetString(0);
        await SendSuccessResponseAsync(request.Id, new { definition }, isMcpToolCall: true);
    }

    private async Task HandleExecuteDatabaseQueryAsync(JsonRpcRequest request)
    {
        if (request.Params == null
            || !request.Params.TryGetValue("arguments", out JsonElement args)
            || !args.TryGetProperty("database", out JsonElement database)
            || !args.TryGetProperty("query", out JsonElement query))
        {
            await SendErrorResponseAsync(request.Id, JsonRpcErrorCodes.InvalidParams, "Parameters are required", isMcpToolCall: true);
            return;
        }

        using SqlConnection connection = new(_connectionString);
        await connection.OpenAsync();

        // Switch to the specified database
        using (SqlCommand useDbCommand = new($"USE [{database}]", connection))
        {
            await useDbCommand.ExecuteNonQueryAsync();
        }

        using SqlCommand command = new(query.ToString(), connection);
        using SqlDataReader reader = await command.ExecuteReaderAsync();

        List<Dictionary<string, object?>> results = await ReadDataReaderAsync(reader);
        await SendSuccessResponseAsync(request.Id, new { results }, isMcpToolCall: true);
    }

    private async Task HandleExecuteSystemQueryAsync(JsonRpcRequest request)
    {
        if (request.Params == null ||
            !request.Params.TryGetValue("arguments", out JsonElement args) ||
            !args.TryGetProperty("query", out JsonElement query))
        {
            await SendErrorResponseAsync(request.Id, JsonRpcErrorCodes.InvalidParams, "SQL query is required", isMcpToolCall: true);
            return;
        }

        using SqlConnection connection = new(_connectionString);
        await connection.OpenAsync();

        using SqlCommand command = new(query.ToString(), connection);
        using SqlDataReader reader = await command.ExecuteReaderAsync();

        List<Dictionary<string, object?>> results = await ReadDataReaderAsync(reader);
        await SendSuccessResponseAsync(request.Id, new { results }, isMcpToolCall: true);
    }

    private async Task HandleExecuteProcedureAsync(JsonRpcRequest request)
    {
        if (request.Params == null
            || !request.Params.TryGetValue("arguments", out JsonElement args)
            || !args.TryGetProperty("database", out JsonElement database)
            || !args.TryGetProperty("procedure", out JsonElement procedure)
            )
        {
            await SendErrorResponseAsync(request.Id, JsonRpcErrorCodes.InvalidParams, "Database and Procedure Parameters are required", isMcpToolCall: true);
            return;
        }

        string schema = "dbo";
        if (request.Params.TryGetValue("schema", out JsonElement schemaParam))
        {
            schema = schemaParam.ToString();
        }

        Dictionary<string, JsonElement>? parameters = null;
        if (request.Params.TryGetValue("parameters", out JsonElement parametersParam))
        {
            parameters = parametersParam.Deserialize<Dictionary<string, JsonElement>>();
        }

        using SqlConnection connection = new(_connectionString);
        await connection.OpenAsync();

        // Switch to the specified database
        using (SqlCommand useDbCommand = new($"USE [{database}]", connection))
        {
            await useDbCommand.ExecuteNonQueryAsync();
        }

        using SqlCommand command = new($"[{schema}].[{procedure}]", connection);
        command.CommandType = CommandType.StoredProcedure;

        if (parameters != null)
        {
            foreach (KeyValuePair<string, JsonElement> param in parameters)
            {
                command.Parameters.AddWithValue(param.Key, param.Value.GetRawText());
            }
        }

        using SqlDataReader reader = await command.ExecuteReaderAsync();
        List<Dictionary<string, object?>> results = await ReadDataReaderAsync(reader);

        await SendSuccessResponseAsync(request.Id, new { results }, isMcpToolCall: true);
    }

    private static async Task<List<Dictionary<string, object?>>> ReadDataReaderAsync(SqlDataReader reader)
    {
        List<Dictionary<string, object?>> results = [];

        while (await reader.ReadAsync())
        {
            Dictionary<string, object?> row = [];

            for (int i = 0; i < reader.FieldCount; i++)
            {
                string columnName = reader.GetName(i);
                object? value = reader.IsDBNull(i) ? null : reader.GetValue(i);
                row[columnName] = value;
            }

            results.Add(row);
        }

        return results;
    }

    private async Task SendSuccessResponseAsync(object? id, object? result, bool isMcpToolCall = false)
    {
        JsonRpcResponse response;

        if (isMcpToolCall)
        {
            response = new JsonRpcResponse
            {
                Id = id,
                Result = new McpToolResult
                {
                    Content =
                    [
                        new McpContent
                        {
                            Type = "text",
                            Text = JsonSerializer.Serialize(result, _jsonOptions)
                        }
                    ]
                }
            };
        }
        else
        {
            response = new JsonRpcResponse
            {
                Id = id,
                Result = result
            };
        }

        string responseJson = JsonSerializer.Serialize(response, _jsonOptions);
        if (_debugMode)
        {
            await _writeLogWriter.WriteLineAsync($"[{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff}] RESPONSE: {responseJson}");
        }
        await Console.Out.WriteLineAsync(responseJson);
    }

    private async Task SendErrorResponseAsync(object? id, int code, string message, bool isMcpToolCall = false)
    {
        JsonRpcResponse response;

        if (isMcpToolCall)
        {
            response = new JsonRpcResponse
            {
                Id = id,
                Error = new JsonRpcError { Code = code, Message = message },
                Result = new McpToolResult
                {
                    Content =
                    [
                        new McpContent
                        {
                            Type = "text",
                            Text = message
                        }
                    ],
                    IsError = true
                }
            };
        }
        else
        {
            response = new JsonRpcResponse
            {
                Id = id,
                Error = new JsonRpcError { Code = code, Message = message }
            };
        }

        string responseJson = JsonSerializer.Serialize(response, _jsonOptions);
        if (_debugMode)
        {
            await _writeLogWriter.WriteLineAsync($"[{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff}] ERROR RESPONSE: {responseJson}");
        }
        await Console.Out.WriteLineAsync(responseJson);
    }

}
