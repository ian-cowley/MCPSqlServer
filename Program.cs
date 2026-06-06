using Microsoft.Extensions.Configuration;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace MCPSqlServer;

/// <summary>
/// MCP SQL Server addon for Windsurf IDE
/// This server acts as an MCP (Model Context Protocol) server that interacts with SQL Server
/// to provide schema information, query execution, and other database operations.
/// </summary>
public class Program
{
    private static readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) },
        PropertyNameCaseInsensitive = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false
    };

    public static async Task Main(string[] args)
    {
        Console.Error.WriteLine("MCP SQL Server addon starting...");
        StreamWriter? writeLogWriter = null;

        try
        {
            InitializeConsoleEncoding();

            // Load configuration and get connection string
            var (connectionString, debugMode, logPath) = LoadConfiguration(args);
            Console.Error.WriteLine($"Log path: {logPath}");

            var (readLogWriter, writeLogWriterTemp) = InitializeLogFiles(logPath);
            writeLogWriter = writeLogWriterTemp;

            // Create JSON-RPC handler with logging
            JsonRpcHandler handler = new(_jsonOptions, connectionString, writeLogWriter, debugMode);
            Console.Error.WriteLine("JSON-RPC handler created");

            // Process requests
            await ProcessRequestsAsync(handler, readLogWriter, writeLogWriter);
        }
        catch (Exception ex)
        {
            await HandleFatalError(ex, writeLogWriter);
        }
        finally
        {
            writeLogWriter?.Dispose();
        }
    }

    private static void InitializeConsoleEncoding()
    {
        Console.InputEncoding = Encoding.UTF8;
        Console.OutputEncoding = Encoding.UTF8;
        Console.Error.WriteLine("Encodings set to UTF8");
    }

    private static (StreamWriter readLogWriter, StreamWriter writeLogWriter) InitializeLogFiles(string basePath)
    {
        // Ensure the log directory exists
        Directory.CreateDirectory(basePath);

        string readLogPath = Path.Combine(basePath, "read.log");
        string writeLogPath = Path.Combine(basePath, "write.log");

        // Create log files with UTF8 encoding and append mode
        StreamWriter readLogWriter = new(readLogPath, append: true, encoding: Encoding.UTF8);
        StreamWriter writeLogWriter = new(writeLogPath, append: true, encoding: Encoding.UTF8);
        readLogWriter.AutoFlush = true;
        writeLogWriter.AutoFlush = true;
        Console.Error.WriteLine("Log files initialized");

        return (readLogWriter, writeLogWriter);
    }

    private static (string connectionString, bool debugMode, string logPath) LoadConfiguration(string[] args)
    {
        string? configPath = null;

        // 1. Check if a path to appsettings.json is passed as an argument
        if (args.Length > 0 && !string.IsNullOrWhiteSpace(args[0]))
        {
            string argPath = args[0];
            if (File.Exists(argPath))
            {
                configPath = argPath;
            }
            else if (Directory.Exists(argPath))
            {
                string combined = Path.Combine(argPath, "appsettings.json");
                if (File.Exists(combined))
                {
                    configPath = combined;
                }
            }

            if (configPath != null)
            {
                Console.Error.WriteLine($"Using configuration file from command-line argument: {configPath}");
            }
            else
            {
                Console.Error.WriteLine($"Warning: Configuration file/directory specified in argument not found: {argPath}");
            }
        }

        // 2. If not passed or not found, try to locate it dynamically
        if (configPath == null)
        {
            configPath = FindAppSettingsPath();
        }

        if (configPath == null)
        {
            throw new FileNotFoundException("Could not find 'appsettings.json' in either the workspace directory, its subdirectories, or the application binary directory.");
        }

        // Load configuration
        string directory = Path.GetDirectoryName(configPath) ?? AppContext.BaseDirectory;
        string filename = Path.GetFileName(configPath);

        IConfigurationRoot configuration = new ConfigurationBuilder()
            .SetBasePath(directory)
            .AddJsonFile(filename, optional: false)
            .Build();
        Console.Error.WriteLine($"Configuration loaded from: {configPath}");

        // Get connection string from configuration
        string? connectionString = configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException($"Connection string 'DefaultConnection' not found in config file: {configPath}");
        }
        Console.Error.WriteLine("Connection string found");

        // Get debug mode setting
        var debugMode = configuration["DebugMode"]?.ToLowerInvariant() is "true";
        Console.Error.WriteLine($"Debug mode: {debugMode}");

        // Get log path setting
        string? logPath = configuration["LogPath"];
        if (string.IsNullOrEmpty(logPath))
        {
            // Fallback to default path if not specified
            logPath = AppContext.BaseDirectory;
            Console.Error.WriteLine("LogPath not found in configuration, using default path");
        }

        return (connectionString, debugMode, logPath);
    }

    private static string? FindAppSettingsPath()
    {
        string currentDir = Directory.GetCurrentDirectory();

        // 1. Check current directory directly
        string directPath = Path.Combine(currentDir, "appsettings.json");
        if (File.Exists(directPath))
        {
            return directPath;
        }

        // 2. Scan subdirectories (excluding common build/temp folders)
        try
        {
            var candidates = Directory.EnumerateFiles(currentDir, "appsettings.json", SearchOption.AllDirectories)
                .Where(p => !p.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}") &&
                            !p.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}") &&
                            !p.Contains($"{Path.DirectorySeparatorChar}.git{Path.DirectorySeparatorChar}") &&
                            !p.Contains($"{Path.DirectorySeparatorChar}.vs{Path.DirectorySeparatorChar}") &&
                            !p.Contains($"{Path.DirectorySeparatorChar}.vscode{Path.DirectorySeparatorChar}"))
                .ToList();

            if (candidates.Count == 1)
            {
                return candidates[0];
            }
            else if (candidates.Count > 1)
            {
                Console.Error.WriteLine("Multiple appsettings.json files found in workspace:");
                foreach (var candidate in candidates)
                {
                    Console.Error.WriteLine($"  - {candidate}");
                }
                var bestCandidate = candidates.OrderBy(c => c.Split(Path.DirectorySeparatorChar).Length).First();
                Console.Error.WriteLine($"Using the one closest to root: {bestCandidate}");
                return bestCandidate;
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Warning: Error searching workspace subdirectories for appsettings.json: {ex.Message}");
        }

        // 3. Fallback to AppContext.BaseDirectory
        string fallbackPath = Path.Combine(AppContext.BaseDirectory, "appsettings.json");
        if (File.Exists(fallbackPath))
        {
            return fallbackPath;
        }

        return null;
    }

    private static async Task ProcessRequestsAsync(JsonRpcHandler handler, StreamWriter readLogWriter, StreamWriter writeLogWriter)
    {
        // Read from stdin and process requests
        using StreamReader streamReader = new(Console.OpenStandardInput(), Console.InputEncoding);
        Console.Error.WriteLine("Ready to process requests");

        while (true)
        {
            string? line = await streamReader.ReadLineAsync();
            if (line == null)
            {
                // End of input
                break;
            }

            // Log the read input
            await readLogWriter.WriteLineAsync($"[{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff}] {line}");

            try
            {
                await handler.ProcessRequestAsync(line);
            }
            catch (Exception ex)
            {
                await LogErrorAsync(ex, writeLogWriter);
            }
        }
    }

    private static async Task LogErrorAsync(Exception ex, StreamWriter? writeLogWriter)
    {
        Console.Error.WriteLine($"Error processing request: {ex}");
        if (writeLogWriter != null)
        {
            await writeLogWriter.WriteLineAsync($"[{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff}] Error: {ex}");
        }
    }

    private static async Task HandleFatalError(Exception ex, StreamWriter? writeLogWriter)
    {
        Console.Error.WriteLine($"Fatal error: {ex}");
        if (writeLogWriter != null)
        {
            await writeLogWriter.WriteLineAsync($"[{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff}] Fatal error: {ex}");
            await writeLogWriter.DisposeAsync();
        }
        Environment.Exit(1);
    }
}
