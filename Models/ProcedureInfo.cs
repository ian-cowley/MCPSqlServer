using System.Text.Json.Serialization;

namespace MCPSqlServer.Models;

public class ProcedureInfo
{
    [JsonPropertyName("schema")]
    public string Schema { get; set; } = string.Empty;

    [JsonPropertyName("name")]
    public string Name { get; set; } = string.Empty;

    [JsonPropertyName("definition")]
    public string? Definition { get; set; }
}
