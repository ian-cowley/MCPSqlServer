using System.Text.Json.Serialization;

namespace MCPSqlServer.Models;

public class McpResource
{
    [JsonPropertyName("uri")]
    public string Uri { get; set; } = string.Empty;

    [JsonPropertyName("mimeType")]
    public string MimeType { get; set; } = string.Empty;

    [JsonPropertyName("text")]
    public string Text { get; set; } = string.Empty;
}
