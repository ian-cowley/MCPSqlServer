using System.Text.Json.Serialization;

namespace MCPSqlServer.Models;

public class ColumnInfo
{
    [JsonPropertyName("name")]
    public string Name { get; set; } = string.Empty;

    [JsonPropertyName("dataType")]
    public string DataType { get; set; } = string.Empty;

    [JsonPropertyName("maxLength")]
    public int? MaxLength { get; set; }

    [JsonPropertyName("precision")]
    public byte? Precision { get; set; }

    [JsonPropertyName("scale")]
    public int? Scale { get; set; }

    [JsonPropertyName("isNullable")]
    public bool IsNullable { get; set; }

    [JsonPropertyName("isIdentity")]
    public bool IsIdentity { get; set; }

    [JsonPropertyName("isPrimaryKey")]
    public bool IsPrimaryKey { get; set; }
}
