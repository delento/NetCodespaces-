using System.Net;
using System.Text.Json;
using Microsoft.Azure.Functions.Worker;        // ✅ Correct
using Microsoft.Azure.Functions.Worker.Http;   // ✅ Correct
using Microsoft.Extensions.Logging;
using MySqlConnector;


public class Ingest
{
    private readonly ILogger _logger;
    public Ingest(ILoggerFactory loggerFactory) => _logger = loggerFactory.CreateLogger<Ingest>();

    [Function("Ingest")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "post")] HttpRequestData req)
    {
        try
        {
            var body = await new StreamReader(req.Body).ReadToEndAsync();
            var json = JsonDocument.Parse(body).RootElement;
            var deviceId = json.GetProperty("id").GetString();
            var type = json.GetProperty("type").GetString();
            var dataArray = json.GetProperty("data").EnumerateArray();

            // MySQL connection string
            var host = Environment.GetEnvironmentVariable("MYSQL_HOST");
            var db   = Environment.GetEnvironmentVariable("MYSQL_DB");
            var user = Environment.GetEnvironmentVariable("MYSQL_USER");
            var pwd  = Environment.GetEnvironmentVariable("MYSQL_PASSWORD");
            var cs   = $"Server={host};Database={db};User ID={user};Password={pwd};SslMode=Required;";

            using var conn = new MySqlConnection(cs);
            await conn.OpenAsync();

            foreach (var item in dataArray)
            {
                if (type == "dailyReading")
                {
                    var ts = ConvertToSGTime(item.GetProperty("timeStamp").GetInt64());
                    var readings = item.GetProperty("port1").GetDecimal();
                    var cmd = new MySqlCommand("INSERT INTO dailyreading (id, DateTime, Readings) VALUES (@id,@dt,@read)", conn);
                    cmd.Parameters.AddWithValue("@id", deviceId);
                    cmd.Parameters.AddWithValue("@dt", ts);
                    cmd.Parameters.AddWithValue("@read", readings);
                    await cmd.ExecuteNonQueryAsync();
                }
                else if (type == "intervalFlow")
                {
                    var interval = item.GetProperty("interval").GetInt32();
                    var startTs = item.GetProperty("startTimeStamp").GetInt64();
                    var values = item.GetProperty("intervalConsumption").EnumerateArray().ToList();
                    for (int i = 0; i < values.Count; i++)
                    {
                        var ts = ConvertToSGTime(startTs + (i * interval));
                        var val = values[i].GetDecimal();
                        var cmd = new MySqlCommand("INSERT INTO intervalflow (id, DateTime, Data) VALUES (@id,@dt,@data)", conn);
                        cmd.Parameters.AddWithValue("@id", deviceId);
                        cmd.Parameters.AddWithValue("@dt", ts);
                        cmd.Parameters.AddWithValue("@data", val);
                        await cmd.ExecuteNonQueryAsync();
                    }
                }
                else if (type == "meterInfo")
                {
                    var ts = ConvertToSGTime(item.GetProperty("timeStamp").GetInt64());
                    var cmd = new MySqlCommand(@"
                        INSERT INTO meterinfo
                        (id, Date, imei, firmwareVersion, sn, nominalBatteryCapacity, meterModel, firmwareUpdateDateTime, latencyResponse, battPercentage, rssi, dot)
                        VALUES (@id,@dt,@imei,@fw,@sn,@cap,@model,@fwdate,@lat,@batt,@rssi,@dot)", conn);

                    cmd.Parameters.AddWithValue("@id", deviceId);
                    cmd.Parameters.AddWithValue("@dt", ts);
                    cmd.Parameters.AddWithValue("@imei", item.GetProperty("imei").GetString());
                    cmd.Parameters.AddWithValue("@fw", item.GetProperty("firmwareVersion").GetString());
                    cmd.Parameters.AddWithValue("@sn", item.GetProperty("sn").GetString());
                    cmd.Parameters.AddWithValue("@cap", item.GetProperty("nominalBatteryCapacity").GetInt32());
                    cmd.Parameters.AddWithValue("@model", item.GetProperty("meterModel").GetString());
                    cmd.Parameters.AddWithValue("@fwdate", item.GetProperty("firmwareUpdateDateTime").GetString());
                    cmd.Parameters.AddWithValue("@lat", item.GetProperty("latencyResponse").GetInt32());
                    cmd.Parameters.AddWithValue("@batt", item.GetProperty("battPercentage").GetInt32());
                    cmd.Parameters.AddWithValue("@rssi", item.GetProperty("rssi").GetInt32());
                    cmd.Parameters.AddWithValue("@dot", item.GetProperty("dot").GetDecimal());
                    await cmd.ExecuteNonQueryAsync();
                }
                else if (type == "alarm")
                {
                    var tsField = item.TryGetProperty("timeStamp", out var tsProp) ? tsProp.GetInt64() : item.GetProperty("timestamp").GetInt64();
                    var ts = ConvertToSGTime(tsField);
                    foreach (var prop in item.EnumerateObject())
                    {
                        if (int.TryParse(prop.Name, out int alarmCode))
                        {
                            var condition = prop.Value.GetBoolean();
                            var value = item.TryGetProperty("value", out var valProp) ? valProp.GetDecimal() : (decimal?)null;
                            var cmd = new MySqlCommand("INSERT INTO alarm (id, Date, AlarmType, Condition, value) VALUES (@id,@dt,@code,@cond,@val)", conn);
                            cmd.Parameters.AddWithValue("@id", deviceId);
                            cmd.Parameters.AddWithValue("@dt", ts);
                            cmd.Parameters.AddWithValue("@code", alarmCode);
                            cmd.Parameters.AddWithValue("@cond", condition ? 1 : 0);
                            cmd.Parameters.AddWithValue("@val", value.HasValue ? value.Value : DBNull.Value);
                            await cmd.ExecuteNonQueryAsync();
                        }
                    }
                }
            }

            var response = req.CreateResponse(HttpStatusCode.OK);
            await response.WriteStringAsync("Data inserted successfully");
            return response;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing request");
            var response = req.CreateResponse(HttpStatusCode.InternalServerError);
            await response.WriteStringAsync($"Error: {ex.Message}");
            return response;
        }
    }

    private string ConvertToSGTime(long unixSeconds)
    {
        var sgTime = DateTimeOffset.FromUnixTimeSeconds(unixSeconds).ToOffset(TimeSpan.FromHours(8));
        return sgTime.ToString("yyyy-MM-dd HH:mm:ss");
    }
}
