using Dapper;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Dynamic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IConfiguration _configuration;
    private readonly TimeSpan _interval = TimeSpan.FromSeconds(30);

    public Worker(ILogger<Worker> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var tablesToSync = new (string TableName, string[] KeyColumns)[]
                    {
                        ("HMS_GUESTS",        new[] { "GUEST_CODE" }),
                        ("HMS_CHECKIN_HEADER",new[] { "RESV_ID", "CHECKIN_ID" }),   // ← composite key
                        ("HMS_CHECKIN_LINES", new[] { "Auto_No" }),
                        ("HMS_POST_CHARGES",  new[] { "SERIAL_NO" }),
                        ("HMS_RESERVATION_ROOM_GUEST",new[]{ "RESV_ID", "GUEST_CODE"})
                    };

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                foreach (var (table, keys) in tablesToSync)
                    await SyncTableFromCloud(table, keys);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in SyncDataAsync");
            }

            await Task.Delay(_interval, stoppingToken);
        }
    }

    private async Task SyncTableFromCloud(string tableName, string[] keyColumns)
    {
        var cloudConnStr = _configuration.GetConnectionString("CloudDb");
        var localConnStr = _configuration.GetConnectionString("LocalDb");

        // -------------------------------
        // 1. Pull unsynced rows from cloud
        // -------------------------------
        IEnumerable<IDictionary<string, object>> rows;
        using (var cloud = new SqlConnection(cloudConnStr))
        {
            rows = (await cloud.QueryAsync(
                $"SELECT * FROM {tableName} WHERE Synced = 0"))
                .Cast<IDictionary<string, object>>()    // keep as dictionary for easy access
                .ToList();
        }

        if (!rows.Any()) return;

        // -------------------------------
        // 2. Up-sert into local
        // -------------------------------
        using (var local = new SqlConnection(localConnStr))
        {
            await local.OpenAsync();

            foreach (var row in rows)
            {
                // build a param dictionary *without* the Synced flag
                var param = new ExpandoObject() as IDictionary<string, object>;
                foreach (var kv in row)
                    if (!kv.Key.Equals("Synced", StringComparison.OrdinalIgnoreCase))
                        param[kv.Key] = kv.Value;

                // -------- existence test --------
                var whereClause = string.Join(" AND ", keyColumns.Select(c => $"{c} = @{c}"));
                var existsSql = $"SELECT 1 FROM {tableName} WHERE {whereClause}";
                bool exists = (await local.ExecuteScalarAsync<int?>(existsSql, param)) == 1;

                if (exists)
                {
                    // build the list of columns we want to update:
                    var columnsToUpdate = param.Keys
                        .Except(keyColumns, StringComparer.OrdinalIgnoreCase);

                    // special‐case: never update Auto_No in HMS_CHECKIN_HEADER
                    if (string.Equals(tableName, "HMS_CHECKIN_HEADER", StringComparison.OrdinalIgnoreCase))
                    {
                        columnsToUpdate = columnsToUpdate
                            .Where(c => !c.Equals("Auto_No", StringComparison.OrdinalIgnoreCase));
                    }

                    var setList = string.Join(", ",
                        columnsToUpdate.Select(c => $"{c} = @{c}")
                    );

                    var updateSql = $@"
                                        UPDATE {tableName}
                                        SET {setList}
                                        WHERE {whereClause}
                                    ";

                    await local.ExecuteAsync(updateSql, param);
                }
                else
                {
                    // Clone & filter out any special‐case columns for insert
                    var insertParam = param;
                    if (tableName.Equals("HMS_CHECKIN_HEADER", StringComparison.OrdinalIgnoreCase) || tableName.Equals("HMS_RESERVATION_ROOM_GUEST", StringComparison.OrdinalIgnoreCase))
                    {
                        insertParam = param
                            .Where(kv => !kv.Key.Equals("Auto_No", StringComparison.OrdinalIgnoreCase))
                            .ToDictionary(kv => kv.Key, kv => kv.Value);
                    }



                    // === NEW: For CHECKIN_LINES, override Auto_No with header's Auto_No ===
                    if (tableName.Equals("HMS_CHECKIN_LINES", StringComparison.OrdinalIgnoreCase))
                    {
                        // grab the RESV_ID and CHECKIN_ID from the row
                        var resvId = param["RESV_ID"];
                        var checkinId = param["CHECKIN_ID"];

                        // look up the header Auto_No in the local DB
                        var headerAutoNo = await local.ExecuteScalarAsync<int?>(@"
                                            SELECT Auto_No 
                                            FROM HMS_CHECKIN_HEADER 
                                            WHERE RESV_ID    = @ResvId 
                                              AND CHECKIN_ID = @CheckinId",
                            new { ResvId = resvId, CheckinId = checkinId }
                        );

                        if (headerAutoNo == null)
                            throw new InvalidOperationException(
                                $"No HMS_CHECKIN_HEADER found for RESV_ID={resvId}, CHECKIN_ID={checkinId}"
                            );

                        // overwrite (or add) the Auto_No in our insert parameters
                        insertParam["Auto_No"] = headerAutoNo.Value;
                    }
                    // ====================================================================

                    // Build the INSERT statement from whichever columns remain
                    var cols = string.Join(", ", insertParam.Keys);
                    var vals = string.Join(", ", insertParam.Keys.Select(c => "@" + c));
                    var insertSql = $@"
                        INSERT INTO {tableName} ({cols})
                        VALUES ({vals})
                    ";

                    await local.ExecuteAsync(insertSql, insertParam);
                }
            }
        }

        // -------------------------------
        // 3. Mark processed rows as Synced
        // -------------------------------
        using (var cloud = new SqlConnection(cloudConnStr))
        {
            await cloud.OpenAsync();
            foreach (var row in rows)
            {
                var syncSql = $"UPDATE {tableName} SET Synced = 1 WHERE " +
                                  string.Join(" AND ", keyColumns.Select(c => $"{c} = @{c}"));
                await cloud.ExecuteAsync(syncSql, row);
            }
        }

        _logger.LogInformation("Synced {Count} rows from {Table}", rows.Count(), tableName);
    }

}
