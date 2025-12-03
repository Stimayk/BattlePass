using System.Text.Json;
using Dapper;
using Microsoft.Extensions.Logging;
using MySqlConnector;

namespace BattlePass;

public class DataBaseService
{
    private readonly BattlePassConfig _config;
    private readonly string _connectionString;
    private readonly ILogger<DataBaseService> _logger;

    public DataBaseService(BattlePassConfig config)
    {
        var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        _logger = loggerFactory.CreateLogger<DataBaseService>();
        _config = config;
        _connectionString = BuildDatabaseConnectionString();
    }

    private string BuildDatabaseConnectionString()
    {
        MySqlConnectionStringBuilder builder = new()
        {
            Server = _config.DatabaseHost,
            Port = (uint)_config.DatabasePort,
            UserID = _config.DatabaseUser,
            Password = _config.DatabasePassword,
            Database = _config.DatabaseName,
            Pooling = true
        };
        return builder.ConnectionString;
    }

    private async Task<MySqlConnection> GetOpenConnectionAsync()
    {
        var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();
        return connection;
    }

    public async Task InitTablesAsync()
    {
        await using var conn = await GetOpenConnectionAsync();

        await conn.ExecuteAsync("""
                                    CREATE TABLE IF NOT EXISTS `bp_users` (
                                        `id` INT AUTO_INCREMENT PRIMARY KEY,
                                        `server_id` INT NOT NULL DEFAULT 1,
                                        `steamid` VARCHAR(64) NOT NULL,
                                        `level` INT NOT NULL DEFAULT 1,
                                        `xp` INT NOT NULL DEFAULT 0,
                                        `premium` TINYINT(1) NOT NULL DEFAULT 0,
                                        `premium_expire` DATETIME NULL DEFAULT NULL,
                                        UNIQUE KEY `idx_steam_server` (`steamid`, `server_id`)
                                    );
                                """);

        await conn.ExecuteAsync("""
                                    CREATE TABLE IF NOT EXISTS `bp_levels` (
                                        `server_id` INT NOT NULL,
                                        `id` INT NOT NULL,
                                        `xp` INT NOT NULL DEFAULT 0,
                                        PRIMARY KEY (`server_id`, `id`)
                                    );
                                """);

        await conn.ExecuteAsync("""
                                    CREATE TABLE IF NOT EXISTS `bp_tasks` (
                                        `server_id` INT NOT NULL,
                                        `identifier` VARCHAR(64) NOT NULL,
                                        `group_name` VARCHAR(128) NOT NULL,
                                        `name` VARCHAR(255) NOT NULL,
                                        `description` TEXT,
                                        `description_premium` TEXT,
                                        `event` VARCHAR(64) NOT NULL,
                                        `event_params` TEXT, 
                                        `need` INT NOT NULL DEFAULT 0,
                                        `need_premium` INT NOT NULL DEFAULT 0,
                                        `xp` INT NOT NULL DEFAULT 0,
                                        `xp_premium` INT NOT NULL DEFAULT 0,
                                        `premium_pass` TINYINT(1) NOT NULL DEFAULT 0,
                                        PRIMARY KEY (`server_id`, `identifier`)
                                    );
                                """);

        await conn.ExecuteAsync("""
                                    CREATE TABLE IF NOT EXISTS `bp_user_missions` (
                                        `id` INT AUTO_INCREMENT PRIMARY KEY,
                                        `user_id` INT NOT NULL,
                                        `mission` VARCHAR(64) NOT NULL,
                                        `progress` INT NOT NULL DEFAULT 0,
                                        KEY `idx_user` (`user_id`),
                                        KEY `idx_mission` (`mission`),
                                        UNIQUE KEY `idx_user_mission` (`user_id`, `mission`)
                                    );
                                """);

        await conn.ExecuteAsync("""
                                    CREATE TABLE IF NOT EXISTS `bp_user_rewards` (
                                        `user_id` INT NOT NULL,
                                        `level_id` INT NOT NULL,
                                        `is_premium` TINYINT(1) NOT NULL DEFAULT 0,
                                        PRIMARY KEY (`user_id`, `level_id`, `is_premium`)
                                    );
                                """);

        await conn.ExecuteAsync("""
                                    CREATE TABLE IF NOT EXISTS `bp_rotations` (
                                        `server_id` INT NOT NULL,
                                        `group_key` VARCHAR(64) NOT NULL,
                                        `expire_at` DATETIME NOT NULL,
                                        `active_tasks` TEXT NOT NULL,
                                        PRIMARY KEY (`server_id`, `group_key`)
                                    );
                                """);

        _logger.LogInformation("Database tables initialized.");
    }

    public async Task SyncStaticDataAsync()
    {
        await using var conn = await GetOpenConnectionAsync();
        await using var trans = await conn.BeginTransactionAsync();

        try
        {
            await conn.ExecuteAsync("DELETE FROM `bp_levels` WHERE `server_id` = @sid", new { sid = _config.ServerId },
                trans);

            var levelsData = _config.Levels.Select(kv => new { _config.ServerId, Id = kv.Key, kv.Value.Xp });
            if (levelsData.Any())
                await conn.ExecuteAsync(
                    "INSERT INTO `bp_levels` (`server_id`, `id`, `xp`) VALUES (@ServerId, @Id, @Xp)", levelsData,
                    trans);

            await conn.ExecuteAsync("DELETE FROM `bp_tasks` WHERE `server_id` = @sid", new { sid = _config.ServerId },
                trans);
            var tasksToInsert = new List<object>();

            foreach (var groupKvp in _config.Tasks)
            {
                var group = groupKvp.Value;
                var actualTasks = group.GetActualTasks();
                foreach (var (id, t) in actualTasks)
                    tasksToInsert.Add(new
                    {
                        _config.ServerId, Identifier = id, group.GroupName, t.Name, t.Description, t.DescriptionPremium,
                        t.Event, EventParams = JsonSerializer.Serialize(t.EventParams), t.Need, t.NeedPremium, t.Xp,
                        t.XpPremium, PremiumPass = t.PremiumPass ? 1 : 0
                    });
            }

            if (tasksToInsert.Count != 0)
            {
                const string sql = """
                                       INSERT INTO `bp_tasks` (`server_id`, `identifier`, `group_name`, `name`, `description`, `description_premium`, `event`, `event_params`, `need`, `need_premium`, `xp`, `xp_premium`, `premium_pass`) 
                                       VALUES (@ServerId, @Identifier, @GroupName, @Name, @Description, @DescriptionPremium, @Event, @EventParams, @Need, @NeedPremium, @Xp, @XpPremium, @PremiumPass)
                                   """;
                await conn.ExecuteAsync(sql, tasksToInsert, trans);
            }

            await trans.CommitAsync();
            _logger.LogInformation("Static data synced.");
        }
        catch (Exception ex)
        {
            await trans.RollbackAsync();
            _logger.LogError(ex, "Error syncing static data.");
            throw;
        }
    }

    public async Task<BattlePassPlayer> LoadPlayerAsync(ulong steamId64, int startLvl, int startXp)
    {
        await using var conn = await GetOpenConnectionAsync();
        var steamIdStr = steamId64.ToString();

        var userData = await conn.QueryFirstOrDefaultAsync(
            "SELECT * FROM `bp_users` WHERE `steamid` = @steamId AND `server_id` = @serverId",
            new { steamId = steamIdStr, serverId = _config.ServerId });

        if (userData == null)
        {
            await conn.ExecuteAsync(
                "INSERT INTO `bp_users` (`server_id`, `steamid`, `level`, `xp`, `premium`) VALUES (@sid, @s, @l, @x, 0)",
                new { sid = _config.ServerId, s = steamIdStr, l = startLvl, x = startXp });

            userData = await conn.QuerySingleAsync(
                "SELECT * FROM `bp_users` WHERE `steamid` = @steamId AND `server_id` = @serverId",
                new { steamId = steamIdStr, serverId = _config.ServerId });
        }

        bool isPremium = Convert.ToBoolean(userData.premium);
        DateTime? expireDate = userData.premium_expire;

        if (isPremium && expireDate.HasValue && expireDate.Value < DateTime.UtcNow)
        {
            isPremium = false;
            expireDate = null;
            _ = conn.ExecuteAsync("UPDATE `bp_users` SET `premium` = 0, `premium_expire` = NULL WHERE `id` = @id",
                new { userData.id });
        }

        var player = new BattlePassPlayer
        {
            DbId = (int)userData.id,
            SteamId64 = steamId64,
            Level = (int)userData.level,
            Xp = (int)userData.xp,
            IsPremium = isPremium,
            PremiumExpireDate = expireDate
        };

        var missions = await conn.QueryAsync(
            "SELECT `mission`, `progress` FROM `bp_user_missions` WHERE `user_id` = @uid",
            new { uid = player.DbId });

        foreach (var m in missions) player.MissionProgress[m.mission] = m.progress;

        var rewards = await conn.QueryAsync(
            "SELECT `level_id`, `is_premium` FROM `bp_user_rewards` WHERE `user_id` = @uid",
            new { uid = player.DbId });

        foreach (var r in rewards)
            if (Convert.ToBoolean(r.is_premium))
                player.ClaimedPremiumRewards.Add((int)r.level_id);
            else
                player.ClaimedNormalRewards.Add((int)r.level_id);

        return player;
    }

    public async Task SavePlayerAsync(BattlePassPlayer player)
    {
        await using var conn = await GetOpenConnectionAsync();
        await using var trans = await conn.BeginTransactionAsync();

        try
        {
            await conn.ExecuteAsync(
                "UPDATE `bp_users` SET `level` = @l, `xp` = @x, `premium` = @p, `premium_expire` = @pe WHERE `id` = @id",
                new
                {
                    l = player.Level,
                    x = player.Xp,
                    p = player.IsPremium ? 1 : 0,
                    pe = player.PremiumExpireDate,
                    id = player.DbId
                },
                trans);

            foreach (var kvp in player.MissionProgress)
                await conn.ExecuteAsync("""
                                            INSERT INTO `bp_user_missions` (`user_id`, `mission`, `progress`) 
                                            VALUES (@uid, @mis, @prog)
                                            ON DUPLICATE KEY UPDATE `progress` = @prog
                                        """,
                    new { uid = player.DbId, mis = kvp.Key, prog = kvp.Value },
                    trans);

            var rewardsParams = new List<object>();

            foreach (var lvlId in player.ClaimedNormalRewards)
                rewardsParams.Add(new { uid = player.DbId, lvl = lvlId, prem = 0 });

            foreach (var lvlId in player.ClaimedPremiumRewards)
                rewardsParams.Add(new { uid = player.DbId, lvl = lvlId, prem = 1 });

            if (rewardsParams.Count > 0)
                await conn.ExecuteAsync("""
                                            INSERT IGNORE INTO `bp_user_rewards` (`user_id`, `level_id`, `is_premium`) 
                                            VALUES (@uid, @lvl, @prem)
                                        """, rewardsParams, trans);

            await trans.CommitAsync();
        }
        catch (Exception ex)
        {
            await trans.RollbackAsync();
            _logger.LogError(ex, "Error saving player {PlayerSteamId64}", player.SteamId64);
        }
    }

    public async Task<RotationData?> GetRotationAsync(string groupKey)
    {
        await using var conn = await GetOpenConnectionAsync();
        var result = await conn.QueryFirstOrDefaultAsync(
            "SELECT * FROM `bp_rotations` WHERE `server_id` = @sid AND `group_key` = @key",
            new { sid = _config.ServerId, key = groupKey });

        if (result == null) return null;

        return new RotationData
        {
            ExpireAt = result.expire_at,
            ActiveTasks = JsonSerializer.Deserialize<List<string>>((string)result.active_tasks) ?? []
        };
    }

    public async Task SaveRotationAsync(string groupKey, DateTime expireAt, List<string> activeTasks)
    {
        await using var conn = await GetOpenConnectionAsync();
        var jsonTasks = JsonSerializer.Serialize(activeTasks);

        await conn.ExecuteAsync("""
                                    INSERT INTO `bp_rotations` (`server_id`, `group_key`, `expire_at`, `active_tasks`)
                                    VALUES (@sid, @key, @exp, @tasks)
                                    ON DUPLICATE KEY UPDATE `expire_at` = @exp, `active_tasks` = @tasks
                                """,
            new { sid = _config.ServerId, key = groupKey, exp = expireAt, tasks = jsonTasks });
    }

    public async Task ResetMissionsProgressAsync(IEnumerable<string> taskIds)
    {
        var enumerable = taskIds as string[] ?? taskIds.ToArray();
        if (enumerable.Length == 0) return;

        await using var conn = await GetOpenConnectionAsync();

        await conn.ExecuteAsync(
            "DELETE FROM `bp_user_missions` WHERE `mission` IN @ids",
            new { ids = enumerable }
        );

        _logger.LogInformation("[BattlePass] Progress reset for tasks: {Tasks}", string.Join(", ", enumerable));
    }

    public class RotationData
    {
        public DateTime ExpireAt { get; set; }
        public List<string> ActiveTasks { get; set; } = [];
    }
}