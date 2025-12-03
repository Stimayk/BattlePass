using System.Text.Json;
using CounterStrikeSharp.API.Core;

namespace BattlePass;

public class BattlePassConfig : BasePluginConfig
{
    public string DatabaseHost { get; set; } = "";
    public int DatabasePort { get; set; } = 3306;
    public string DatabaseUser { get; set; } = "";
    public string DatabasePassword { get; set; } = "";
    public string DatabaseName { get; set; } = "";
    public int ServerId { get; set; } = 1;
    public List<string> Commands { get; set; } = ["bp", "battlepass"];
    public int MinPlayers { get; set; } = 2;
    public int StartXp { get; set; } = 0;
    public int StartLevel { get; set; } = 1;
    public Dictionary<int, LevelSettings> Levels { get; set; } = new();
    public Dictionary<int, TaskGroup> Tasks { get; set; } = new();
}

public class LevelSettings
{
    public int Xp { get; set; } = 0;
    public string? Item { get; set; } = null;
    public string? Command { get; set; } = null;
    public string? ItemPremium { get; set; } = null;
    public string? CommandPremium { get; set; } = null;
}

public class TaskGroup
{
    public string GroupName { get; set; } = "";
    public int CountGive { get; set; } = 0;
    public bool ResetProgress { get; set; } = false;
    public Dictionary<string, object> RawTasks { get; set; } = new();

    public Dictionary<string, TaskSettings> GetActualTasks()
    {
        var result = new Dictionary<string, TaskSettings>();

        foreach (var kvp in RawTasks)
        {
            if (kvp.Value is not JsonElement jsonElement) continue;
            var task = jsonElement.Deserialize<TaskSettings>();
            if (task != null) result.Add(kvp.Key, task);
        }

        return result;
    }
}

public class TaskSettings
{
    public string Name { get; set; } = "";
    public string Description { get; set; } = "";
    public string DescriptionPremium { get; set; } = "";
    public string Event { get; set; } = "";
    public Dictionary<string, object> EventParams { get; set; } = new();
    public int Need { get; set; }
    public int NeedPremium { get; set; }
    public int Xp { get; set; }
    public int XpPremium { get; set; }
    public bool PremiumPass { get; set; }
}