namespace BattlePass;

public class BattlePassPlayer
{
    public int DbId { get; set; }
    public ulong SteamId64 { get; set; }
    public int Level { get; set; }
    public int Xp { get; set; }
    public bool IsPremium { get; set; }
    public DateTime? PremiumExpireDate { get; set; }
    public Dictionary<string, int> MissionProgress { get; set; } = new();
    public HashSet<int> ClaimedNormalRewards { get; set; } = [];
    public HashSet<int> ClaimedPremiumRewards { get; set; } = [];
}