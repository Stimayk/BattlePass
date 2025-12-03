using System.Collections.Concurrent;
using System.Reflection;
using System.Text.Json;
using CounterStrikeSharp.API;
using CounterStrikeSharp.API.Core;
using CounterStrikeSharp.API.Core.Capabilities;
using CounterStrikeSharp.API.Modules.Commands;
using CounterStrikeSharp.API.Modules.Events;
using CounterStrikeSharp.API.Modules.Timers;
using MenuManager;
using Microsoft.Extensions.Logging;

namespace BattlePass;

public class BattlePass : BasePlugin, IPluginConfig<BattlePassConfig>
{
    public override string ModuleName => "BattlePass";
    public override string ModuleAuthor => "E!N";
    public override string ModuleVersion => "v1.0.0";
    
    private static IMenuApi _menuApi = null!;
    private static readonly PluginCapability<IMenuApi?> MenuCapability = new("menu:nfcore");
    private readonly ConcurrentDictionary<string, byte> _activeTaskIds = new();
    private readonly Dictionary<string, List<(string TaskId, TaskSettings Settings)>> _tasksByEvent = new();
    private static DataBaseService? _dataBaseService;
    private static readonly ConcurrentDictionary<ulong, BattlePassPlayer> OnlinePlayers = new();
    private static readonly ConcurrentDictionary<ulong, PlayerRoundData> RoundSessions = new();

    private string Prefix => Localizer["battlepass.prefix"];
    public BattlePassConfig Config { get; set; } = new();

    public void OnConfigParsed(BattlePassConfig config)
    {
        Config = config;
        _dataBaseService = new DataBaseService(config);

        Task.Run(async () =>
        {
            try
            {
                await _dataBaseService.InitTablesAsync();
                await _dataBaseService.SyncStaticDataAsync();
                await CheckAndRotateTasks();
            }
            catch (Exception ex)
            {
                Logger.LogError("[BattlePass] DB Init Critical Error: {ExMessage}", ex.Message);
            }
        });
    }

    public override void Load(bool hotReload)
    {
        RegisterEventHandler<EventPlayerConnectFull>(OnPlayerConnectFull);
        RegisterEventHandler<EventPlayerDisconnect>(OnPlayerDisconnect);
        RegisterEventHandler<EventRoundEnd>(OnRoundEnd);

        Config.Commands.ForEach(c =>
        {
            AddCommand($"css_{c}", "BattlePass main menu", (player, _) => MainMenu(player));
        });

        AddCommand("css_bp_give_premium", "Give BP Premium", CmdGivePremium);
        AddCommand("css_bp_take_premium", "Take BP Premium", CmdTakePremium);
        AddCommand("css_bp_give_xp", "Give BP XP", CmdGiveXp);
        AddCommand("css_bp_take_xp", "Take BP XP", CmdTakeXp);

        InitTaskEvents();

        if (hotReload)
            LoadAllOnlinePlayers();

        AddTimer(60.0f, () => { Task.Run(CheckAndRotateTasks); }, TimerFlags.REPEAT);
    }

    private void LoadAllOnlinePlayers()
    {
        var players = Utilities.GetPlayers();
        var count = 0;

        foreach (var player in players.Where(player => player is
                     { IsValid: true, IsBot: false, IsHLTV: false, AuthorizedSteamID: not null }))
        {
            LoadPlayerToCache(player.AuthorizedSteamID!.SteamId64, player.PlayerName);
            count++;

            RoundSessions.TryAdd(player.AuthorizedSteamID.SteamId64, new PlayerRoundData());

            if (!OnlinePlayers.TryGetValue(player.AuthorizedSteamID.SteamId64, out var bpPlayer)) continue;
            CheckLevelUp(bpPlayer, player);
        }

        if (count > 0) Logger.LogInformation("[BattlePass] Started loading data for {Count} online players...", count);
    }

    private void LoadPlayerToCache(ulong steamId, string playerName)
    {
        Task.Run(async () =>
        {
            try
            {
                if (_dataBaseService == null) return;

                var bpPlayer = await _dataBaseService.LoadPlayerAsync(steamId, Config.StartLevel, Config.StartXp);

                OnlinePlayers.AddOrUpdate(steamId, bpPlayer, (_, _) => bpPlayer);
            }
            catch (Exception ex)
            {
                Logger.LogError("[BattlePass] Error loading player {PlayerName}: {ExMessage}", playerName, ex.Message);
            }
        });
    }

    private HookResult OnPlayerConnectFull(EventPlayerConnectFull @event, GameEventInfo info)
    {
        var player = @event.Userid;

        if (player == null || !player.IsValid || player.IsBot || player.IsHLTV || player.AuthorizedSteamID == null)
            return HookResult.Continue;

        var steamId = player.AuthorizedSteamID.SteamId64;

        RoundSessions.TryAdd(steamId, new PlayerRoundData());

        LoadPlayerToCache(steamId, player.PlayerName);

        if (!OnlinePlayers.TryGetValue(steamId, out var bpPlayer))
            return HookResult.Continue;

        CheckLevelUp(bpPlayer, player);
        return HookResult.Continue;
    }

    private static HookResult OnPlayerDisconnect(EventPlayerDisconnect @event, GameEventInfo info)
    {
        var player = @event.Userid;

        if (player == null || !player.IsValid || player.IsBot || player.AuthorizedSteamID == null)
            return HookResult.Continue;

        var steamId = player.AuthorizedSteamID.SteamId64;

        RoundSessions.TryRemove(steamId, out _);

        if (OnlinePlayers.TryRemove(steamId, out var bpPlayer))
            Task.Run(async () =>
            {
                if (_dataBaseService == null) return;
                await _dataBaseService.SavePlayerAsync(bpPlayer);
            });

        return HookResult.Continue;
    }

    private HookResult OnRoundEnd(EventRoundEnd @event, GameEventInfo info)
    {
        var players = Utilities.GetPlayers();
        var currentPlayersCount = CountHumanPlayers();
        var minPlayers = Config.MinPlayers;

        foreach (var player in players)
        {
            if (!player.IsValid || player.IsBot || player.IsHLTV || player.AuthorizedSteamID == null)
                continue;

            var steamId = player.AuthorizedSteamID.SteamId64;

            if (!RoundSessions.TryGetValue(steamId, out var roundData) ||
                !OnlinePlayers.TryGetValue(steamId, out var bpPlayer))
                continue;

            if (roundData.LowPlayerCountWarning)
            {
                player.PrintToChat($" {Prefix} {Localizer["battlepass.error.min_players", currentPlayersCount, minPlayers]}");
            }

            if (roundData.ProgressDeltas.Count > 0)
            {
                player.PrintToChat($" {Prefix} {Localizer["battlepass.round_summary.title"]}");

                foreach (var (taskId, delta) in roundData.ProgressDeltas)
                {
                    var taskSettings = GetTaskSettingsById(taskId);
                    if (taskSettings == null) continue;

                    var need = bpPlayer.IsPremium && taskSettings.NeedPremium > 0 ? taskSettings.NeedPremium : taskSettings.Need;
                    var totalProgress = bpPlayer.MissionProgress.TryGetValue(taskId, out var current) ? current : 0;

                    if (totalProgress >= need)
                    {
                        var reward = bpPlayer.IsPremium && taskSettings.XpPremium > 0 ? taskSettings.XpPremium : taskSettings.Xp;
                        player.PrintToChat($" {Localizer["battlepass.tasks.completed", taskSettings.Name, reward]}");
                        Logger.LogInformation("[BattlePass] Player {Name} completed task {TaskId} (+{Reward} XP)", player.PlayerName, taskId, reward);
                    }
                    else
                    {
                        player.PrintToChat($" {Localizer["battlepass.tasks.round_progress", taskSettings.Name, delta, totalProgress, need]}");
                    }
                }

                CheckLevelUp(bpPlayer, player);

                Task.Run(async () =>
                {
                    if (_dataBaseService != null) await _dataBaseService.SavePlayerAsync(bpPlayer);
                });
            }

            roundData.ProgressDeltas.Clear();
            roundData.LowPlayerCountWarning = false;
        }

        return HookResult.Continue;
    }

    private TaskSettings? GetTaskSettingsById(string taskId)
    {
        foreach (var tasks in Config.Tasks.Values.Select(group => group.GetActualTasks()))
        {
            if (tasks.TryGetValue(taskId, out var settings)) return settings;
        }

        return null;
    }

    private async Task CheckAndRotateTasks()
    {
        if (_dataBaseService == null) return;

        var tasksChanged = false;

        foreach (var (key, group) in Config.Tasks)
        {
            var durationKeyStr = key.ToString();
            if (!long.TryParse(durationKeyStr, out var durationSeconds)) continue;

            var rotation = await _dataBaseService.GetRotationAsync(durationKeyStr);
            var now = DateTime.UtcNow;

            if (rotation != null && now < rotation.ExpireAt) continue;
            var allTaskIds = group.GetActualTasks().Keys.ToList();
            var currentTaskIds = allTaskIds.OrderBy(_ => Guid.NewGuid()).Take(group.CountGive).ToList();

            var newExpire = now.AddSeconds(durationSeconds);

            await _dataBaseService.SaveRotationAsync(durationKeyStr, newExpire, currentTaskIds);

            if (group.ResetProgress && currentTaskIds.Count > 0)
            {
                await _dataBaseService.ResetMissionsProgressAsync(currentTaskIds);
                Server.NextFrame(() => ResetOnlinePlayersProgress(currentTaskIds));
            }

            Server.NextFrame(() => AnnounceNewTasks(group.GroupName, currentTaskIds, group));

            tasksChanged = true;
            Logger.LogInformation("[BattlePass] Rotated group '{GroupGroupName}'. ResetProgress: {GroupResetProgress}", group.GroupName, group.ResetProgress);
        }

        if (tasksChanged || _activeTaskIds.IsEmpty)
        {
            await RefreshActiveTaskCache();
        }
    }

    private static void ResetOnlinePlayersProgress(List<string> taskIds)
    {
        foreach (var player in OnlinePlayers.Values)
        {
            foreach (var taskId in taskIds.Where(taskId => player.MissionProgress.ContainsKey(taskId)))
            {
                player.MissionProgress[taskId] = 0;
            }
        }
    }

    private async Task RefreshActiveTaskCache()
    {
        var tempSet = new HashSet<string>();
        foreach (var groupEntry in Config.Tasks)
        {
            var rotation = await _dataBaseService!.GetRotationAsync(groupEntry.Key.ToString());
            if (rotation == null) continue;
            foreach (var taskId in rotation.ActiveTasks)
            {
                tempSet.Add(taskId);
            }
        }

        _activeTaskIds.Clear();
        foreach (var t in tempSet) _activeTaskIds.TryAdd(t, 0);
    }

    private void AnnounceNewTasks(string groupName, List<string> taskIds, TaskGroup group)
    {
        var tasks = group.GetActualTasks();

        Server.PrintToChatAll($" {Prefix} {Localizer["battlepass.tasks.update", groupName]}");

        foreach (var id in taskIds)
        {
            if (tasks.TryGetValue(id, out var tSettings))
            {
                Server.PrintToChatAll($" {Localizer["battlepass.tasks.list_item", tSettings.Name, tSettings.Description]}");
            }
        }
    }

    private void InitTaskEvents()
    {
        _tasksByEvent.Clear();
        var uniqueEvents = new HashSet<string>();

        foreach (var group in Config.Tasks.Values)
        foreach (var (taskId, task) in group.GetActualTasks())
        {
            if (string.IsNullOrEmpty(task.Event)) continue;

            if (!_tasksByEvent.TryGetValue(task.Event, out var value))
            {
                value = [];
                _tasksByEvent[task.Event] = value;
            }

            value.Add((taskId, task));
            uniqueEvents.Add(task.Event);
        }

        foreach (var eventName in uniqueEvents) RegisterDynamicEventHandler(eventName);
    }

    private void RegisterDynamicEventHandler(string eventName)
    {
        try
        {
            var pascalName = ToPascalCase(eventName);
            var className = $"Event{pascalName}";

            var assembly = typeof(GameEvent).Assembly;
            var eventType = assembly.GetTypes()
                .FirstOrDefault(t =>
                    t.Name.Equals(className, StringComparison.OrdinalIgnoreCase) &&
                    typeof(GameEvent).IsAssignableFrom(t));

            if (eventType == null)
            {
                Logger.LogError("[BattlePass] Event type '{ClassName}' not found for config event '{EventName}'",
                    className, eventName);
                return;
            }

            var handlerMethodInfo = GetType()
                .GetMethod(nameof(GenericEventHandler), BindingFlags.Public | BindingFlags.Instance);
            if (handlerMethodInfo == null)
            {
                Logger.LogError($"[BattlePass] Method '{nameof(GenericEventHandler)}' not found or is not public.");
                return;
            }

            var genericHandler = handlerMethodInfo.MakeGenericMethod(eventType);

            var delegateType = typeof(GameEventHandler<>).MakeGenericType(eventType);
            var handlerDelegate = Delegate.CreateDelegate(delegateType, this, genericHandler);

            var registerMethodInfo = typeof(BasePlugin)
                .GetMethods(BindingFlags.Public | BindingFlags.Instance)
                .FirstOrDefault(m =>
                    m is { Name: "RegisterEventHandler", IsGenericMethod: true } &&
                    m.GetParameters().Length == 2
                );

            if (registerMethodInfo == null)
            {
                Logger.LogError("[BattlePass] Could not find RegisterEventHandler method in BasePlugin.");
                return;
            }

            var genericRegister = registerMethodInfo.MakeGenericMethod(eventType);

            genericRegister.Invoke(this, [handlerDelegate, HookMode.Post]);

            Logger.LogInformation("[BattlePass] Successfully hooked event: {EventName} ({ClassName})", eventName,
                className);
        }
        catch (Exception ex)
        {
            Logger.LogError("[BattlePass] Failed to register event {EventName}: {ExMessage} | {InnerExceptionMessage}",
                eventName, ex.Message, ex.InnerException?.Message);
        }
    }

    public HookResult GenericEventHandler<T>(T @event, GameEventInfo info) where T : GameEvent
    {
        var eventName = @event.EventName;

        if (!_tasksByEvent.TryGetValue(eventName, out var tasks)) return HookResult.Continue;

        foreach (var (taskId, task) in tasks)
            try
            {
                ProcessTask(@event, task, taskId);
            }
            catch (Exception ex)
            {
                Logger.LogError("[BattlePass] Error processing task {TaskName}: {ExMessage}", task.Name, ex.Message);
            }

        return HookResult.Continue;
    }

    private void ProcessTask<T>(T @event, TaskSettings task, string taskId) where T : GameEvent
    {
        CCSPlayerController? player = null;

        foreach (var (paramName, requiredValueRaw) in task.EventParams)
        {
            if (!_activeTaskIds.ContainsKey(taskId)) return;
            var rawStr = GetRawString(requiredValueRaw);

            if (rawStr != "{userid}") continue;
            var prop = typeof(T).GetProperty(paramName,
                BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);
            if (prop == null) continue;
            var val = prop.GetValue(@event);
            if (val is CCSPlayerController p) player = p;
        }

        if (player == null || !player.IsValid || player.IsBot || player.AuthorizedSteamID == null) return;

        var steamId = player.AuthorizedSteamID.SteamId64;

        if (!OnlinePlayers.TryGetValue(steamId, out var bpPlayer)) return;

        foreach (var (paramName, requiredValueRaw) in task.EventParams)
        {
            var rawStr = GetRawString(requiredValueRaw);

            if (rawStr == "{userid}") continue;

            var isNegated = false;
            var targetValue = rawStr;

            if (targetValue.StartsWith('!'))
            {
                isNegated = true;
                targetValue = targetValue[1..];
            }

            var prop = typeof(T).GetProperty(paramName,
                BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);
            if (prop == null) return;
            var actualValue = prop.GetValue(@event);

            if (targetValue == "{userid}")
            {
                var actualController = actualValue as CCSPlayerController;

                var isSamePlayer = false;

                if (actualController != null && actualController.IsValid)
                    isSamePlayer = actualController.Index == player.Index;

                switch (isNegated)
                {
                    case true when isSamePlayer:
                    case false when !isSamePlayer:
                        return;
                    default:
                        continue;
                }
            }

            var matches = CompareValues(actualValue, targetValue);

            if (isNegated)
            {
                if (matches) return;
            }
            else
            {
                if (!matches) return;
            }
        }

        if (CountHumanPlayers() < Config.MinPlayers)
        {
            if (RoundSessions.TryGetValue(steamId, out var roundData))
            {
                roundData.LowPlayerCountWarning = true;
            }

            return;
        }

        AddTaskProgress(player, bpPlayer, taskId, task);
    }

    private static int CountHumanPlayers()
    {
        return Utilities.GetPlayers().Count(p => p is
            { IsValid: true, IsBot: false, IsHLTV: false, Connected: PlayerConnectedState.PlayerConnected });
    }

    private static string GetRawString(object raw)
    {
        if (raw is JsonElement jsonElem)
            return jsonElem.ValueKind == JsonValueKind.String
                ? jsonElem.GetString() ?? ""
                : jsonElem.GetRawText();

        return raw.ToString() ?? "";
    }

    private static bool CompareValues(object? actual, string required)
    {
        if (actual == null) return required == "null";

        var actualStr = actual.ToString() ?? "";

        if (actual is bool boolVal)
        {
            if (bool.TryParse(required, out var reqBool)) return boolVal == reqBool;
            return required switch
            {
                "1" => boolVal,
                "0" => !boolVal,
                _ => string.Equals(actualStr, required, StringComparison.OrdinalIgnoreCase)
            };
        }

        if (actual is not string) return string.Equals(actualStr, required, StringComparison.OrdinalIgnoreCase);

        if (string.Equals(actualStr, required, StringComparison.OrdinalIgnoreCase)) return true;

        if (actualStr.Contains(required, StringComparison.OrdinalIgnoreCase)) return true;
        return required.Contains(actualStr, StringComparison.OrdinalIgnoreCase) && actualStr.Length > 1;
    }

    private static void AddTaskProgress(CCSPlayerController player, BattlePassPlayer bpPlayer, string taskId,
        TaskSettings task)
    {
        if (task.PremiumPass && !bpPlayer.IsPremium) 
            return;
        
        var currentProgress = bpPlayer.MissionProgress.TryGetValue(taskId, out var p) ? p : 0;
        var need = bpPlayer.IsPremium && task.NeedPremium > 0 ? task.NeedPremium : task.Need;

        if (currentProgress >= need) return;

        currentProgress++;
        bpPlayer.MissionProgress[taskId] = currentProgress;

        if (currentProgress >= need)
        {
            var reward = bpPlayer.IsPremium && task.XpPremium > 0 ? task.XpPremium : task.Xp;
            bpPlayer.Xp += reward;
        }

        if (player.AuthorizedSteamID == null || !RoundSessions.TryGetValue(player.AuthorizedSteamID.SteamId64, out var roundData)) return;
        roundData.ProgressDeltas.TryAdd(taskId, 0);

        roundData.ProgressDeltas[taskId]++;
    }

    private void CheckLevelUp(BattlePassPlayer bpPlayer, CCSPlayerController? player = null)
    {
        while (true)
        {
            var nextLevel = bpPlayer.Level + 1;

            if (!Config.Levels.TryGetValue(nextLevel, out var levelSettings)) break;

            if (bpPlayer.Xp < levelSettings.Xp) break;

            bpPlayer.Level = nextLevel;

            Logger.LogInformation("[BattlePass] Player {BpPlayerSteamId64} leveled up to {NextLevel}!",
                bpPlayer.SteamId64, nextLevel);

            if (player != null && player.IsValid)
                player.PrintToChat($" {Prefix} {Localizer["battlepass.levelup", nextLevel]}");
        }
    }

    private static string ToPascalCase(string str)
    {
        if (string.IsNullOrEmpty(str)) return str;
        return string.Join("", str.Split('_').Select(s =>
            s.Length > 0 ? char.ToUpper(s[0]) + s[1..] : ""));
    }

    public override void OnAllPluginsLoaded(bool hotReload)
    {
        _menuApi = MenuCapability.Get()!;
    }

    private void MainMenu(CCSPlayerController? player)
    {
        if (player == null || !player.IsValid || player.AuthorizedSteamID == null) return;

        if (!OnlinePlayers.TryGetValue(player.AuthorizedSteamID.SteamId64, out var bpPlayer))
        {
            player.PrintToChat($" {Prefix} {Localizer["battlepass.main_menu.loading"]}");
            return;
        }

        var menu = _menuApi.GetMenu(Localizer["battlepass.main_menu.title"]);

        var passType = bpPlayer.IsPremium
            ? Localizer["battlepass.main_menu.pass_premium"]
            : Localizer["battlepass.main_menu.pass_free"];

        menu.AddMenuOption(Localizer["battlepass.main_menu.xp", bpPlayer.Xp], (_, _) => { }, true);
        menu.AddMenuOption(Localizer["battlepass.main_menu.lvl", bpPlayer.Level], (_, _) => { }, true);
        menu.AddMenuOption(passType, (_, _) => { }, true);
        menu.AddMenuOption(Localizer["battlepass.main_menu.tasks"], (p, _) => OpenMenuTasks(p));
        menu.AddMenuOption(Localizer["battlepass.main_menu.lvls"], (p, _) => OpenMenuLvls(p));

        menu.Open(player);
    }

    private void OpenMenuTasks(CCSPlayerController player)
    {
        var menu = _menuApi.GetMenu(Localizer["battlepass.tasks_menu.title"], MainMenu);

        foreach (var group in Config.Tasks.Select(taskGroupEntry => taskGroupEntry.Value))
        {
            var groupTasks = group.GetActualTasks();
            var hasActiveTasks = groupTasks.Keys.Any(taskId => _activeTaskIds.ContainsKey(taskId));

            if (hasActiveTasks)
            {
                menu.AddMenuOption(group.GroupName, (p, _) => OpenMenuTaskGroup(p, group));
            }
        }

        menu.Open(player);
    }

    private void OpenMenuTaskGroup(CCSPlayerController player, TaskGroup group)
    {
        var menu = _menuApi.GetMenu(group.GroupName, OpenMenuTasks);
        var actualTasks = group.GetActualTasks();

        foreach (var (taskId, taskSettings) in actualTasks)
        {
            if (!_activeTaskIds.ContainsKey(taskId))
                continue;

            menu.AddMenuOption(taskSettings.Name, (p, _) => OpenMenuTaskDetail(p, taskId, taskSettings, group));
        }

        menu.Open(player);
    }

    private void OpenMenuTaskDetail(CCSPlayerController player, string taskId, TaskSettings task, TaskGroup group)
    {
        if (player.AuthorizedSteamID == null ||
            !OnlinePlayers.TryGetValue(player.AuthorizedSteamID.SteamId64, out var bpPlayer)) return;

        var menu = _menuApi.GetMenu(task.Name, p => OpenMenuTaskGroup(p, group));

        var hasPremium = bpPlayer.IsPremium;
        var need = hasPremium && task.NeedPremium > 0 ? task.NeedPremium : task.Need;
        var xpReward = hasPremium && task.XpPremium > 0 ? task.XpPremium : task.Xp;
        var desc = hasPremium && !string.IsNullOrEmpty(task.DescriptionPremium)
            ? task.DescriptionPremium
            : task.Description;

        var currentProgress = bpPlayer.MissionProgress.TryGetValue(taskId, out var prog) ? prog : 0;
        var isCompleted = currentProgress >= need;

        menu.AddMenuOption(Localizer["battlepass.task_detail.button_details"], (p, _) =>
        {
            p.PrintToChat($" {Prefix} {Localizer["battlepass.task_detail.chat_name", task.Name]}");
            p.PrintToChat($" {Prefix} {Localizer["battlepass.task_detail.chat_desc", desc]}");
        });

        var statusText = isCompleted
            ? Localizer["battlepass.task_detail.status_completed"]
            : Localizer["battlepass.task_detail.status_not_completed"];
        menu.AddMenuOption(Localizer["battlepass.task_detail.status", statusText], (_, _) => { }, true);

        var displayProgress = currentProgress > need ? need : currentProgress;
        menu.AddMenuOption(Localizer["battlepass.task_detail.progress_menu", displayProgress, need], (_, _) => { }, true);

        if (task.PremiumPass && !hasPremium)
        {
            menu.AddMenuOption(Localizer["battlepass.task_detail.req_premium"],
                (_, _) => { player.PrintToChat($" {Prefix} {Localizer["battlepass.task_detail.req_premium_chat"]}"); },
                true);
        }
        else
        {
            var rewardText = isCompleted
                ? Localizer["battlepass.task_detail.reward_claimed", xpReward]
                : Localizer["battlepass.task_detail.reward", xpReward];
            menu.AddMenuOption(rewardText, (_, _) => { }, true);
        }

        menu.Open(player);
    }

    private void OpenMenuLvls(CCSPlayerController player)
    {
        var menu = _menuApi.GetMenu(Localizer["battlepass.lvls_menu.title"], MainMenu);

        var sortedLevels = Config.Levels.OrderBy(x => x.Key);

        if (player.AuthorizedSteamID == null ||
            !OnlinePlayers.TryGetValue(player.AuthorizedSteamID.SteamId64, out var bpPlayer)) return;

        foreach (var (lvlId, settings) in sortedLevels)
        {
            string menuItemText;
            if (bpPlayer.Level >= lvlId)
            {
                var normClaimed = bpPlayer.ClaimedNormalRewards.Contains(lvlId);
                var premClaimed = bpPlayer.ClaimedPremiumRewards.Contains(lvlId);

                if (normClaimed && (premClaimed || !bpPlayer.IsPremium))
                    menuItemText = Localizer["battlepass.lvls_menu.item_claimed", lvlId];
                else
                    menuItemText = Localizer["battlepass.lvls_menu.item_unclaimed", lvlId];
            }
            else
            {
                menuItemText = Localizer["battlepass.lvls_menu.item_locked", lvlId];
            }

            menu.AddMenuOption(menuItemText, (p, _) => OpenMenuLevelDetail(p, lvlId, settings));
        }

        menu.Open(player);
    }

    private void OpenMenuLevelDetail(CCSPlayerController player, int levelId, LevelSettings settings)
    {
        if (player.AuthorizedSteamID == null ||
            !OnlinePlayers.TryGetValue(player.AuthorizedSteamID.SteamId64, out var bpPlayer)) return;

        var menu = _menuApi.GetMenu(Localizer["battlepass.main_menu.lvl", levelId], OpenMenuLvls);

        menu.AddMenuOption(Localizer["battlepass.lvl.xp_req", settings.Xp], (_, _) => { }, true);

        if (!string.IsNullOrEmpty(settings.Item) && !bpPlayer.IsPremium)
            menu.AddMenuOption(Localizer["battlepass.lvl.reward", settings.Item], (_, _) => { }, true);

        if (!string.IsNullOrEmpty(settings.ItemPremium))
            menu.AddMenuOption(Localizer["battlepass.lvl.reward_premium", settings.ItemPremium], (_, _) => { }, true);

        var levelReached = bpPlayer.Level >= levelId;

        if (bpPlayer.ClaimedNormalRewards.Contains(levelId) && !bpPlayer.IsPremium)
        {
            menu.AddMenuOption(Localizer["battlepass.lvl.status_claimed"], (_, _) => { }, true);
        }
        else
        {
            if (!bpPlayer.IsPremium)
            {
                var btnText = levelReached
                    ? Localizer["battlepass.lvl.btn_claim"]
                    : Localizer["battlepass.lvl.btn_unavailable"];

                menu.AddMenuOption(btnText, (p, _) =>
                {
                    if (TryClaimReward(p, bpPlayer, levelId, settings, false)) OpenMenuLevelDetail(p, levelId, settings);
                }, !levelReached);
            }
        }

        if (bpPlayer.ClaimedPremiumRewards.Contains(levelId))
        {
            menu.AddMenuOption(Localizer["battlepass.lvl.status_claimed_premium"], (_, _) => { }, true);
        }
        else
        {
            if (!bpPlayer.IsPremium)
            {
                menu.AddMenuOption(Localizer["battlepass.lvl.btn_buy_premium"],
                    (_, _) =>
                    {
                        player.PrintToChat($" {Prefix} {Localizer["battlepass.lvl.chat_only_premium"]}");
                    });
            }
            else
            {
                var btnText = levelReached
                    ? Localizer["battlepass.lvl.btn_claim_premium"]
                    : Localizer["battlepass.lvl.btn_premium_unavailable"];

                menu.AddMenuOption(btnText, (p, _) =>
                {
                    if (TryClaimReward(p, bpPlayer, levelId, settings, true)) OpenMenuLevelDetail(p, levelId, settings);
                }, !levelReached);
            }
        }

        menu.Open(player);
    }

    private bool TryClaimReward(CCSPlayerController player, BattlePassPlayer bpPlayer, int levelId,
        LevelSettings settings, bool isPremiumReward)
    {
        if (bpPlayer.Level < levelId)
        {
            player.PrintToChat($" {Prefix} {Localizer["battlepass.claim.not_reached"]}");
            return false;
        }

        switch (isPremiumReward)
        {
            case true when !bpPlayer.IsPremium:
                player.PrintToChat($" {Prefix} {Localizer["battlepass.claim.need_premium"]}");
                return false;
            case true:
            {
                if (bpPlayer.ClaimedPremiumRewards.Contains(levelId)) return false;
                break;
            }
            default:
            {
                if (bpPlayer.ClaimedNormalRewards.Contains(levelId)) return false;
                break;
            }
        }

        var command = isPremiumReward ? settings.CommandPremium : settings.Command;
        var itemName = isPremiumReward ? settings.ItemPremium : settings.Item;

        if (string.IsNullOrEmpty(command))
        {
            MarkAsClaimed(bpPlayer, levelId, isPremiumReward);
            return true;
        }

        var authId = player.AuthorizedSteamID;

        var finalCommand = command
            .Replace("{steamid}", authId?.SteamId64.ToString() ?? "0")
            .Replace("{steamid2}", authId?.SteamId2 ?? "0")
            .Replace("{steamid3}", authId?.SteamId3 ?? "0")
            .Replace("{name}", player.PlayerName)
            .Replace("{userid}", player.UserId.ToString());

        try
        {
            Server.ExecuteCommand(finalCommand);
            MarkAsClaimed(bpPlayer, levelId, isPremiumReward);

            player.PrintToChat($" {Prefix} {Localizer["battlepass.claim.success", itemName ?? ""]}");
            Logger.LogInformation(
                "[BattlePass] Player {PlayerPlayerName} claimed Level {LevelId} reward ({ItemName}). Cmd: {FinalCommand}",
                player.PlayerName, levelId, itemName, finalCommand);

            Task.Run(async () =>
            {
                if (_dataBaseService != null) await _dataBaseService.SavePlayerAsync(bpPlayer);
            });

            return true;
        }
        catch (Exception ex)
        {
            Logger.LogError("[BattlePass] Failed to execute reward command: {FinalCommand}. Error: {ExMessage}",
                finalCommand, ex.Message);
            player.PrintToChat($" {Prefix} {Localizer["battlepass.claim.error"]}");
            return false;
        }
    }

    private static void MarkAsClaimed(BattlePassPlayer player, int levelId, bool isPremium)
    {
        if (isPremium)
            player.ClaimedPremiumRewards.Add(levelId);
        else
            player.ClaimedNormalRewards.Add(levelId);
    }

    private void CmdGivePremium(CCSPlayerController? player, CommandInfo info)
    {
        if (player != null) return;

        if (info.ArgCount < 3)
        {
            info.ReplyToCommand($"{Prefix} {Localizer["battlepass.admin.usage_give_premium"]}");
            return;
        }

        if (!ulong.TryParse(info.GetArg(1), out var targetSteamId))
        {
            info.ReplyToCommand($"{Prefix} {Localizer["battlepass.admin.invalid_steamid"]}");
            return;
        }

        if (!int.TryParse(info.GetArg(2), out var seconds))
        {
            info.ReplyToCommand($"{Prefix} {Localizer["battlepass.admin.invalid_args"]}");
            return;
        }

        ProcessOfflineOrOnlinePlayer(targetSteamId, bpPlayer =>
        {
            bpPlayer.IsPremium = true;
            if (seconds > 0)
                bpPlayer.PremiumExpireDate = DateTime.UtcNow.AddSeconds(seconds);
            else
                bpPlayer.PremiumExpireDate = null;

            var timeStr = seconds > 0 ? $"{seconds} min" : "FOREVER";
            Logger.LogInformation("[BattlePass] Admin gave premium to {SteamID}. Duration: {Duration}", targetSteamId,
                timeStr);
        });

        info.ReplyToCommand($"{Prefix} {Localizer["battlepass.admin.success"]}");
    }

    private void CmdTakePremium(CCSPlayerController? player, CommandInfo info)
    {
        if (player != null) return;

        if (info.ArgCount < 2)
        {
            info.ReplyToCommand($"{Prefix} {Localizer["battlepass.admin.usage_take_premium"]}");
            return;
        }

        if (!ulong.TryParse(info.GetArg(1), out var targetSteamId))
        {
            info.ReplyToCommand($"{Prefix} {Localizer["battlepass.admin.invalid_steamid"]}");
            return;
        }

        ProcessOfflineOrOnlinePlayer(targetSteamId, bpPlayer =>
        {
            bpPlayer.IsPremium = false;
            bpPlayer.PremiumExpireDate = null;
            Logger.LogInformation("[BattlePass] Admin took premium from {SteamID}", targetSteamId);
        });

        info.ReplyToCommand($"{Prefix} {Localizer["battlepass.admin.success"]}");
    }

    private void CmdGiveXp(CCSPlayerController? player, CommandInfo info)
    {
        if (player != null) return;

        if (info.ArgCount < 3)
        {
            info.ReplyToCommand($"{Prefix} {Localizer["battlepass.admin.usage_give_xp"]}");
            return;
        }

        if (!ulong.TryParse(info.GetArg(1), out var targetSteamId) || !int.TryParse(info.GetArg(2), out var amount))
        {
            info.ReplyToCommand($"{Prefix} {Localizer["battlepass.admin.invalid_args"]}");
            return;
        }

        ProcessOfflineOrOnlinePlayer(targetSteamId, bpPlayer =>
        {
            bpPlayer.Xp += amount;
            Logger.LogInformation("[BattlePass] Admin gave {Amount} XP to {SteamID}. Total: {TotalXP}", amount,
                targetSteamId, bpPlayer.Xp);
            var onlineTarget = Utilities.GetPlayers()
                .FirstOrDefault(p => p.AuthorizedSteamID?.SteamId64 == targetSteamId);
            CheckLevelUp(bpPlayer, onlineTarget);
        });

        info.ReplyToCommand($"{Prefix} {Localizer["battlepass.admin.success"]}");
    }

    private void CmdTakeXp(CCSPlayerController? player, CommandInfo info)
    {
        if (player != null) return;

        if (info.ArgCount < 3)
        {
            info.ReplyToCommand($"{Prefix} {Localizer["battlepass.admin.usage_take_xp"]}");
            return;
        }

        if (!ulong.TryParse(info.GetArg(1), out var targetSteamId) || !int.TryParse(info.GetArg(2), out var amount))
        {
            info.ReplyToCommand($"{Prefix} {Localizer["battlepass.admin.invalid_args"]}");
            return;
        }

        ProcessOfflineOrOnlinePlayer(targetSteamId, bpPlayer =>
        {
            bpPlayer.Xp -= amount;
            if (bpPlayer.Xp < 0) bpPlayer.Xp = 0;

            Logger.LogInformation("[BattlePass] Admin took {Amount} XP from {SteamID}. Total: {TotalXP}", amount,
                targetSteamId, bpPlayer.Xp);
        });

        info.ReplyToCommand($"{Prefix} {Localizer["battlepass.admin.success"]}");
    }

    private void ProcessOfflineOrOnlinePlayer(ulong steamId, Action<BattlePassPlayer> action)
    {
        if (OnlinePlayers.TryGetValue(steamId, out var onlinePlayer))
        {
            action(onlinePlayer);
            Task.Run(async () =>
            {
                if (_dataBaseService != null)
                    await _dataBaseService.SavePlayerAsync(onlinePlayer);
            });
            return;
        }

        Task.Run(async () =>
        {
            if (_dataBaseService == null) return;
            try
            {
                var offlinePlayer = await _dataBaseService.LoadPlayerAsync(steamId, Config.StartLevel, Config.StartXp);
                action(offlinePlayer);
                await _dataBaseService.SavePlayerAsync(offlinePlayer);
            }
            catch (Exception ex)
            {
                Logger.LogError("[BattlePass] Failed to process offline player {SteamId}: {Ex}", steamId, ex.Message);
            }
        });
    }
}

public class PlayerRoundData
{
    public Dictionary<string, int> ProgressDeltas { get; set; } = new();
    public bool LowPlayerCountWarning { get; set; }
}