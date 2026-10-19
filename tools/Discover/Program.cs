using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using ProxyNodeHub;
using ProxyNodeHub.Web;

namespace ProxyNodeHub.Discover;

/// <summary>
/// 无头发现入口：等价于 POST /api/explore 一路跑到完成，然后发布合并去重后的订阅。
///
/// 只读引用 core/ 与 web/，不改动其源码。core 的发现循环入口 DiscoveryWorker.DiscoverAsync
/// 是 internal（仅对 ProxyNodeHub.Tests 可见），因此这里用 core 的公开 API 复刻同一流程。
///
/// 路径约定（相对仓库根目录）：
///   state/    搜索历史、特征库、上一轮快照（StateStore + FeatureLibrary，两者构造函数都接受目录）
///   output/subscriptions.txt  合并去重后的订阅文本，一行一个节点
///
/// GitHub Token：读环境变量 PROXYNODEHUB_PAT。未设置则匿名运行——
/// 匿名搜索配额远低于带 Token（搜索接口 10 次/分钟且共享 IP 池），实际跑不完一轮完整发现，
/// 正式部署必须配置该变量。
/// </summary>
internal static class Program
{
    private static readonly object LogGate = new();
    private static readonly List<LogEntry> Entries = new();

    private static void Log(string message)
    {
        var entry = new LogEntry(DateTimeOffset.UtcNow, message);
        lock (LogGate) Entries.Add(entry);
        Console.WriteLine($"[{entry.At:yyyy-MM-dd HH:mm:ss}] {message}");
    }

    private static LogEntry[] SnapshotLogs()
    {
        lock (LogGate) return Entries.ToArray();
    }

    private static async Task<int> Main()
    {
        try
        {
            await RunAsync();
            return 0;
        }
        catch (Exception ex)
        {
            // 只打印异常本身；Token 从不进入日志或异常文本。
            Console.Error.WriteLine($"[失败] {ex.GetType().Name}: {ex.Message}");
            if (ex.StackTrace is not null) Console.Error.WriteLine(ex.StackTrace);
            return 1;
        }
    }

    private static async Task RunAsync()
    {
        var root = FindRepoRoot();
        var stateDir = Path.Combine(root, "state");
        var outputPath = Path.Combine(root, "output", "subscriptions.txt");
        Directory.CreateDirectory(stateDir);

        var token = Environment.GetEnvironmentVariable("PROXYNODEHUB_PAT");
        if (string.IsNullOrWhiteSpace(token))
        {
            // 匿名模式只够演示：搜索配额不足，跑完整轮发现大概率中途被限流。
            token = null;
            Log("未设置 PROXYNODEHUB_PAT，按匿名模式运行；匿名搜索配额不足以支撑一轮完整发现。");
        }
        else
        {
            Log("已读取 PROXYNODEHUB_PAT（值不打印）。");
        }

        var store = new StateStore(stateDir, new DiscoverySettings());
        var features = new FeatureLibrary(stateDir);
        var started = DateTimeOffset.UtcNow;
        var searched = new ConcurrentBag<string>();
        DiscoverySnapshot? snapshot = null;
        string? error = null;

        using var github = new GitHubService(token, restrictPublicSources: true);
        try
        {
            var settings = store.State.Settings;
            await github.ConfigurePublicMirrorAsync(settings.DownloadMirror, CancellationToken.None);
            var exclusions = StateStore.ExplorationExclusions(store.State, started);

            // 与 Web 后台任务相同的 15 分钟预算，避免 CI 上无限挂起。
            using var budget = new CancellationTokenSource(TimeSpan.FromMinutes(15));
            snapshot = await DiscoverAsync(github, settings, features, exclusions, searched.Add, budget.Token);
            Log($"发现完成：{snapshot.Repositories.Length} 个仓库，{snapshot.Subscriptions.Length} 条有效订阅。");
            if (snapshot.FailedSearches + snapshot.FailedRepositories + snapshot.FailedDownloads > 0)
                Log($"部分来源失败：搜索 {snapshot.FailedSearches}，仓库 {snapshot.FailedRepositories}，下载 {snapshot.FailedDownloads}；发布本轮验证通过的结果。");
        }
        catch (OperationCanceledException)
        {
            error = "任务已取消或超过时限，保留上次结果。";
        }
        catch (Exception ex)
        {
            error = ex is InvalidDataException ? ex.Message : $"更新失败：{ex.GetType().Name}，保留上次结果。";
        }

        if (error is not null) Log(error);
        var finished = DateTimeOffset.UtcNow;
        store.FinishRun(snapshot,
            new RunHistory(started, finished, snapshot is not null, snapshot?.Subscriptions.Length ?? 0, error, SnapshotLogs()),
            searched.ToArray(), merge: true);
        if (snapshot is null) throw new InvalidOperationException(error ?? "本轮未产生有效快照。");

        var urls = store.State.Current?.Subscriptions ?? [];
        if (urls.Length == 0) throw new InvalidDataException("没有可发布的订阅。");

        using var exportBudget = new CancellationTokenSource(TimeSpan.FromMinutes(10));
        var merged = await MergeSubscriptionsAsync(github, urls, exportBudget.Token);
        Directory.CreateDirectory(Path.GetDirectoryName(outputPath)!);
        AtomicFile.Write(outputPath, merged);
        Log($"已发布 {outputPath}（{merged.Split('\n').Length - 1} 行节点）。");
    }

    /// <summary>
    /// 复刻 core/SubscriptionFinder + DiscoveryEngine 的一轮完整发现（等价 POST /api/explore）。
    /// </summary>
    private static async Task<DiscoverySnapshot> DiscoverAsync(GitHubService github, DiscoverySettings settings,
        FeatureLibrary features, HashSet<string> exclusions, Action<string> analyzed, CancellationToken ct)
    {
        var found = new Dictionary<string, GitHubRepo>(StringComparer.OrdinalIgnoreCase);
        var failedSearches = 0;
        foreach (var query in DiscoveryEngine.Queries(settings.InactiveDays))
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                foreach (var repo in await github.SearchReposAsync(query, 30, ct))
                    if (!repo.Fork && !GitHubAnalyzer.IsStale(repo) && !exclusions.Contains(repo.FullName))
                        found.TryAdd(repo.FullName, repo);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
            catch (Exception ex) when (ex is HttpRequestException or JsonException or OperationCanceledException)
            {
                failedSearches++;
                Log($"搜索请求失败：{ex.GetType().Name}。");
            }
            // 搜索接口有独立的每分钟预算，即使带 Token 也要限速。
            await Task.Delay(TimeSpan.FromSeconds(1), ct);
        }
        if (found.Count == 0)
            throw new InvalidDataException(exclusions.Count > 0
                ? "排除已分析或已收藏仓库后没有新来源；可重置搜索记忆或执行普通刷新。保留上次结果。"
                : "没有发现仓库，请检查网络或 GitHub API 限额；保留上次结果。");
        Log($"搜索命中 {found.Count} 个候选仓库。");

        var candidates = found.Values.Take(settings.RepoCount).Select(GitHubAnalyzer.ToRepoInfo).ToArray();
        var (repos, failed) = await AnalyzeAsync(github, candidates, settings, features, analyzed, ct);
        if (repos.Length == 0) throw new InvalidDataException("本轮没有验证通过的订阅，保留上次有效结果。");
        var urls = repos.SelectMany(r => r.Links).Select(l => l.Url).Distinct(StringComparer.Ordinal).ToArray();
        if (urls.Length == 0) throw new InvalidDataException("本轮没有验证通过的订阅，保留上次有效结果。");
        return new(DateTimeOffset.UtcNow, repos, urls, failedSearches, failed, github.FailedDownloads);
    }

    private static async Task<(RepoInfo[] Repositories, int Failed)> AnalyzeAsync(GitHubService github,
        RepoInfo[] candidates, DiscoverySettings settings, FeatureLibrary features, Action<string> analyzed,
        CancellationToken ct)
    {
        settings.Validate();
        using var limit = new SemaphoreSlim(settings.AnalysisConcurrency, settings.AnalysisConcurrency);
        var failed = 0;
        var completed = 0;
        var results = await Task.WhenAll(candidates.Select(async original =>
        {
            await limit.WaitAsync(ct);
            try
            {
                // 分析会就地修改模型：不能直接改已发布的快照/收藏实例。
                var repo = JsonSerializer.Deserialize<RepoInfo>(
                    JsonSerializer.Serialize(original, StateStore.JsonOptions), StateStore.JsonOptions)!;
                var outcome = await DiscoveryEngine.AnalyzeAsync(github, repo, ct, Log, features);
                if (!(outcome.CommitsSucceeded && outcome.SubscriptionsSucceeded)) Interlocked.Increment(ref failed);
                repo.Links = repo.Links
                    .Where(l => l.IsAnalyzed && l.IsValid && l.NodeCount > 0 && GitHubService.IsAllowedSubscription(l.Url))
                    .ToList();
                repo.TotalNodes = GitHubAnalyzer.BestNodeCount(repo);
                var done = Interlocked.Increment(ref completed);
                Log($"分析进度 {done}/{candidates.Length}");
                analyzed(repo.FullName);
                return repo.Links.Count > 0 ? repo : null;
            }
            finally { limit.Release(); }
        }));
        return (results.OfType<RepoInfo>().OrderByDescending(r => r.Score).ToArray(), failed);
    }

    /// <summary>
    /// 合并去重订阅文本，输出格式与 web 的 SubscriptionExporter（plain）一致：
    /// 解析每个订阅 → 按 server:port 去重 → 一行一个节点 URI。
    /// 与 Web 的差异：单条下载失败或内容异常时跳过该条并记录，
    /// 而不是整轮放弃——定时任务不该因为某个历史仓库删了文件就永远发不出新订阅。
    /// </summary>
    private static async Task<string> MergeSubscriptionsAsync(GitHubService github, string[] urls, CancellationToken ct)
    {
        var contents = new List<string>();
        var bytes = 0;
        var skipped = 0;
        foreach (var url in urls)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                var content = await github.GetUrlAsync(url, ct)
                    ?? throw new HttpRequestException("Subscription download failed.");
                if (NodeParser.SplitNodeLines(content).Count == 0)
                    throw new InvalidDataException("包含非 URI/Base64 订阅。");
                bytes += Encoding.UTF8.GetByteCount(content);
                if (bytes > 16 * 1024 * 1024) throw new InvalidDataException("合并内容超过 16 MiB。");
                contents.Add(content);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
            catch (Exception ex) when (ex is HttpRequestException or InvalidDataException)
            {
                skipped++;
                Log($"跳过无法使用的订阅 {url}：{ex.Message}");
            }
        }
        if (contents.Count == 0) throw new InvalidDataException("所有订阅下载均失败，未输出不完整的合并结果。");
        if (skipped > 0) Log($"共跳过 {skipped} 条失效订阅，发布其余 {contents.Count} 条。");
        var (plain, _, total, unique) = NodeParser.MergeNodes(contents);
        Log($"合并 {total} 条节点，去重后 {unique} 条。");
        return plain + "\n";
    }

    private static string FindRepoRoot()
    {
        var overrideRoot = Environment.GetEnvironmentVariable("PROXYNODEHUB_ROOT");
        if (!string.IsNullOrWhiteSpace(overrideRoot))
        {
            var full = Path.GetFullPath(overrideRoot);
            if (!File.Exists(Path.Combine(full, "ProxyNodeHub.slnx")))
                throw new DirectoryNotFoundException($"PROXYNODEHUB_ROOT={full} 下没有 ProxyNodeHub.slnx。");
            return full;
        }
        foreach (var start in new[] { Directory.GetCurrentDirectory(), AppContext.BaseDirectory })
        {
            var dir = new DirectoryInfo(Path.GetFullPath(start));
            while (dir is not null)
            {
                if (File.Exists(Path.Combine(dir.FullName, "ProxyNodeHub.slnx"))) return dir.FullName;
                dir = dir.Parent;
            }
        }
        throw new DirectoryNotFoundException("找不到仓库根目录（ProxyNodeHub.slnx）；请在仓库内运行或设置 PROXYNODEHUB_ROOT。");
    }
}
