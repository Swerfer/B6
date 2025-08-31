using System.Collections.Concurrent;

namespace B6.Backend.Services
{
    public class NotificationScheduler : BackgroundService
    {
        private static readonly ConcurrentDictionary<string, CancellationTokenSource> _timers = new();
        private readonly ILogger<NotificationScheduler> _log;

        public                          NotificationScheduler(ILogger<NotificationScheduler> log) {
             _log = log; 
        }

        protected override async Task   ExecuteAsync(CancellationToken stoppingToken) {
            // No periodic DB scans here yet – we schedule from PushFanout when webhooks arrive.
            while (!stoppingToken.IsCancellationRequested)
                await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
        }

        public static void              ScheduleOnce(DateTime whenUtc, Func<Task> job) {
            var key = $"{whenUtc:O}|{job.GetHashCode()}";
            if (_timers.ContainsKey(key)) return;

            var cts = new CancellationTokenSource();
            if (!_timers.TryAdd(key, cts)) return;

            _ = Task.Run(async () =>
            {
                try
                {
                    var delay = whenUtc - DateTime.UtcNow;
                    if (delay > TimeSpan.Zero) await Task.Delay(delay, cts.Token);
                    await job();
                }
                catch (TaskCanceledException) { /* ignore */ }
                finally { _timers.TryRemove(key, out _); }
            });
        }
    }
}
