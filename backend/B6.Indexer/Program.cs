using Azure.Extensions.AspNetCore.Configuration.Secrets;
using Azure.Identity;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Hosting.WindowsServices;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.EventLog;
using System;
using System.IO;

using B6.Indexer;

var host = Host.CreateDefaultBuilder(args)
    .UseWindowsService()
    .ConfigureAppConfiguration((ctx, cfg) =>
    {
        cfg.SetBasePath(AppContext.BaseDirectory);
        cfg.AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);
        cfg.AddJsonFile($"appsettings.{ctx.HostingEnvironment.EnvironmentName}.json", optional: true, reloadOnChange: true);
        cfg.AddEnvironmentVariables();

        // 🔽 Key Vault overrides
        cfg.AddAzureKeyVault(
            new Uri("https://B6Missions.vault.azure.net/"),
            new DefaultAzureCredential()
        );
    })
    .ConfigureLogging((ctx, logging) =>
    {
        logging.ClearProviders();

        // Pull Logging section (so category filters from appsettings can apply)
        logging.AddConfiguration(ctx.Configuration.GetSection("Logging"));

        logging.AddConsole();
#pragma warning disable CA1416
        // Align SourceName with appsettings.json ("B6Indexer")
        var src = ctx.Configuration.GetValue<string>("Logging:EventLog:SourceName") ?? "B6Indexer";
        logging.AddEventLog(new EventLogSettings { SourceName = src });

        // Keep default noise down…
        logging.AddFilter<EventLogLoggerProvider>("Default", LogLevel.Warning);
        logging.AddFilter<EventLogLoggerProvider>("Microsoft", LogLevel.Warning);
        // …but allow Information for your indexer category
        logging.AddFilter<EventLogLoggerProvider>("B6.Indexer.MissionIndexer", LogLevel.Information);
        // Keep the lifetime “Service started successfully.” message
        logging.AddFilter<EventLogLoggerProvider>("Microsoft.Hosting.Lifetime", LogLevel.Information);
#pragma warning restore CA1416

        // Global floor (safe): info
        logging.SetMinimumLevel(LogLevel.Information);
    })
    .ConfigureServices((ctx, services) =>
    {
        services.AddHostedService<MissionIndexer>();
        services.AddHostedService<RealtimeStatusRefresher>();
    })
    .Build();

Directory.SetCurrentDirectory(AppContext.BaseDirectory);
await host.RunAsync();
