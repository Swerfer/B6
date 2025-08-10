using System;
using System.IO;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.EventLog;
using Microsoft.Extensions.Hosting.WindowsServices;

using B6.Indexer;

using Azure.Identity;
using Azure.Extensions.AspNetCore.Configuration.Secrets;

var host = Host.CreateDefaultBuilder(args)
    .UseWindowsService()
    .ConfigureAppConfiguration((ctx, cfg) =>
    {
        // read local files first (optional), then override from Key Vault
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
    .ConfigureLogging(logging =>
    {
        logging.ClearProviders();
        logging.AddConsole();
#pragma warning disable CA1416
        logging.AddEventLog(new EventLogSettings { SourceName = "B6.Indexer" });
#pragma warning restore CA1416
        logging.SetMinimumLevel(LogLevel.Information);
    })
    .ConfigureServices((ctx, services) =>
    {
        services.AddHostedService<MissionIndexer>();
    })
    .Build();

Directory.SetCurrentDirectory(AppContext.BaseDirectory);
await host.RunAsync();
