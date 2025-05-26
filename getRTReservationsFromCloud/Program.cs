using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace YourNamespace
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((hostContext, services) =>
                {
                    // Register your background worker
                    services.AddHostedService<Worker>();

                    // Add access to IConfiguration if needed
                    services.AddSingleton<IConfiguration>(hostContext.Configuration);

                    // Optional: Add logging configuration here if needed
                });
    }
}
