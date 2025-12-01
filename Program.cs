
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
// IMPORTANT: use Microsoft.Azure.Functions.Worker
using Microsoft.Azure.Functions.Worker;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureAppConfiguration((context, config) =>
    {
        config.AddEnvironmentVariables();
    })
    .Build();

host.Run();
