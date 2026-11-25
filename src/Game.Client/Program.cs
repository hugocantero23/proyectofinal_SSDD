using Game.Client;
using Microsoft.Extensions.Configuration;
using Spectre.Console;

var serverAddress = Environment.GetEnvironmentVariable("GAME_SERVER_ADDRESS") ?? "http://localhost:5000";

var configuration = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: false)
    .AddEnvironmentVariables()
    .Build();
// 1. Cargar configuración de rabbitMQ en la variable rabbitOptions y 
var rabbitOptions = new RabbitMqConnectionOptions(
    Host:     configuration["RabbitMQ:Host"]!,
    UserName: configuration["RabbitMQ:UserName"]!,
    Password: configuration["RabbitMQ:Password"]!,
    Port:     int.Parse(configuration["RabbitMQ:Port"]!)
);
// 2. Crear instancia de RabbitGameEventSubscriber con la configuración cargada.
var eventSubscriber = new RabbitGameEventSubscriber(rabbitOptions);
 
//--------


// Aquí se crea el cliente gRPC y se pasa la dirección del servidor:
await using var grpcClient = new GrpcGameClient(serverAddress);


var ansiConsole = AnsiConsole.Console;

//Cuando se esté implementando rabbitMQ, sustituir null por el suscriptor de eventos creado en el paso 2.
var consoleGame = new ConsoleGame(grpcClient, eventSubscriber, ansiConsole);
if (Console.IsInputRedirected ||
    Console.IsOutputRedirected ||
    !ansiConsole.Profile.Capabilities.Interactive)
{
    await consoleGame.RunAsync(Console.In, Console.Out);
}
else
{
    await consoleGame.RunInteractiveAsync();
}
