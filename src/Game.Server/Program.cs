using Game.Core;
using Game.Server;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Configuration;

var builder = WebApplication.CreateBuilder(args);

/*
    1. Cargar configuración de rabbitMQ
    2. Crear instancia de RabbitGameEventPublisher con la configuración cargada.
    3. Llamar a InitializeAsync() para inicializar la conexión y el canal de RabbitMQ.
    4. Resgistrar RabbitGameEventPublisher como singleton en el contenedor de servicios, con el evento de publicación creado.
*/

IConfiguration conf = builder.Configuration;

RabbitMqConnectionOptions rabbitOpt = new RabbitMqConnectionOptions(
    Host: conf["RabbitMQ:Host"]!,
    UserName: conf["RabbitMQ:UserName"]!,
    Password: conf["RabbitMQ:Password"]!,
    Port: int.Parse(conf["RabbitMQ:Port"]!)
);

RabbitGameEventPublisher publisher = new RabbitGameEventPublisher(rabbitOpt);

await publisher.InitializeAsync();

builder.Services.AddSingleton(publisher);

builder.Services.AddSingleton<IGameEventPublisher>(publisher);

var serverAddress =
    Environment.GetEnvironmentVariable("GAME_SERVER_URL") ?? "http://localhost:5000";

builder.WebHost.UseUrls(serverAddress);

builder.WebHost.ConfigureKestrel(options =>
    options.ConfigureEndpointDefaults(endpoint => endpoint.Protocols = HttpProtocols.Http2)
);

// añadir servicios gRPC y registrar GameCoordinator y RandomDiceRoller como singleton
builder.Services.AddGrpc();
builder.Services.AddSingleton<GameCoordinator>();
builder.Services.AddSingleton<RandomDiceRoller>();

//--------

await using var app = builder.Build();

// mapear el servicio gRPC
app.MapGrpcService<GameGrpcService>();

//--------

app.MapGet("/", () => "Servidor de juego gRPC.");
await app.RunAsync();
