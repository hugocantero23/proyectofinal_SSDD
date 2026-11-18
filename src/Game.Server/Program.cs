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

IConfiguration conf = BuildConfiguration(); 

RabbitMqConnectionOptions rabbitOpt = new RabbitMqConnectionOptions(
    Host:     conf["RabbitMQ:Host"], 
    UserName: conf["RabbitMQ:UserName"],
    Password: conf["RabbitMQ:Password"],
    Port:     conf["RabbitMQ:Port"]
);

RabbitGameEventPublisher publisher = new RabbitGameEventPublisher(rabbitOpt);

builder.Services.AddSingleton<RabbitGameEventPublisher>();


var serverAddress = Environment.GetEnvironmentVariable("GAME_SERVER_URL") ?? "http://localhost:5000";

builder.WebHost.UseUrls(serverAddress);

builder.WebHost.ConfigureKestrel(options =>
    options.ConfigureEndpointDefaults(endpoint => endpoint.Protocols = HttpProtocols.Http2));

// añadir servicios gRPC y registrar GameCoordinator y RandomDiceRoller como singleton
builder.Service.AddGrpc();
builder.Service.AddSingleton<GameCoordinator>();
builder.Service.AddSingleton<RandomDiceRoller>();
//--------


await using var app = builder.Build();
// mapear el servicio gRPC
app.MapGrpcService<GameGrpcService>();
//--------

app.MapGet("/", () => "Servidor de juego gRPC.");
await app.RunAsync();
