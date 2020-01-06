using System;
using System.Threading;
using System.Threading.Tasks;
using dotnet_etcd;
using Dream_Stream.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Prometheus;

namespace Dream_Stream
{
    public class Startup
    {
        public static bool ShuttingDown { get; set; }

        // This method gets called by the runtime. Use this method to add services to the container.
        // For more information on how to configure your application, visit https://go.microsoft.com/fwlink/?LinkID=398940
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddControllers();

            if(Environment.GetEnvironmentVariable("LOCAL_STORAGE") == "TRUE")
                services.AddSingleton<IStorage, StorageService>();
            else
                services.AddSingleton<IStorage, StorageApiService>();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public async void Configure(IApplicationBuilder app, IWebHostEnvironment env, IHostApplicationLifetime appLifeTime, IStorage storageService)
        {
            
            if (env.IsDevelopment()) app.UseDeveloperExceptionPage();

            app.UseMetricServer();

            app.UseRouting();
            
            app.UseAuthorization();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            });

            app.UseWebSockets(new WebSocketOptions
            {
                KeepAliveInterval = TimeSpan.FromSeconds(120),
                ReceiveBufferSize = 1024 * 900
            });

            app.Use(async (context, next) =>
            {
                if (context.Request.Path == "/ws")
                {
                    if (context.WebSockets.IsWebSocketRequest)
                    {
                        try
                        {
                            var webSocket = await context.WebSockets.AcceptWebSocketAsync();
                            await Task.Delay(30000);
                            await new MessageHandler(storageService).Handle(context, webSocket);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine($"Connection closed from startup with exception: {e}");
                        }
                    }
                    else
                    {
                        context.Response.StatusCode = 400;
                    }
                }
                else
                {
                    await next();
                }
            });

            var client = env.IsDevelopment() ? new EtcdClient("http://localhost") : new EtcdClient("http://etcd");
            var me = Guid.NewGuid().ToString();

            await Task.Delay(1*1000);
            var brokerTable = new BrokerTable(client);
            await brokerTable.ImHere();

            var topicList = new TopicList(client, me);
            await topicList.SetupTopicListWatch();

            appLifeTime.ApplicationStopping.Register(() =>
            {
                Console.WriteLine("Started shutting down...");
                ShuttingDown = true;
                brokerTable.Shutdown();
                topicList.Shutdown();
                Console.WriteLine("Shutdown...");
            });
        }
    }
}