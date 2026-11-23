using chat.Components;
using chat.Session;
using Microsoft.AspNetCore.Components.Server.Circuits;
using Microsoft.Extensions.FileProviders;
using Radzen;

namespace chat
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var builder = WebApplication.CreateBuilder(args);

            // Add services to the container.
            builder.Services.AddRazorComponents()
                .AddInteractiveServerComponents();

            //builder.Services.AddScoped<XmppClient>(sp =>
            //{
            //    var client = new XmppClient(conf =>
            //    {
            //        var alamatOpenfireLocal = new Uri(builder.Configuration["OpenFire:Url"]!);
            //        conf.UseWebSocketTransport(new StaticNameResolver(alamatOpenfireLocal));
            //        conf.AutoReconnect = true;
            //    });
            //    return client;

            //});

            builder.Services.AddScoped<UserSession>();
            builder.Services.AddScoped<CircuitHandler, SessionWebCircuit>();
            builder.Services.AddRadzenComponents();
            builder.Services.AddBootstrapBlazor();

            var app = builder.Build();

            var uploadsDir = Path.Combine(app.Environment.ContentRootPath, "uploads");
            Directory.CreateDirectory(uploadsDir);

            app.UseStaticFiles(new StaticFileOptions
            {
                FileProvider = new PhysicalFileProvider(uploadsDir),
                RequestPath = "/uploads",
                OnPrepareResponse = ctx =>
                    ctx.Context.Response.Headers["X-Content-Type-Options"] = "nosniff"
            });

            // Configure the HTTP request pipeline.
            if (!app.Environment.IsDevelopment())
            {
                app.UseExceptionHandler("/Error");
            }

            app.UseStatusCodePagesWithReExecute("/not-found", createScopeForStatusCodePages: true);
            app.UseAntiforgery();

            app.MapStaticAssets();
            app.MapRazorComponents<App>()
                .AddInteractiveServerRenderMode();

            app.Run();
        }
    }
}
