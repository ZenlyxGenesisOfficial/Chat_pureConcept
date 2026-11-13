using Microsoft.AspNetCore.Components.Server.Circuits;
namespace chat.Session
{
    public class SessionWebCircuit : CircuitHandler
    {
        private readonly UserSession _session;
        private CancellationTokenSource? _cts;

        public SessionWebCircuit(UserSession session)
        {
            _session = session;
        }

        public override Task OnConnectionDownAsync(Circuit circuit, CancellationToken ct)
        {
            _cts?.Cancel();
            _cts = new();
            var token = _cts.Token;

            _ = Task.Run(async() => {
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(3), token);
                    await _session.DisconnectAsync();
                }
                catch (OperationCanceledException) { }
                catch (Exception ex) { Console.WriteLine(ex); }
            });

            return Task.CompletedTask;
        }

        public override Task OnConnectionUpAsync(Circuit circuit, CancellationToken ct)
        {
            _cts?.Cancel();
            return Task.CompletedTask;
        }
    }
}
