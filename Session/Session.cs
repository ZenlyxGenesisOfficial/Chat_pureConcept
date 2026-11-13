using Microsoft.JSInterop;
using XmppDotNet;
using XmppDotNet.Xmpp.Client;
using XmppDotNet.Transport;
using XmppDotNet.Transport.WebSocket;
using XmppDotNet.Xmpp.HttpUpload;
using Microsoft.Extensions.Configuration;
using System.Reactive.Linq;
using XmppDotNet.Xmpp.MessageArchiving;
using XmppDotNet.Extensions.Client.Presence;
using XmppDotNet.Xmpp;
using XmppDotNet.Extensions.Client.Message;
using System.Text.Json;
using XmppDotNet.Extensions.Client.Roster;
using System.Reflection.Metadata.Ecma335;
using System.Reactive.Disposables;

namespace chat.Session
{
    public class ChatModel
    {
        public string Id { get; set; }
        public string MyTargetJid { get; set; } = "";
        public string MyTargetFullName { get; set; } = "";
        public bool IsMine { get; set; }
        public string Text { get; set; } = "";
        public string Time { get; set; } = "";
    }
    public class UserSession
    {
        public static IConfiguration Config { get; } = new ConfigurationBuilder()
            .AddUserSecrets<Program>()
            .Build();
        //Client
        private XmppClient _client;
        private string _jid = string.Empty;
        private string _fullName = string.Empty;
        public string Jid
        {
            get => _jid;
            private set
            {
                if (_jid != value)
                {
                    _jid = value;
                    _fullName = value.ToString().Split('@')[0];
                    Console.WriteLine($"kare '{value}' maka jadi {_fullName} dengan {_jid}");
                }
            }
        }
        public string _password { get; private set; } = string.Empty;

        //Presence Seeker
        public readonly Dictionary<string, Dictionary<string, Dictionary<string, string>>> _presences = new(StringComparer.OrdinalIgnoreCase);
        public Dictionary<string, string> GetState(string bareJid)
        {
            if (!_presences.TryGetValue(bareJid, out var res) || res.Count == 0)
            {
                return new() { { "Offline", "" } };
            }

            return res.Values.FirstOrDefault()!;
        }

        //UI Needed
        public event Action? OnChanged;
        public int _presencePending;
        public void NotifyStateChanged()
        {
            if (Interlocked.Exchange(ref _presencePending, 1) == 1) return;
            _ = Task.Run(async () =>
            {
                Interlocked.Exchange(ref _presencePending, 0);
                try { OnChanged?.Invoke(); }
                catch (Exception ex) { Console.WriteLine(ex); }
                await Task.Delay(300);
            });
        }
        public bool IsConnected { get; private set; }
        private readonly IJSRuntime _js;
        public UserSession(IJSRuntime js) => _js = js;

        //Contact
        public Dictionary<string, string>? Contacts { get; set; } = new Dictionary<string, string>();
        public List<string>? ContactsName { get; set; } = new List<string>();
        public Dictionary<string, string>? ContactsFullJid { get; set; } = new Dictionary<string, string>();


        //Background Thread
        private readonly CompositeDisposable _subs = new();

        // -- Login --
        public async Task ConnectAsync(string jid, string password)
        {
            _subs.Clear();
            Jid = jid.Trim();
            _password = password.Trim();

            _client = new XmppClient(conf =>
            {
                conf.UseWebSocketTransport(new StaticNameResolver(
                    new Uri(Config["OpenFire:Url"])
                ));
            })
            {
                Jid = _jid,
                Password = _password
            };

            _subs.Add(_client.XmppXElementReceived
                .OfType<Message>()
                .Where(m => !string.IsNullOrEmpty(m.Body))
                .Subscribe(async m =>
                {
                    Console.WriteLine(m.ToString());
                    var SenderJid = m.From!.ToString().Split('/')[0];
                    await Save(new ChatModel
                    {
                        Id = m.Id ?? Guid.NewGuid().ToString(),
                        MyTargetJid = SenderJid,
                        MyTargetFullName = SenderJid.Split("@")[0],
                        Text = m.Body,
                        IsMine = false,
                        Time = DateTime.Now.ToString("HH:mm:ss - dd/MM/yyyy")
                    });
                    OnChanged.Invoke();
                }));

            _subs.Add(_client.StateChanged
                .Where(s => s == SessionState.Binded)
                .Subscribe(async async =>
                {
                    var roster = await _client.RequestRosterAsync();
                    Contacts.Clear();
                    ContactsName.Clear();
                    foreach (var item in roster.Query.Elements().Where(e => e.Name.LocalName == "item"))
                    {
                        var jid = item.Attribute("jid")?.Value;
                        var fullname = item.Attribute("name")?.Value;
                        var sub = item.Attribute("subscription")?.Value.ToLower();
                        if (jid == "" || ContactsName.Contains(fullname)) continue;
                        ContactsName.Add(fullname);
                        Contacts[fullname] = jid;
                    }
                    ContactsName.Sort();
                    IsConnected = true;
                    await SetPresence("Chat", "Online");
                    //await _client.SendPresenceAsync(Show.Chat, "Online");
                    OnChanged.Invoke();
                }));

            _subs.Add(_client.XmppXElementReceived
                .OfType<Presence>()
                .Subscribe(p =>
                {
                    try
                    {
                        var parts = p.From.ToString().Split('/', 2);
                        var bare = parts[0].ToLower();
                        var resource = parts.Length > 1 ? parts[1] : "";

                        if (bare == _jid) return;

                        var type = (string?)p.Attribute("type");

                        if (type != null && type != "unavailable") return;

                        if (!_presences.TryGetValue(bare, out var res))
                        {
                            _presences[bare] = res = new Dictionary<string, Dictionary<string, string>>();
                        }

                        if (type == "unavailable")
                        {
                            res.Remove(resource);
                        }
                        else
                        {
                            var show = p.Elements()
                            .FirstOrDefault(e => e.Name.LocalName == "show")?.Value ?? "Online";

                            show = show == "chat" ? "Online" : show;

                            var status = p.Elements()
                            .FirstOrDefault(e => e.Name.LocalName == "status")?.Value ?? "";

                            res[resource] = new() { { show, status } };
                        }

                        NotifyStateChanged();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[PRESENCE ERR] {ex.Message}");
                    }
                }));

            try
            {
                await _client.ConnectAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                IsConnected = false;
                OnChanged.Invoke();
            }
        }

        // -- Presence Adjustment --
        [JSInvokable]
        public async Task SetPresence(string showStr, string?status)
        {
            var show = showStr.ToLower() switch
            {
                "chat" => Show.Chat,
                "away" => Show.Away,
                "xa" => Show.ExtendedAway,
                "dnd" => Show.DoNotDisturb,
                _ => Show.None,
            };
            if (_client == null || !IsConnected) return;
            await _client.SendPresenceAsync(show, status);
        }

        public async Task SendAsync(string toJid, string text)
        {
            await _client.SendChatMessageAsync(toJid, text);
            await Save(new ChatModel
            {
                Id = Guid.NewGuid().ToString(),
                MyTargetJid = toJid,
                MyTargetFullName = toJid.Split("@")[0],
                Text = text,
                IsMine = true,
                Time = DateTime.Now.ToString("HH:mm:ss - dd/MM/yyyy")
            });
        }

        private string Key(string contact) => $"chat@{_fullName}@{contact}";

        public async Task<List<ChatModel>> Load(string contact)
        {
            Console.WriteLine(Key(contact));
            var json = await _js.InvokeAsync<string?>("localStorage.getItem", Key(contact));
            return string.IsNullOrEmpty(json)
                ? new()
                : JsonSerializer.Deserialize<List<ChatModel>>(json) ?? new();
        }

        private async Task Save(ChatModel msg)
        {
            var list = await Load(msg.MyTargetFullName);
            if (list.Any(x => x.Id == msg.Id)) return;
            list.Add(msg);
            await _js.InvokeVoidAsync("localStorage.setItem", Key(msg.MyTargetFullName), JsonSerializer.Serialize(list));
        }

        public async Task DisconnectAsync()
        {
            _subs.Clear();
            if (_client != null) await _client.DisconnectAsync();
            IsConnected = false;
        }

        public async ValueTask Dispose()
        {
            _subs.Dispose();
        }
    }
}
