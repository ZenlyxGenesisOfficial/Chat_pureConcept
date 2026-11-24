using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.Extensions.Configuration;
using Microsoft.JSInterop;
using System.Collections.Concurrent;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Text.Json;
using System.Xml.Linq;
using XmppDotNet;
using XmppDotNet.Extensions.Client.Message;
using XmppDotNet.Extensions.Client.Presence;
using XmppDotNet.Extensions.Client.Roster;
using XmppDotNet.Transport;
using XmppDotNet.Transport.WebSocket;
using XmppDotNet.Xmpp;
using XmppDotNet.Xmpp.Client;
using XmppDotNet.Xmpp.HttpUpload;
using XmppDotNet.Xmpp.MessageArchiving;

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
        public DateTime At { get; set; } = DateTime.Now;   // untuk mengurutkan
    }

    public class UserSession
    {
        public static IConfiguration Config { get; } = new ConfigurationBuilder()
            .AddUserSecrets<Program>()
            .Build();

        // -- Client --
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
                }
            }
        }
        public string _password { get; private set; } = string.Empty;
        public string MyPresenceStatus { get; set; } = string.Empty;

        // -- Presence Seeker --
        public readonly Dictionary<string, Dictionary<string, Dictionary<string, string>>> _presences = new(StringComparer.OrdinalIgnoreCase);
        public Dictionary<string, string> GetState(string bareJid)
        {
            if (!_presences.TryGetValue(bareJid, out var res) || res.Count == 0)
            {
                return new() { { "Offline", "" } };
            }

            return res.Values.FirstOrDefault()!;
        }

        // -- UI Needed --
        public event Action? OnChanged;
        public event Action? OnCH_Changed;
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

        // -- Contact --
        public Dictionary<string, string>? Contacts { get; set; } = new();
        public List<string>? ContactsName { get; set; } = new();
        public Dictionary<string, string>? ContactsFullJid { get; set; } = new();

        // -- Chat List --
        private readonly Dictionary<string, List<ChatModel>> _chats = new(StringComparer.OrdinalIgnoreCase);
        private Dictionary<string, string>? _chatHead = new();
        private List<string>? _openedTab = new();

        public Dictionary<string, string>? ChatHead
        {
            get => _chatHead;
            set
            {
                if (value != null)
                {
                    foreach (var item in value)
                        _chatHead![item.Key] = item.Value;
                }

                OnCH_Changed.Invoke();
            }
        }

        public List<string>? OpenedTab
        {
            get => _openedTab;
            set
            {
                if (value != null)
                {
                    _openedTab.AddRange(value);
                }

                OnCH_Changed.Invoke();
            }
        }

        // -- Background Thread --
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
                .Subscribe(m =>
                {
                    var SenderJid = m.From!.ToString().Split('/')[0];
                    Save(new ChatModel
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
        public async Task SetPresence(string showStr, string? status)
        {
            if (!string.IsNullOrEmpty(MyPresenceStatus))
            {
                status = MyPresenceStatus;
            }
            var show = showStr.ToLower() switch
            {
                "chat" => Show.Chat,
                "away" => Show.Away,
                "xa" => Show.ExtendedAway,
                "dnd" => Show.DoNotDisturb,
                _ => Show.None,
            };
            if (_client == null || !IsConnected) return;
            await _client.SendPresenceAsync(show, status ?? "", 1);
        }

        // -- Sending Message --
        public async Task SendAsync(string toJid, string text)
        {
            await _client.SendChatMessageAsync(toJid, text);
            Save(new ChatModel
            {
                Id = Guid.NewGuid().ToString(),
                MyTargetJid = toJid,
                MyTargetFullName = toJid.Split("@")[0],
                Text = text,
                IsMine = true,
                Time = DateTime.Now.ToString("HH:mm:ss - dd/MM/yyyy")
            });
        }

        // -- Chat History --
        public async Task<List<ChatModel>> Load(string contact)
        {
            lock (_chats)
            {
                if (_chats.TryGetValue(contact, out var cached)) return cached.ToList();
            }

            var history = await LoadFromServer(contact);
            return history??new List<ChatModel>();
        }
        private void Save(ChatModel msg)
        {
            lock (_chats)
            {
                if (!_chats.TryGetValue(msg.MyTargetFullName, out var list)) return;
                if (list.Any(x => x.Id == msg.Id)) return;
                list.Add(msg);
            }
        }
        private async Task<List<ChatModel>?> LoadFromServer(string contact)
        {
            var found = new List<ChatModel>();
            if (_client == null || !IsConnected) return null;

            var withJid = Contacts!.Values.FirstOrDefault(j => j.Split('@')[0].Equals(contact, StringComparison.OrdinalIgnoreCase))
                          ?? (contact.Contains('@') ? contact : $"{contact}@{Config["OpenFire:Domain"]}");
            var queryId = Guid.NewGuid().ToString();

            XNamespace mam = "urn:xmpp:mam:2";
            XNamespace fwd = "urn:xmpp:forward:0";
            XNamespace dly = "urn:xmpp:delay";
            XNamespace dfm = "jabber:x:data";
            XNamespace rsm = "http://jabber.org/protocol/rsm";

            using var sub = _client.XmppXElementReceived
                .Where(e => e.Name.LocalName == "message")
                .Subscribe(e =>
                {
                    var r = e.Element(mam + "result");
                    if (r == null || (string?)r.Attribute("queryid") != queryId) return;

                    var forwarded = r.Element(fwd + "forwarded");
                    var inner = forwarded?.Elements().FirstOrDefault(x => x.Name.LocalName == "message");
                    if (inner == null) return;

                    var body = inner.Elements().FirstOrDefault(x => x.Name.LocalName == "body")?.Value;
                    if (string.IsNullOrEmpty(body)) return;

                    var from = ((string?)inner.Attribute("from") ?? "").Split('/')[0];
                    var stamp = (string?)forwarded!.Element(dly + "delay")?.Attribute("stamp");
                    var time = DateTimeOffset.TryParse(stamp, out var t) ? t.LocalDateTime : DateTime.Now;

                    lock (found)
                    {
                        found.Add(new ChatModel
                        {
                            Id = (string?)inner.Attribute("id") ?? (string?)r.Attribute("id") ?? Guid.NewGuid().ToString(),
                            MyTargetJid = withJid,
                            MyTargetFullName = withJid.Split('@')[0],
                            Text = body,
                            IsMine = !string.Equals(from, withJid, StringComparison.OrdinalIgnoreCase),
                            Time = time.ToString("HH:mm:ss - dd/MM/yyyy"),
                            At = time
                        });
                    }
                });

            var query = new XElement(mam + "query",
                new XAttribute("queryid", queryId),
                new XElement(dfm + "x",
                    new XAttribute("type", "submit"),
                    new XElement(dfm + "field",
                        new XAttribute("var", "FORM_TYPE"),
                        new XAttribute("type", "hidden"),
                        new XElement(dfm + "value", "urn:xmpp:mam:2")),
                    new XElement(dfm + "field",
                        new XAttribute("var", "with"),
                        new XElement(dfm + "value", withJid))),
                new XElement(rsm + "set",
                    new XElement(rsm + "max", 50),
                    new XElement(rsm + "before")));

            try
            {
                var iq = new Iq { Type = IqType.Set, Id = Guid.NewGuid().ToString() };
                iq.Add(query);
                var res = await _client.SendIqAsync(iq);

                if ((string?)res.Attribute("type") == "error")
                {
                    Console.WriteLine($"[MAM ERR] server menolak query: {res}");
                    return null;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[MAM ERR] {ex.Message}");
                return null;
            }

            lock (found)
            {
                return found.ToList();
            }
        }

        // -- Disconnect --
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