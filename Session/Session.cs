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

        public const long MaxFile = 5 * 1024 * 1024;   // 5 MB
        public static readonly string[] AllowedExt = { ".jpg", ".jpeg", ".png", ".gif", ".webp" };

        // -- Jabber Protocol --
        private const string DiscoNs = "http://jabber.org/protocol/disco#info";
        private const string SiNs = "http://jabber.org/protocol/si";
        private const string FtNs = "http://jabber.org/protocol/si/profile/file-transfer";
        private const string FnNs = "http://jabber.org/protocol/feature-neg";
        private const string IbbNs = "http://jabber.org/protocol/ibb";

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
        private readonly Microsoft.AspNetCore.Hosting.IWebHostEnvironment _env;
        public UserSession(IJSRuntime js, Microsoft.AspNetCore.Hosting.IWebHostEnvironment env)
        {
            _js = js;
            _env = env;
        }

        // -- Contact --
        public Dictionary<string, string>? Contacts { get; set; } = new Dictionary<string, string>();
        public List<string>? ContactsName { get; set; } = new List<string>();
        public Dictionary<string, string>? ContactsFullJid { get; set; } = new Dictionary<string, string>();

        // -- Chat List --
        private readonly Dictionary<string, List<ChatModel>> _chats = new(StringComparer.OrdinalIgnoreCase);

        // -- Attachment --
        private readonly Dictionary<string, List<ChatModel>> _files = new(StringComparer.OrdinalIgnoreCase);
        private class FileIn
        {
            public string From = "";
            public string FileName = "";
            public long Size;
            public MemoryStream Data = new();
        }
        private readonly ConcurrentDictionary<string, FileIn> _incoming = new();

        // -- Background Thread --
        private readonly CompositeDisposable _subs = new();

        // -- Login --
        public async Task ConnectAsync(string jid, string password)
        {
            _subs.Clear();
            lock (_chats) { _chats.Clear(); _files.Clear(); }
            _incoming.Clear();
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

            _subs.Add(_client.XmppXElementReceived
                .OfType<Iq>()
                .Subscribe(iq => _ = HandleIqAsync(iq)));

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
            await _client.SendPresenceAsync(show, status ?? "");
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

        // -- Attachment Prosedure --
        private async Task<XElement> IqAsync(string to, XElement body)
        {
            var iq = new Iq { Type = IqType.Set, Id = Guid.NewGuid().ToString() };
            iq.SetAttributeValue("to", to);
            iq.Add(body);
            var res = await _client.SendIqAsync(iq);
            if ((string?)res.Attribute("type") == "error") throw new Exception($"Refused: {res}");
            return res;
        }
        private async Task<XElement> IqWaitAsync(Iq iq, int seconds)
        {
            var id = (string)iq.Attribute("id")!;
            var tcs = new TaskCompletionSource<XElement>();

            using var sub = _client.XmppXElementReceived
                .Where(e => e.Name.LocalName == "iq"
                         && (string?)e.Attribute("id") == id
                         && ((string?)e.Attribute("type") == "result" || (string?)e.Attribute("type") == "error"))
                .Subscribe(e => tcs.TrySetResult(e));

            _ = _client.SendIqAsync(iq).ContinueWith(t => { _ = t.Exception; });

            var done = await Task.WhenAny(tcs.Task, Task.Delay(TimeSpan.FromSeconds(seconds)));
            if (done != tcs.Task) throw new Exception("No answer (timeout)");
            return await tcs.Task;
        }
        private async Task ReplyAsync(string to, string id, XElement? body = null, string? error = null)
        {
            var iq = new Iq { Type = error == null ? IqType.Result : IqType.Error, Id = id };
            iq.SetAttributeValue("to", to);
            if (body != null) iq.Add(body);
            if (error != null)
                iq.Add(XElement.Parse($"<error type='cancel'><{error} xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/></error>"));
            await _client.SendAsync(iq);
        }
        private string StoreFile(string fileName, byte[] data)
        {
            var id = Guid.NewGuid().ToString("N");
            var dir = Path.Combine(_env.ContentRootPath, "uploads", id);
            Directory.CreateDirectory(dir);
            File.WriteAllBytes(Path.Combine(dir, fileName), data);
            return $"/uploads/{id}/{Uri.EscapeDataString(fileName)}";
        }
        private void AddFileMessage(string bareJid, string url, bool mine)
        {
            var msg = new ChatModel
            {
                Id = Guid.NewGuid().ToString(),
                MyTargetJid = bareJid,
                MyTargetFullName = bareJid.Split('@')[0],
                Text = url,
                IsMine = mine,
                Time = DateTime.Now.ToString("HH:mm:ss - dd/MM/yyyy"),
                At = DateTime.Now
            };

            lock (_chats)
            {
                if (!_files.TryGetValue(msg.MyTargetFullName, out var fl))
                    _files[msg.MyTargetFullName] = fl = new List<ChatModel>();
                fl.Add(msg);

                if (_chats.TryGetValue(msg.MyTargetFullName, out var list)) list.Add(msg);
            }
        }
        public async Task SendFileAsync(string toJid, string fileName, string contentType, byte[] data)
        {
            var bare = toJid.Split('/')[0];
            var resource = _presences.TryGetValue(bare, out var res) ? res.Keys.FirstOrDefault() : null;
            if (string.IsNullOrEmpty(resource)) throw new Exception("Contact is offline");
            var fullJid = $"{bare}/{resource}";
            var sid = Guid.NewGuid().ToString("N");

            XNamespace si = SiNs, ft = FtNs, fn = FnNs, df = "jabber:x:data", ibb = IbbNs;

            var offer = new Iq { Type = IqType.Set, Id = Guid.NewGuid().ToString() };
            offer.SetAttributeValue("to", fullJid);
            offer.Add(new XElement(si + "si",
                new XAttribute("id", sid),
                new XAttribute("mime-type", contentType),
                new XAttribute("profile", FtNs),
                new XElement(ft + "file",
                    new XAttribute("name", fileName),
                    new XAttribute("size", data.Length)),
                new XElement(fn + "feature",
                    new XElement(df + "x",
                        new XAttribute("type", "form"),
                        new XElement(df + "field",
                            new XAttribute("var", "stream-method"),
                            new XAttribute("type", "list-single"),
                            new XElement(df + "option",
                                new XElement(df + "value", IbbNs)))))));

            var answer = await IqWaitAsync(offer, 120);
            if ((string?)answer.Attribute("type") == "error")
                throw new Exception($"{bare.Split('@')[0]} declined the file");

            const int block = 4096;
            await IqAsync(fullJid, new XElement(ibb + "open",
                new XAttribute("sid", sid),
                new XAttribute("block-size", block),
                new XAttribute("stanza", "iq")));

            for (int i = 0, seq = 0; i < data.Length; i += block, seq++)
            {
                var chunk = Convert.ToBase64String(data, i, Math.Min(block, data.Length - i));
                await IqAsync(fullJid, new XElement(ibb + "data",
                    new XAttribute("sid", sid),
                    new XAttribute("seq", seq),
                    chunk));
            }

            await IqAsync(fullJid, new XElement(ibb + "close", new XAttribute("sid", sid)));

            AddFileMessage(bare, StoreFile(fileName, data), true);
        }
        private async Task HandleIqAsync(XElement iq)
        {
            try
            {
                var type = (string?)iq.Attribute("type");
                if (type != "get" && type != "set") return;

                var id = (string?)iq.Attribute("id") ?? "";
                var from = (string?)iq.Attribute("from") ?? "";
                var child = iq.Elements().FirstOrDefault();
                if (child == null || from == "") return;
                var ns = child.Name.NamespaceName;

                if (type == "get" && ns == DiscoNs && child.Attribute("node") == null)
                {
                    await ReplyAsync(from, id, XElement.Parse(
                        $"<query xmlns='{DiscoNs}'>" +
                        "<identity category='client' type='web' name='Chat Widget'/>" +
                        $"<feature var='{DiscoNs}'/><feature var='{SiNs}'/><feature var='{FtNs}'/><feature var='{IbbNs}'/>" +
                        "</query>"));
                }
                else if (type == "set" && ns == SiNs)
                {
                    var file = child.Descendants().FirstOrDefault(x => x.Name.LocalName == "file");
                    var sid = (string?)child.Attribute("id") ?? "";
                    var name = Path.GetFileName((string?)file?.Attribute("name") ?? "");
                    long.TryParse((string?)file?.Attribute("size"), out var size);
                    var methods = child.Descendants().Where(x => x.Name.LocalName == "value").Select(x => x.Value).ToList();
                    var bare = from.Split('/')[0];

                    var ok = sid != "" && file != null
                          && (string?)child.Attribute("profile") == FtNs
                          && Contacts!.Values.Any(j => j.Equals(bare, StringComparison.OrdinalIgnoreCase))
                          && AllowedExt.Contains(Path.GetExtension(name).ToLower())
                          && size > 0 && size <= MaxFile
                          && methods.Contains(IbbNs);

                    if (!ok)
                    {
                        await ReplyAsync(from, id, null, "forbidden");
                        return;
                    }

                    _incoming[sid] = new FileIn { From = from, FileName = name, Size = size };
                    await ReplyAsync(from, id, XElement.Parse(
                        $"<si xmlns='{SiNs}'><feature xmlns='{FnNs}'><x xmlns='jabber:x:data' type='submit'>" +
                        $"<field var='stream-method'><value>{IbbNs}</value></field></x></feature></si>"));
                }
                else if (type == "set" && ns == IbbNs)
                {
                    var sid = (string?)child.Attribute("sid") ?? "";
                    if (!_incoming.TryGetValue(sid, out var f) || !f.From.Equals(from, StringComparison.OrdinalIgnoreCase))
                    {
                        await ReplyAsync(from, id, null, "item-not-found");
                        return;
                    }

                    switch (child.Name.LocalName)
                    {
                        case "open":
                            await ReplyAsync(from, id);
                            break;

                        case "data":
                            f.Data.Write(Convert.FromBase64String(child.Value.Trim()));
                            if (f.Data.Length > f.Size)
                            {
                                _incoming.TryRemove(sid, out _);
                                await ReplyAsync(from, id, null, "not-acceptable");
                                break;
                            }
                            await ReplyAsync(from, id);
                            break;

                        case "close":
                            _incoming.TryRemove(sid, out _);
                            await ReplyAsync(from, id);
                            if (f.Data.Length > 0)
                            {
                                var url = StoreFile(f.FileName, f.Data.ToArray());
                                AddFileMessage(f.From.Split('/')[0], url, false);
                                OnChanged?.Invoke();
                            }
                            break;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[IQ ERR] {ex.Message}");
            }
        }

        // -- Chat History --
        public async Task<List<ChatModel>> Load(string contact)
        {
            lock (_chats)
            {
                if (_chats.TryGetValue(contact, out var cached)) return cached.ToList();
            }

            var history = await LoadFromServer(contact);
            if (history == null) return new List<ChatModel>();

            lock (_chats)
            {
                if (!_chats.TryGetValue(contact, out var list))
                {
                    if (_files.TryGetValue(contact, out var fl))
                    {
                        history.AddRange(fl);
                        history = history.OrderBy(x => x.At).ToList();
                    }
                    _chats[contact] = list = history;
                }
                return list.ToList();
            }
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