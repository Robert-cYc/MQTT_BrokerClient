#!/usr/bin/env python3
"""
MQTT Suite — a minimal MQTT 3.1.1 broker + client with a Tkinter GUI.
Broker: pure asyncio, no dependencies.  Client: paho-mqtt.
"""
import asyncio
import logging
import queue
import struct
import threading
import time
import tkinter as tk
from tkinter import ttk, messagebox

import ui_theme

# --------------------------------------------------------------------------
#  MQTT wire-format helpers
# --------------------------------------------------------------------------
CONNECT, CONNACK, PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP = 1, 2, 3, 4, 5, 6, 7
SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK = 8, 9, 10, 11
PINGREQ, PINGRESP, DISCONNECT = 12, 13, 14


class MQTTError(Exception):
    pass


def enc_len(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n % 128
        n //= 128
        if n:
            b |= 0x80
        out.append(b)
        if not n:
            return bytes(out)


def enc_str(s: str) -> bytes:
    b = s.encode("utf-8")
    return struct.pack("!H", len(b)) + b


class Reader:
    """Cursor over a decoded variable-header/payload buffer."""

    def __init__(self, data: bytes):
        self.d, self.i = data, 0

    def byte(self) -> int:
        self._need(1)
        self.i += 1
        return self.d[self.i - 1]

    def uint16(self) -> int:
        self._need(2)
        self.i += 2
        return struct.unpack_from("!H", self.d, self.i - 2)[0]

    def string(self) -> str:
        n = self.uint16()
        self._need(n)
        self.i += n
        return self.d[self.i - n:self.i].decode("utf-8", "replace")

    def rest(self) -> bytes:
        r = self.d[self.i:]
        self.i = len(self.d)
        return r

    @property
    def left(self) -> int:
        return len(self.d) - self.i

    def _need(self, n):
        if self.left < n:
            raise MQTTError("truncated packet")


async def read_packet(reader: asyncio.StreamReader):
    """Return (packet_type, flags, payload_bytes)."""
    head = await reader.readexactly(1)
    mult, value = 1, 0
    while True:
        eb = (await reader.readexactly(1))[0]
        value += (eb & 0x7F) * mult
        mult *= 128
        if mult > 128 ** 3 * 128:
            raise MQTTError("malformed remaining length")
        if not eb & 0x80:
            break
    body = await reader.readexactly(value) if value else b""
    return head[0] >> 4, head[0] & 0x0F, body


def topic_matches(filt: str, topic: str) -> bool:
    """MQTT wildcard matching: + = one level, # = rest of the tree."""
    if filt == topic:
        return True
    f, t = filt.split("/"), topic.split("/")
    for i, part in enumerate(f):
        if part == "#":
            return not (i == 0 and topic.startswith("$"))
        if i >= len(t):
            return False
        if part == "+":
            continue
        if part != t[i]:
            return False
    return len(f) == len(t)


# --------------------------------------------------------------------------
#  Broker
# --------------------------------------------------------------------------
class Session:
    def __init__(self, client_id, writer, clean, keepalive, peer):
        self.client_id = client_id
        self.writer = writer
        self.clean = clean
        self.keepalive = keepalive
        self.peer = peer
        self.subs = {}          # filter -> granted qos
        self.will = None        # (topic, payload, qos, retain)
        self._pid = 0

    def next_pid(self) -> int:
        self._pid = self._pid % 65535 + 1
        return self._pid


class Broker:
    """Minimal MQTT 3.1.1 broker: QoS 0/1, retained messages, wills, wildcards."""

    def __init__(self, host="0.0.0.0", port=1883, auth=None, on_event=None):
        self.host, self.port = host, port
        self.auth = auth                     # None or {"user": "pass"}
        self.on_event = on_event or (lambda *a: None)
        self.sessions = {}                   # client_id -> Session
        self.retained = {}                   # topic -> (payload, qos)
        self.msg_count = 0
        self._server = None
        self._loop = None

    # -- lifecycle ---------------------------------------------------------
    async def start(self):
        self._loop = asyncio.get_running_loop()
        self._server = await asyncio.start_server(self._handle, self.host, self.port)
        self.on_event("broker", f"listening on {self.host}:{self.port}")

    async def stop(self):
        for s in list(self.sessions.values()):
            try:
                s.writer.close()
            except Exception:
                pass
        self.sessions.clear()
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        self.on_event("broker", "stopped")

    # -- connection handler -------------------------------------------------
    async def _handle(self, reader, writer):
        peer = "%s:%s" % writer.get_extra_info("peername")[:2]
        sess = None
        clean_exit = False
        try:
            ptype, flags, body = await asyncio.wait_for(read_packet(reader), 10)
            if ptype != CONNECT:
                raise MQTTError("first packet was not CONNECT")
            sess = await self._do_connect(body, writer, peer)

            timeout = sess.keepalive * 1.5 if sess.keepalive else None
            while True:
                ptype, flags, body = await asyncio.wait_for(read_packet(reader), timeout)
                if ptype == PUBLISH:
                    await self._on_publish(sess, flags, body)
                elif ptype == SUBSCRIBE:
                    await self._on_subscribe(sess, body)
                elif ptype == UNSUBSCRIBE:
                    await self._on_unsubscribe(sess, body)
                elif ptype == PINGREQ:
                    self._send(sess, b"\xd0\x00")
                elif ptype == PUBACK:
                    pass                                  # QoS 1 ack from subscriber
                elif ptype == DISCONNECT:
                    clean_exit = True
                    break
                else:
                    raise MQTTError(f"unsupported packet type {ptype}")
        except (asyncio.IncompleteReadError, ConnectionResetError, asyncio.TimeoutError):
            pass
        except MQTTError as e:
            self.on_event("error", f"{peer}: {e}")
        except Exception as e:                            # noqa: BLE001
            self.on_event("error", f"{peer}: {type(e).__name__}: {e}")
        finally:
            if sess:
                if sess.will and not clean_exit:
                    t, p, q, r = sess.will
                    self.on_event("will", f"{sess.client_id} -> {t}")
                    await self.route(t, p, q, r)
                if self.sessions.get(sess.client_id) is sess:
                    del self.sessions[sess.client_id]
                self.on_event("disconnect", f"{sess.client_id} ({peer})")
                self.on_event("clients", self.client_list())
            try:
                writer.close()
            except Exception:
                pass

    async def _do_connect(self, body, writer, peer) -> Session:
        r = Reader(body)
        proto, level = r.string(), r.byte()
        if proto not in ("MQTT", "MQIsdp"):
            raise MQTTError(f"bad protocol name {proto!r}")
        f = r.byte()
        keepalive = r.uint16()
        clean = bool(f & 0x02)
        cid = r.string() or f"anon-{int(time.time()*1000) % 10**6}"
        will = None
        if f & 0x04:
            wt, wp = r.string(), r.rest()[:0] or None   # placeholder, replaced below
            # will payload is a length-prefixed binary field
            r.i -= 0                                     # (kept simple: re-read)
        # re-parse the optional fields in order
        r2 = Reader(body)
        r2.string(); r2.byte(); r2.byte(); r2.uint16(); r2.string()
        if f & 0x04:
            wtopic = r2.string()
            wlen = r2.uint16()
            wpayload = r2.d[r2.i:r2.i + wlen]
            r2.i += wlen
            will = (wtopic, wpayload, (f >> 3) & 0x03, bool(f & 0x20))
        user = r2.string() if f & 0x80 else None
        pwlen = r2.uint16() if f & 0x40 else 0
        pw = r2.d[r2.i:r2.i + pwlen].decode("utf-8", "replace") if f & 0x40 else None

        if self.auth is not None and self.auth.get(user) != pw:
            writer.write(b"\x20\x02\x00\x05")             # bad user/password
            await writer.drain()
            raise MQTTError(f"auth failed for {user!r}")

        if cid in self.sessions:                          # takeover
            try:
                self.sessions[cid].writer.close()
            except Exception:
                pass

        sess = Session(cid, writer, clean, keepalive, peer)
        sess.will = will
        self.sessions[cid] = sess
        writer.write(b"\x20\x02\x00\x00")                 # CONNACK, accepted
        await writer.drain()
        self.on_event("connect", f"{cid} from {peer} (mqtt v{level}, keepalive {keepalive}s)")
        self.on_event("clients", self.client_list())
        return sess

    # -- packet handlers ----------------------------------------------------
    async def _on_publish(self, sess, flags, body):
        qos = (flags >> 1) & 0x03
        retain = bool(flags & 0x01)
        r = Reader(body)
        topic = r.string()
        pid = r.uint16() if qos > 0 else None
        payload = r.rest()
        if qos == 1:
            self._send(sess, b"\x40\x02" + struct.pack("!H", pid))
        elif qos == 2:
            raise MQTTError("QoS 2 is not supported by this broker")
        self.msg_count += 1
        self.on_event("publish",
                      f"{sess.client_id} -> {topic} (qos {qos}{', retain' if retain else ''}) "
                      f"{payload.decode('utf-8', 'replace')[:120]}")
        await self.route(topic, payload, qos, retain)

    async def _on_subscribe(self, sess, body):
        r = Reader(body)
        pid = r.uint16()
        granted = []
        pairs = []
        while r.left:
            filt = r.string()
            q = min(r.byte() & 0x03, 1)                   # cap at QoS 1
            sess.subs[filt] = q
            granted.append(q)
            pairs.append(filt)
        self._send(sess, b"\x90" + enc_len(2 + len(granted))
                   + struct.pack("!H", pid) + bytes(granted))
        self.on_event("subscribe", f"{sess.client_id} -> {', '.join(pairs)}")
        for filt in pairs:                                # replay retained messages
            for topic, (payload, q) in list(self.retained.items()):
                if topic_matches(filt, topic):
                    await self._publish_to(sess, topic, payload,
                                           min(q, sess.subs[filt]), retain=True)

    async def _on_unsubscribe(self, sess, body):
        r = Reader(body)
        pid = r.uint16()
        while r.left:
            sess.subs.pop(r.string(), None)
        self._send(sess, b"\xb0\x02" + struct.pack("!H", pid))

    # -- routing ------------------------------------------------------------
    async def route(self, topic, payload, qos, retain):
        if retain:
            if payload:
                self.retained[topic] = (payload, qos)
            else:
                self.retained.pop(topic, None)
        for sess in list(self.sessions.values()):
            best = None
            for filt, q in sess.subs.items():
                if topic_matches(filt, topic):
                    best = q if best is None else max(best, q)
            if best is not None:
                await self._publish_to(sess, topic, payload, min(qos, best), False)

    async def _publish_to(self, sess, topic, payload, qos, retain):
        vh = enc_str(topic)
        if qos > 0:
            vh += struct.pack("!H", sess.next_pid())
        pkt = bytes([PUBLISH << 4 | qos << 1 | (1 if retain else 0)])
        pkt += enc_len(len(vh) + len(payload)) + vh + payload
        self._send(sess, pkt)

    def _send(self, sess, data: bytes):
        try:
            sess.writer.write(data)
        except Exception as e:                            # noqa: BLE001
            self.on_event("error", f"send to {sess.client_id}: {e}")

    def client_list(self):
        return [(s.client_id, s.peer, len(s.subs)) for s in self.sessions.values()]


class BrokerThread:
    """Runs the asyncio broker in a background thread so Tk stays responsive."""

    def __init__(self, host, port, auth, on_event):
        self.broker = Broker(host, port, auth, on_event)
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self._ready = threading.Event()
        self.error = None

    def _run(self):
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self.broker.start())
        except Exception as e:                            # noqa: BLE001
            self.error = e
            self._ready.set()
            return
        self._ready.set()
        self.loop.run_forever()

    def start(self, timeout=5):
        self.thread.start()
        self._ready.wait(timeout)
        if self.error:
            raise self.error

    def stop(self):
        fut = asyncio.run_coroutine_threadsafe(self.broker.stop(), self.loop)
        try:
            fut.result(timeout=3)
        except Exception:
            pass
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=3)


# --------------------------------------------------------------------------
#  GUI
# --------------------------------------------------------------------------
try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("MQTT Suite — Broker & Client")
        self.geometry("1180x800")
        self.minsize(1000, 680)
        self.fonts = ui_theme.apply_theme(self)
        self.events = queue.Queue()
        self.broker_thread = None
        self.client = None
        self.connected = False

        self._build_header()
        nb = ttk.Notebook(self)
        nb.pack(fill="both", expand=True, padx=20, pady=(0, 20))
        self.broker_tab = ttk.Frame(nb, padding=(0, 14, 0, 0))
        self.client_tab = ttk.Frame(nb, padding=(0, 14, 0, 0))
        nb.add(self.broker_tab, text="Broker")
        nb.add(self.client_tab, text="Client")
        self._build_broker_tab()
        self._build_client_tab()

        self.protocol("WM_DELETE_WINDOW", self.on_close)
        self.after(100, self._drain_events)

    # -- layout helpers -----------------------------------------------------
    def _build_header(self):
        head = ttk.Frame(self, padding=(20, 16, 20, 14))
        head.pack(fill="x")
        ttk.Label(head, text="◆", style="Logo.TLabel").pack(side="left", padx=(0, 12))
        titles = ttk.Frame(head)
        titles.pack(side="left")
        ttk.Label(titles, text="MQTT Suite", style="Title.TLabel").pack(anchor="w")
        ttk.Label(titles, text="Lightweight MQTT 3.1.1 broker & client",
                  style="Subtitle.TLabel").pack(anchor="w")

        self.stat_msgs = self._stat(head, "MESSAGES ROUTED")
        self.stat_clients = self._stat(head, "CLIENTS ONLINE")

    @staticmethod
    def _stat(parent, caption):
        box = ttk.Frame(parent)
        box.pack(side="right", padx=(28, 0))
        value = ttk.Label(box, text="0", style="StatValue.TLabel")
        value.pack(anchor="e")
        ttk.Label(box, text=caption, style="StatCaption.TLabel").pack(anchor="e")
        return value

    @staticmethod
    def _labeled(parent, label, column, row=0, sticky="w", columnspan=1):
        box = ttk.Frame(parent, style="Card.TFrame")
        box.grid(row=row, column=column, columnspan=columnspan, sticky=sticky,
                 padx=(0, 12), pady=(0, 4))
        ttk.Label(box, text=label, style="Field.TLabel").pack(anchor="w", pady=(0, 4))
        return box

    def _entry(self, parent, label, var, width, column, show="", sticky="w"):
        box = self._labeled(parent, label, column, sticky=sticky)
        ttk.Entry(box, textvariable=var, width=width, show=show).pack(fill="x")

    def _qos_picker(self, parent, var, column):
        box = self._labeled(parent, "QoS", column)
        ttk.Combobox(box, textvariable=var, values=[0, 1], width=4,
                     state="readonly").pack()

    @staticmethod
    def _set_status(label, text, kind):
        label.config(text=f"●  {text}", style=ui_theme.status_style(kind))

    @staticmethod
    def _actions(parent, column):
        box = ttk.Frame(parent, style="Card.TFrame")
        box.grid(row=0, column=column, sticky="se", pady=(0, 4))
        return box

    def _update_stats(self):
        broker = self.broker_thread.broker if self.broker_thread else None
        self.stat_clients.config(text=str(len(broker.sessions)) if broker else "0")
        self.stat_msgs.config(text=f"{broker.msg_count:,}" if broker else "0")

    # -- broker tab ---------------------------------------------------------
    def _build_broker_tab(self):
        p = ui_theme.PALETTE
        f = self.broker_tab
        outer, bar = ui_theme.card(f, "Server")
        outer.pack(fill="x", pady=(0, 14))

        self.b_host = tk.StringVar(value="0.0.0.0")
        self.b_port = tk.StringVar(value="1883")
        self.b_user = tk.StringVar()
        self.b_pass = tk.StringVar()

        self._entry(bar, "Bind address", self.b_host, 16, 0)
        self._entry(bar, "Port", self.b_port, 7, 1)
        self._entry(bar, "Username", self.b_user, 14, 2)
        self._entry(bar, "Password", self.b_pass, 14, 3, show="•")
        bar.columnconfigure(4, weight=1)
        actions = self._actions(bar, 5)
        self.b_state = ttk.Label(actions)
        self.b_state.pack(side="left", padx=(0, 10))
        self._set_status(self.b_state, "stopped", "idle")
        self.b_btn = ttk.Button(actions, text="Start broker", style="Accent.TButton",
                                command=self.toggle_broker)
        self.b_btn.pack(side="left")
        ttk.Label(bar, text="Leave username blank to allow anonymous clients.",
                  style="Hint.TLabel").grid(row=1, column=0, columnspan=6,
                                            sticky="w", pady=(6, 0))

        mid = ttk.Frame(f)
        mid.pack(fill="both", expand=True)
        mid.columnconfigure(1, weight=1)
        mid.rowconfigure(0, weight=1)

        lo, lf = ui_theme.card(mid, "Connected clients")
        lo.grid(row=0, column=0, sticky="nsew", padx=(0, 14))
        self.tree = ttk.Treeview(lf, columns=("peer", "subs"), show="tree headings")
        self.tree.heading("#0", text="CLIENT ID", anchor="w")
        self.tree.heading("peer", text="ADDRESS", anchor="w")
        self.tree.heading("subs", text="SUBS")
        self.tree.column("#0", width=150)
        self.tree.column("peer", width=150)
        self.tree.column("subs", width=56, anchor="center")
        self.tree.tag_configure("odd", background=p.surface)
        self.tree.pack(fill="both", expand=True)

        ro, rf = ui_theme.card(mid, "Broker log",
                               action=("Clear", lambda: self._clear(self.b_log)))
        ro.grid(row=0, column=1, sticky="nsew")
        self.b_log = ui_theme.log_view(rf, self.fonts, height=20, tag_colors={
            "error": p.danger, "connect": p.success, "disconnect": p.warning,
            "will": p.warning, "publish": p.info, "subscribe": p.accent_hover,
            "broker": p.text})

    def toggle_broker(self):
        if self.broker_thread:
            self.broker_thread.stop()
            self.broker_thread = None
            self.b_btn.config(text="Start broker", style="Accent.TButton")
            self._set_status(self.b_state, "stopped", "idle")
            self.tree.delete(*self.tree.get_children())
            self._update_stats()
            return
        auth = None
        if self.b_user.get().strip():
            auth = {self.b_user.get().strip(): self.b_pass.get()}
        try:
            bt = BrokerThread(self.b_host.get().strip(), int(self.b_port.get()),
                              auth, lambda k, v: self.events.put(("broker", k, v)))
            bt.start()
        except Exception as e:                            # noqa: BLE001
            messagebox.showerror("Broker failed to start", str(e))
            return
        self.broker_thread = bt
        self.b_btn.config(text="Stop broker", style="Danger.TButton")
        self._set_status(self.b_state, f"running on :{self.b_port.get()}", "ok")
        self._update_stats()

    # -- client tab ---------------------------------------------------------
    def _build_client_tab(self):
        p = ui_theme.PALETTE
        f = self.client_tab
        co, conn = ui_theme.card(f, "Connection")
        co.pack(fill="x", pady=(0, 14))

        self.c_host = tk.StringVar(value="127.0.0.1")
        self.c_port = tk.StringVar(value="1883")
        self.c_id = tk.StringVar(value=f"gui-{int(time.time()) % 10000}")
        self.c_user = tk.StringVar()
        self.c_pass = tk.StringVar()

        self._entry(conn, "Host", self.c_host, 16, 0)
        self._entry(conn, "Port", self.c_port, 7, 1)
        self._entry(conn, "Client ID", self.c_id, 16, 2)
        self._entry(conn, "Username", self.c_user, 12, 3)
        self._entry(conn, "Password", self.c_pass, 12, 4, show="•")
        conn.columnconfigure(5, weight=1)
        actions = self._actions(conn, 6)
        self.c_state = ttk.Label(actions)
        self.c_state.pack(side="left", padx=(0, 10))
        self._set_status(self.c_state, "offline", "idle")
        self.c_btn = ttk.Button(actions, text="Connect", style="Accent.TButton",
                                command=self.toggle_client)
        self.c_btn.pack(side="left")

        row = ttk.Frame(f)
        row.pack(fill="x", pady=(0, 14))
        row.columnconfigure(0, weight=2, uniform="io")
        row.columnconfigure(1, weight=3, uniform="io")

        so, sub = ui_theme.card(row, "Subscribe")
        so.grid(row=0, column=0, sticky="nsew", padx=(0, 14))
        self.s_topic = tk.StringVar(value="demo/#")
        self.s_qos = tk.IntVar(value=0)
        sub.columnconfigure(0, weight=1)
        self._entry(sub, "Topic filter", self.s_topic, 20, 0, sticky="we")
        self._qos_picker(sub, self.s_qos, 1)
        btns = ttk.Frame(sub, style="Card.TFrame")
        btns.grid(row=1, column=0, columnspan=2, sticky="w", pady=(8, 0))
        ttk.Button(btns, text="Subscribe", style="Accent.TButton",
                   command=self.do_subscribe).pack(side="left")
        ttk.Button(btns, text="Unsubscribe",
                   command=self.do_unsubscribe).pack(side="left", padx=(8, 0))
        self.subs_label = ttk.Label(sub, text="Active: —", style="Hint.TLabel",
                                    wraplength=380, justify="left")
        self.subs_label.grid(row=2, column=0, columnspan=2, sticky="w", pady=(12, 0))
        self.active_subs = set()

        po, pub = ui_theme.card(row, "Publish")
        po.grid(row=0, column=1, sticky="nsew")
        self.p_topic = tk.StringVar(value="demo/temperature")
        self.p_qos = tk.IntVar(value=0)
        self.p_retain = tk.BooleanVar(value=False)
        pub.columnconfigure(0, weight=1)
        self._entry(pub, "Topic", self.p_topic, 20, 0, sticky="we")
        self._qos_picker(pub, self.p_qos, 1)
        ttk.Checkbutton(pub, text="Retain", variable=self.p_retain
                        ).grid(row=0, column=2, sticky="sw", pady=(0, 4))
        box = self._labeled(pub, "Payload", 0, row=1, sticky="we", columnspan=2)
        self.p_body = tk.Text(box, height=3, width=40)
        ui_theme.style_text(self.p_body, self.fonts)
        self.p_body.pack(fill="x")
        self.p_body.insert("1.0", '{"value": 23.5, "unit": "C"}')
        ttk.Button(pub, text="Publish", style="Accent.TButton",
                   command=self.do_publish).grid(row=1, column=2, sticky="se", pady=(0, 4))

        lo, log = ui_theme.card(f, "Messages",
                                action=("Clear", lambda: self._clear(self.c_log)))
        lo.pack(fill="both", expand=True)
        self.c_log = ui_theme.log_view(log, self.fonts, height=12, tag_colors={
            "rx": p.info, "tx": p.success, "sys": p.muted, "error": p.danger})

    # -- client actions -----------------------------------------------------
    def toggle_client(self):
        if mqtt is None:
            messagebox.showerror("Missing dependency", "pip install paho-mqtt")
            return
        if self.client:
            try:
                self.client.disconnect()
                self.client.loop_stop()
            except Exception:
                pass
            self.client = None
            self._client_offline()
            return

        cid = self.c_id.get().strip() or None
        try:
            c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1,
                            client_id=cid, clean_session=True)
        except AttributeError:                            # paho 1.x
            c = mqtt.Client(client_id=cid, clean_session=True)

        if self.c_user.get().strip():
            c.username_pw_set(self.c_user.get().strip(), self.c_pass.get())
        c.will_set(f"clients/{cid}/status", b"offline", qos=0, retain=True)

        q = self.events
        c.on_connect = lambda cl, u, fl, rc: q.put(("client", "connect", rc))
        c.on_disconnect = lambda cl, u, rc: q.put(("client", "disconnect", rc))
        c.on_message = lambda cl, u, m: q.put(("client", "message", m))
        c.on_subscribe = lambda cl, u, mid, g: q.put(("client", "sys", f"SUBACK qos={list(g)}"))
        c.on_publish = lambda cl, u, mid: q.put(("client", "sys", f"PUBACK mid={mid}"))

        try:
            c.connect_async(self.c_host.get().strip(), int(self.c_port.get()), keepalive=30)
            c.loop_start()
        except Exception as e:                            # noqa: BLE001
            messagebox.showerror("Connect failed", str(e))
            return
        self.client = c
        self.c_btn.config(text="Disconnect", style="Danger.TButton")
        self._set_status(self.c_state, "connecting…", "warn")

    def _client_offline(self):
        self.connected = False
        self.active_subs.clear()
        self._refresh_subs()
        self.c_btn.config(text="Connect", style="Accent.TButton")
        self._set_status(self.c_state, "offline", "idle")

    def do_subscribe(self):
        if not self._need_client():
            return
        t = self.s_topic.get().strip()
        if not t:
            return
        self.client.subscribe(t, self.s_qos.get())
        self.active_subs.add(t)
        self._refresh_subs()
        self._log(self.c_log, "sys", f"SUBSCRIBE {t} qos={self.s_qos.get()}")

    def do_unsubscribe(self):
        if not self._need_client():
            return
        t = self.s_topic.get().strip()
        self.client.unsubscribe(t)
        self.active_subs.discard(t)
        self._refresh_subs()
        self._log(self.c_log, "sys", f"UNSUBSCRIBE {t}")

    def do_publish(self):
        if not self._need_client():
            return
        topic = self.p_topic.get().strip()
        body = self.p_body.get("1.0", "end-1c")
        if not topic:
            messagebox.showwarning("Publish", "Topic is required.")
            return
        info = self.client.publish(topic, body.encode("utf-8"),
                                   qos=self.p_qos.get(), retain=self.p_retain.get())
        self._log(self.c_log, "tx",
                  f"TX {topic} (qos {self.p_qos.get()}"
                  f"{', retain' if self.p_retain.get() else ''})  {body[:200]}")
        if info.rc != 0:
            self._log(self.c_log, "error", f"publish failed rc={info.rc}")

    def _need_client(self):
        if not (self.client and self.connected):
            messagebox.showinfo("Not connected", "Connect to a broker first.")
            return False
        return True

    def _refresh_subs(self):
        self.subs_label.config(
            text="Active: " + ("  ·  ".join(sorted(self.active_subs)) or "—"))

    # -- event pump ---------------------------------------------------------
    def _drain_events(self):
        try:
            while True:
                src, kind, val = self.events.get_nowait()
                if src == "broker":
                    self._broker_event(kind, val)
                else:
                    self._client_event(kind, val)
        except queue.Empty:
            pass
        except Exception:                                 # noqa: BLE001
            logging.exception("failed to process GUI event")
        self.after(100, self._drain_events)

    def _broker_event(self, kind, val):
        if kind == "clients":
            self.tree.delete(*self.tree.get_children())
            for i, (cid, peer, n) in enumerate(val):
                self.tree.insert("", "end", iid=cid, text=cid, values=(peer, n),
                                 tags=("odd",) if i % 2 else ())
        else:
            self._log(self.b_log, kind, f"{kind.upper():<11}{val}")
        if kind in ("clients", "publish"):
            self._update_stats()

    def _client_event(self, kind, val):
        if kind == "connect":
            if val == 0:
                self.connected = True
                self._set_status(self.c_state, "connected", "ok")
                self._log(self.c_log, "sys", "CONNACK accepted")
                for t in self.active_subs:                # resubscribe after reconnect
                    self.client.subscribe(t, self.s_qos.get())
            else:
                self._log(self.c_log, "error", f"connection refused rc={val}")
                self._set_status(self.c_state, f"refused ({val})", "err")
        elif kind == "disconnect":
            self.connected = False
            self._set_status(self.c_state, "offline", "err")
            self._log(self.c_log, "sys", f"disconnected rc={val}")
        elif kind == "message":
            m = val
            self._log(self.c_log, "rx",
                      f"RX {m.topic} (qos {m.qos}{', retain' if m.retain else ''})  "
                      f"{m.payload.decode('utf-8', 'replace')[:300]}")
        else:
            self._log(self.c_log, "sys", str(val))

    def _log(self, widget, tag, text):
        widget.config(state="normal")
        widget.insert("end", f"{time.strftime('%H:%M:%S')}  ", "ts")
        widget.insert("end", f"{text}\n", tag)
        if float(widget.index("end")) > 2000:
            widget.delete("1.0", "500.0")
        widget.see("end")
        widget.config(state="disabled")

    @staticmethod
    def _clear(widget):
        widget.config(state="normal")
        widget.delete("1.0", "end")
        widget.config(state="disabled")

    def on_close(self):
        if self.client:
            try:
                self.client.disconnect()
                self.client.loop_stop()
            except Exception:
                pass
        if self.broker_thread:
            self.broker_thread.stop()
        self.destroy()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    ui_theme.enable_hidpi()
    App().mainloop()