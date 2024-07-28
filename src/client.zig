const deque = @import("deque.zig");
const xev = @import("xev");
const std = @import("std");
const mqtt = @import("mqtt");

const mem = std.mem;
const posix = std.posix;
const log = std.log;
const Utf8View = std.unicode.Utf8View;
const Pid = mqtt.types.Pid;
const Packet = mqtt.v3.Packet;
const Connect = mqtt.v3.Connect;
const Publish = mqtt.v3.Publish;
const Subscribe = mqtt.v3.Subscribe;
const Suback = mqtt.v3.Suback;
const FilterWithQoSListView = mqtt.v3.FilterWithQoSListView;

const PacketQueue = deque.Deque(Packet);

pub const Connection = struct {
    id: usize,
    fd: posix.socket_t,
    last_ping: i64 = 0,
    closing: bool = false,
    connected: bool = false,
    keep_alive: u16,
    pkt_buf: [64]u8 = undefined,
    topic_name_buf: [64]u8 = undefined,
    topic_name: []u8 = undefined,
    read_buf: [256]u8 = undefined,
    write_buf: [256]u8 = undefined,
    write_len: usize = 0,
    // TODO: sizeof(xev.Completion) == 256
    read_comp: xev.Completion = .{},
    write_comp: xev.Completion = .{},
    keep_alive_comp: xev.Completion = .{},
    timer_publish_comp: xev.Completion = .{},
    ctrl_comp: xev.Completion = .{},
    pending_packets: PacketQueue,
    allocator: mem.Allocator,

    pub fn init(
        allocator: mem.Allocator,
        id: usize,
        new_fd: posix.socket_t,
        keep_alive: u16,
    ) *Connection {
        const conn = allocator.create(Connection) catch unreachable;
        conn.* = .{
            .id = id,
            .fd = new_fd,
            .last_ping = std.time.milliTimestamp(),
            .keep_alive = keep_alive,
            .pending_packets = PacketQueue.init(allocator) catch unreachable,
            .allocator = allocator,
        };
        conn.topic_name = std.fmt.bufPrint(conn.topic_name_buf[0..], "testtopic/{}", .{conn.fd}) catch unreachable;
        return conn;
    }

    pub fn deinit(self: *Connection) void {
        self.pending_packets.deinit();
        self.allocator.destroy(self);
    }

    pub fn read(self: *Connection, loop: *xev.Loop) void {
        if (self.read_comp.flags.state == .dead) {
            self.read_comp = .{
                .op = .{
                    .recv = .{
                        .fd = self.fd,
                        .buffer = .{ .slice = self.read_buf[0..] },
                    },
                },
                .userdata = self,
                .callback = recvCallback,
            };
            loop.add(&self.read_comp);
        }
    }

    pub fn subscribe(self: *Connection, loop: *xev.Loop) void {
        const topic_buf = std.fmt.bufPrint(self.pkt_buf[0..], "testtopic/{}", .{@rem(self.fd, 10)}) catch unreachable;
        const topic = mqtt.types.TopicFilter.try_from(Utf8View.initUnchecked(topic_buf)) catch unreachable;
        const topic_qos = .{ .filter = topic, .qos = .level2 };
        const topics_view = FilterWithQoSListView.encode_to_view(&.{topic_qos}, self.pkt_buf[topic_buf.len..]);

        const pkt = Packet{ .subscribe = Subscribe{
            .pid = Pid.try_from(345) catch unreachable,
            .topics = topics_view,
        } };
        self.write(loop, pkt);
    }

    pub fn publish(self: *Connection, loop: *xev.Loop) void {
        const pkt = Packet{ .publish = Publish{
            .dup = false,
            .qos_pid = .level0,
            .retain = true,
            .topic_name = mqtt.types.TopicName.try_from(Utf8View.initUnchecked(self.topic_name)) catch unreachable,
            .payload = "hello",
        } };
        self.write(loop, pkt);
    }

    pub fn write(self: *Connection, loop: *xev.Loop, pkt: Packet) void {
        if (self.write_comp.flags.state != .dead or self.write_len > 0) {
            log.info("add to pending: {any}, {}/{}", .{ pkt, self.write_comp.flags.state, self.write_len });
            self.pending_packets.pushBack(pkt) catch unreachable;
        } else {
            log.debug("write: {any}", .{pkt});
            var len: usize = 0;
            pkt.encode(self.write_buf[0..], &len) catch unreachable;
            self.write_comp = .{
                .op = .{
                    .send = .{
                        .fd = self.fd,
                        .buffer = .{ .slice = self.write_buf[0..len] },
                    },
                },
                .userdata = self,
                .callback = sendCallback,
            };
            if (len == 0) {
                @panic("zero len");
            }
            self.write_len = len;
            loop.add(&self.write_comp);
        }
    }

    pub fn close(self: *Connection, loop: *xev.Loop) void {
        self.closing = true;
        self.ctrl_comp = .{
            .op = .{ .close = .{ .fd = self.fd } },
            .userdata = self,
            .callback = closeCallback,
        };
        loop.add(&self.ctrl_comp);
    }
};

pub fn connectCallback(
    ud: ?*anyopaque,
    loop: *xev.Loop,
    _: *xev.Completion,
    result: xev.Result,
) xev.CallbackAction {
    const conn = @as(*Connection, @ptrCast(@alignCast(ud.?)));
    log.info("[connect][{}] result: {any}", .{ conn.fd, result });
    const ts_ms = std.time.milliTimestamp();
    const client_id = std.fmt.bufPrint(
        conn.pkt_buf[0..],
        "{}-{}",
        .{ ts_ms, conn.fd },
    ) catch unreachable;
    const pkt = Packet{ .connect = Connect{
        .protocol = .V311,
        .clean_session = true,
        .keep_alive = conn.keep_alive,
        .client_id = Utf8View.initUnchecked(client_id),
    } };
    conn.read(loop);
    conn.write(loop, pkt);
    loop.timer(&conn.keep_alive_comp, conn.keep_alive * 1000, conn, keepAliveCallback);
    loop.timer(&conn.timer_publish_comp, 3333, conn, timerPublishCallback);
    return .disarm;
}

fn recvCallback(
    ud: ?*anyopaque,
    loop: *xev.Loop,
    comp: *xev.Completion,
    result: xev.Result,
) xev.CallbackAction {
    log.debug("[  recv ] result: {any}", .{result});
    const recv = comp.op.recv;
    const conn = @as(*Connection, @ptrCast(@alignCast(ud.?)));
    if (conn.closing) {
        log.debug("[send] connection already closed", .{});
        return .disarm;
    }
    const read_len = result.recv catch {
        log.debug("close conn when recv", .{});
        conn.close(loop);
        return .disarm;
    };
    const pkt = Packet.decode(recv.buffer.slice[0..read_len]) catch unreachable;
    log.debug("[{}] recv({}): {any}", .{ recv.fd, read_len, pkt });
    if (!conn.connected) {
        if (pkt.connack.code != .accepted) {
            log.info("[{}] Invalid connack: {}", .{ conn.fd, pkt.connack.code });
            conn.close(loop);
            return .disarm;
        }
        conn.subscribe(loop);
        conn.connected = true;
    }
    return .rearm;
}

fn sendCallback(
    ud: ?*anyopaque,
    loop: *xev.Loop,
    comp: *xev.Completion,
    result: xev.Result,
) xev.CallbackAction {
    log.debug("[  send ] result: {any}", .{result});
    const conn = @as(*Connection, @ptrCast(@alignCast(ud.?)));
    if (conn.closing) {
        log.debug("[send] connection already closed", .{});
        return .disarm;
    }
    const send = comp.op.send;
    const send_len = result.send catch {
        log.debug("close conn when send", .{});
        conn.close(loop);
        return .disarm;
    };
    if (comp.flags.state != .dead) {
        log.err(
            "[{}] Invalid state={}, send_len={}, write_len={}",
            .{ conn.fd, comp.flags.state, send_len, conn.write_len },
        );
    }
    log.debug(
        "Send   to {} ({} bytes): {any}",
        .{ send.fd, send_len, send.buffer.slice[0..send_len] },
    );

    if (conn.write_len != send_len) {
        log.err(
            "[{}] Invalid send len: {}/{} (send_len/write_len)",
            .{ conn.fd, send_len, conn.write_len },
        );
        @panic("Ghost send callback");
    }
    conn.write_len -= send_len;
    if (conn.write_len == 0) {
        // FIXME: this is a bug of libxev (backend = kqueue)
        // comp.* = .{};

        if (conn.pending_packets.popFront()) |pkt| {
            log.warn("[{}] popFront", .{conn.fd});
            conn.write(loop, pkt);
        }
        return .disarm;
    } else {
        log.warn("[{}] partially read {}/{}", .{ conn.fd, send_len, conn.write_len });
    }
    return .rearm;
}

fn keepAliveCallback(
    ud: ?*anyopaque,
    loop: *xev.Loop,
    comp: *xev.Completion,
    result: xev.Result,
) xev.CallbackAction {
    log.debug("[ timer ] comp: {} result: {any}", .{ comp.flags.state, result });
    const conn = @as(*Connection, @ptrCast(@alignCast(ud.?)));
    if (conn.closing) {
        log.debug("[timer] connection already closed", .{});
        return .disarm;
    }
    conn.write(loop, .pingreq);
    const now_ms = std.time.milliTimestamp();
    const next_ms = conn.keep_alive * 1000;
    if (now_ms - conn.last_ping >= 1000 + next_ms) {
        log.err(
            "[{}] Ping delay more than 1 seconds ({}ms)",
            .{ conn.fd, now_ms - conn.last_ping },
        );
    }
    conn.last_ping = now_ms;
    loop.timer(&conn.keep_alive_comp, next_ms, conn, keepAliveCallback);
    return .disarm;
}

fn timerPublishCallback(
    ud: ?*anyopaque,
    loop: *xev.Loop,
    _: *xev.Completion,
    _: xev.Result,
) xev.CallbackAction {
    const conn = @as(*Connection, @ptrCast(@alignCast(ud.?)));
    if (conn.closing) {
        log.debug("[timer] connection already closed", .{});
        return .disarm;
    }
    conn.publish(loop);
    loop.timer(&conn.timer_publish_comp, 3333, conn, timerPublishCallback);
    return .disarm;
}

fn closeCallback(
    ud: ?*anyopaque,
    loop: *xev.Loop,
    _: *xev.Completion,
    result: xev.Result,
) xev.CallbackAction {
    const conn = @as(*Connection, @ptrCast(@alignCast(ud.?)));
    const now_ms = std.time.milliTimestamp();
    log.info(
        "[ close ][{}] result: {any}, last_ping: {}, now_ms: {} ({})",
        .{ conn.fd, result, conn.last_ping, now_ms, now_ms - conn.last_ping },
    );
    conn.ctrl_comp = .{
        .op = .{ .cancel = .{ .c = &conn.keep_alive_comp } },
        .userdata = conn,
        .callback = cancelCallback,
    };
    loop.add(&conn.ctrl_comp);
    return .disarm;
}

fn cancelCallback(
    ud: ?*anyopaque,
    _: *xev.Loop,
    _: *xev.Completion,
    result: xev.Result,
) xev.CallbackAction {
    const conn = @as(*Connection, @ptrCast(@alignCast(ud.?)));
    log.info("[ cancel][{}] result: {any}", .{ conn.fd, result });
    conn.deinit();
    return .disarm;
}
