const std = @import("std");
const flags = @import("flags");
const xev = @import("xev");
const mqtt = @import("mqtt");
const deque = @import("deque.zig");

const Instant = std.time.Instant;
const Utf8View = std.unicode.Utf8View;
const mem = std.mem;
const net = std.net;
const posix = std.posix;
const assert = std.debug.assert;
const Packet = mqtt.v3.Packet;
const Connect = mqtt.v3.Connect;

pub const std_options = .{
    // Set the log level to info
    .log_level = .info,
};

pub fn main() !void {
    var general_purpose_allocator = std.heap.GeneralPurposeAllocator(.{}){};
    const gpa = general_purpose_allocator.allocator();

    var args = try std.process.argsWithAllocator(gpa);
    defer args.deinit();

    const cli = flags.parse(&args, Cli, .{});
    const addrs = try std.net.getAddressList(gpa, cli.host, cli.port);
    std.log.info("Server: {s}:{}, addrs: {any}", .{ cli.host, cli.port, addrs.addrs });
    if (addrs.addrs.len == 0) {
        std.log.err("Can't resolve host: {s}", .{cli.host});
        return;
    }
    const addr = addrs.addrs[0];

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    for (0..cli.connections) |idx| {
        // Create a TCP client socket
        const client_conn = try posix.socket(
            addr.any.family,
            posix.SOCK.NONBLOCK | posix.SOCK.STREAM | posix.SOCK.CLOEXEC,
            0,
        );
        errdefer posix.close(client_conn);
        std.log.info("[{}] {} connect to {any}", .{ idx, client_conn, addr });
        const conn = Connection.init(gpa, idx, client_conn, cli.keep_alive);
        // Accept
        conn.ctrl_comp = .{
            .op = .{ .connect = .{ .socket = client_conn, .addr = addr } },
            .userdata = conn,
            .callback = connectCallback,
        };
        loop.add(&conn.ctrl_comp);
        for (0..50) |_| {
            loop.run(.once) catch unreachable;
            if (conn.connected) {
                std.log.info("[{}] {} connected", .{ idx, client_conn });
                break;
            }
        }
    }

    // Run the loop until there are no more completions.
    try loop.run(.until_done);
}

const Cli = struct {
    pub const name = "mqtt-bench";
    pub const help =
        \\A MQTT benchmark tool written in Zig.
        \\Based on io_uring, it is blazing fast!
    ;

    host: []const u8 = "localhost",
    port: u16 = 1883,
    keep_alive: u16 = 5,
    connections: u32 = 1000,

    pub const descriptions = .{
        .host = "Host address",
        .port = "Host port",
        .keep_alive = "Keep alive seconds",
        .connections = "The number of connections",
    };

    pub const switches = .{
        .host = 'H',
        .keep_alive = 'k',
        .connections = 'c',
    };
};

const PacketQueue = deque.Deque(Packet);

const Connection = struct {
    id: usize,
    fd: posix.socket_t,
    last_ping: i64 = 0,
    closing: bool = false,
    connected: bool = false,
    keep_alive: u16,
    client_id: [64]u8 = undefined,
    read_buf: [256]u8 = undefined,
    write_buf: [256]u8 = undefined,
    write_len: usize = 0,
    // TODO: sizeof(xev.Completion) == 256
    read_comp: xev.Completion = .{},
    write_comp: xev.Completion = .{},
    keep_alive_comp: xev.Completion = .{},
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

    pub fn write(self: *Connection, loop: *xev.Loop, pkt: Packet) void {
        if (self.write_comp.flags.state != .dead or self.write_len > 0) {
            std.log.info("add to pending: {any}, {}/{}", .{ pkt, self.write_comp.flags.state, self.write_len });
            self.pending_packets.pushBack(pkt) catch unreachable;
        } else {
            std.log.debug("write: {any}", .{pkt});
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

fn connectCallback(
    ud: ?*anyopaque,
    loop: *xev.Loop,
    _: *xev.Completion,
    result: xev.Result,
) xev.CallbackAction {
    const conn = @as(*Connection, @ptrCast(@alignCast(ud.?)));
    std.log.info("[connect][{}] result: {any}", .{ conn.fd, result });
    const ts_ms = std.time.milliTimestamp();
    const client_id = std.fmt.bufPrint(
        conn.client_id[0..],
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
    return .disarm;
}

fn recvCallback(
    ud: ?*anyopaque,
    loop: *xev.Loop,
    comp: *xev.Completion,
    result: xev.Result,
) xev.CallbackAction {
    std.log.debug("[  recv ] result: {any}", .{result});
    const recv = comp.op.recv;
    const conn = @as(*Connection, @ptrCast(@alignCast(ud.?)));
    if (conn.closing) {
        std.log.debug("[send] connection already closed", .{});
        return .disarm;
    }
    const read_len = result.recv catch {
        std.log.debug("close conn when recv", .{});
        conn.close(loop);
        return .disarm;
    };
    const pkt = Packet.decode(recv.buffer.slice[0..read_len]) catch unreachable;
    std.log.debug(
        "Recv from {} ({} bytes): {any}, {any}",
        .{ recv.fd, read_len, recv.buffer.slice[0..read_len], pkt },
    );
    if (!conn.connected) {
        if (pkt.connack.code != .accepted) {
            std.log.info("[{}] Invalid connack: {}", .{ conn.fd, pkt.connack.code });
            conn.close(loop);
            return .disarm;
        }
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
    std.log.debug("[  send ] result: {any}", .{result});
    const conn = @as(*Connection, @ptrCast(@alignCast(ud.?)));
    if (conn.closing) {
        std.log.debug("[send] connection already closed", .{});
        return .disarm;
    }
    const send = comp.op.send;
    const send_len = result.send catch {
        std.log.debug("close conn when send", .{});
        conn.close(loop);
        return .disarm;
    };
    if (comp.flags.state != .dead) {
        std.log.err(
            "[{}] Invalid state={}, send_len={}, write_len={}",
            .{ conn.fd, comp.flags.state, send_len, conn.write_len },
        );
    }
    std.log.debug(
        "Send   to {} ({} bytes): {any}",
        .{ send.fd, send_len, send.buffer.slice[0..send_len] },
    );

    if (conn.write_len != send_len) {
        std.log.err(
            "[{}] Invalid send len: {}/{} (send_len/write_len)",
            .{ conn.fd, send_len, conn.write_len },
        );
    }
    conn.write_len -= send_len;
    if (conn.write_len == 0) {

        // FIXME: this is a bug of libxev (backend = kqueue)
        comp.* = .{};

        if (conn.pending_packets.popFront()) |pkt| {
            conn.write(loop, pkt);
        }
        return .disarm;
    }
    return .rearm;
}

fn keepAliveCallback(
    ud: ?*anyopaque,
    loop: *xev.Loop,
    comp: *xev.Completion,
    result: xev.Result,
) xev.CallbackAction {
    std.log.debug("[ timer ] comp: {} result: {any}", .{ comp.flags.state, result });
    const conn = @as(*Connection, @ptrCast(@alignCast(ud.?)));
    if (conn.closing) {
        std.log.debug("[timer] connection already closed", .{});
        return .disarm;
    }
    conn.write(loop, .pingreq);
    const now_ms = std.time.milliTimestamp();
    const next_ms = conn.keep_alive * 1000;
    if (now_ms - conn.last_ping >= 1000 + next_ms) {
        std.log.err(
            "[{}] Ping delay more than 1 seconds ({}ms)",
            .{ conn.fd, now_ms - conn.last_ping },
        );
    }
    conn.last_ping = now_ms;
    loop.timer(&conn.keep_alive_comp, next_ms, conn, keepAliveCallback);
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
    std.log.info(
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
    std.log.info("[ cancel][{}] result: {any}", .{ conn.fd, result });
    conn.deinit();
    return .disarm;
}
