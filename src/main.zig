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

    // Create a TCP client socket
    const client_conn = try posix.socket(
        addr.any.family,
        posix.SOCK.NONBLOCK | posix.SOCK.STREAM | posix.SOCK.CLOEXEC,
        0,
    );
    errdefer posix.close(client_conn);

    std.log.info("Connect to {any}", .{addr});

    const conn = Connection.init(gpa, client_conn, cli.keep_alive);
    // Accept
    var connect_comp: xev.Completion = .{
        .op = .{ .connect = .{ .socket = client_conn, .addr = addr } },
        .userdata = conn,
        .callback = connectCallback,
    };
    loop.add(&connect_comp);

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

    pub const descriptions = .{
        .host = "Host address",
        .port = "Host port",
        .keep_alive = "Keep alive seconds",
    };
};

const PacketQueue = deque.Deque(Packet);

const Connection = struct {
    fd: posix.socket_t,
    closing: bool = false,
    keep_alive: u16,
    read_buf: [256]u8 = undefined,
    write_buf: [256]u8 = undefined,
    write_len: usize = 0,
    // TODO: sizeof(xev.Completion) == 256
    read_comp: xev.Completion = .{},
    write_comp: xev.Completion = .{},
    keep_alive_comp: xev.Completion = .{},
    exit_comp: xev.Completion = .{},
    pending_packets: PacketQueue,
    allocator: mem.Allocator,

    pub fn init(allocator: mem.Allocator, new_fd: posix.socket_t, keep_alive: u16) *Connection {
        const conn = allocator.create(Connection) catch unreachable;
        conn.* = .{
            .fd = new_fd,
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
            std.log.info("write: {any}", .{pkt});
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
            self.write_len = len;
            loop.add(&self.write_comp);
        }
    }

    pub fn close(self: *Connection, loop: *xev.Loop) void {
        self.closing = true;
        self.exit_comp = .{
            .op = .{ .close = .{ .fd = self.fd } },
            .userdata = self,
            .callback = closeCallback,
        };
        loop.add(&self.exit_comp);
    }
};

fn connectCallback(
    ud: ?*anyopaque,
    loop: *xev.Loop,
    _: *xev.Completion,
    result: xev.Result,
) xev.CallbackAction {
    std.log.info("[connect] result: {any}", .{result});

    const conn = @as(*Connection, @ptrCast(@alignCast(ud.?)));
    const pkt = Packet{ .connect = Connect{
        .protocol = .V311,
        .clean_session = true,
        .keep_alive = conn.keep_alive,
        .client_id = Utf8View.initUnchecked("sample"),
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
    // std.log.info("[  recv ] result: {any}", .{result});

    const recv = comp.op.recv;
    const conn = @as(*Connection, @ptrCast(@alignCast(ud.?)));
    if (conn.closing) {
        std.log.info("[send] connection already closed", .{});
        return .disarm;
    }
    const read_len = result.recv catch {
        std.log.info("close conn when recv", .{});
        conn.close(loop);
        return .disarm;
    };
    const pkt = Packet.decode(recv.buffer.slice[0..read_len]) catch unreachable;
    std.log.info(
        "Recv from {} ({} bytes): {any}, {any}",
        .{ recv.fd, read_len, recv.buffer.slice[0..read_len], pkt },
    );
    return .rearm;
}

fn sendCallback(
    ud: ?*anyopaque,
    loop: *xev.Loop,
    comp: *xev.Completion,
    result: xev.Result,
) xev.CallbackAction {
    // std.log.info("[  send ] result: {any}", .{result});

    const send = comp.op.send;
    const conn = @as(*Connection, @ptrCast(@alignCast(ud.?)));
    if (conn.closing) {
        std.log.info("[send] connection already closed", .{});
        return .disarm;
    }
    const send_len = result.send catch {
        std.log.info("close conn when send", .{});
        conn.close(loop);
        return .disarm;
    };
    std.log.info(
        "Send   to {} ({} bytes): {any}",
        .{ send.fd, send_len, send.buffer.slice[0..send_len] },
    );

    conn.write_len -= send_len;
    if (conn.write_len == 0) {
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
    std.log.info("[ timer ] comp: {} result: {any}", .{ comp.flags.state, result });
    const conn = @as(*Connection, @ptrCast(@alignCast(ud.?)));
    if (conn.closing) {
        std.log.info("[timer] connection already closed", .{});
        return .disarm;
    }
    conn.write(loop, .pingreq);
    loop.timer(comp, conn.keep_alive * 1000, conn, keepAliveCallback);
    return .disarm;
}

fn closeCallback(
    ud: ?*anyopaque,
    loop: *xev.Loop,
    _: *xev.Completion,
    result: xev.Result,
) xev.CallbackAction {
    std.log.info("[ close ] result: {any}", .{result});
    const conn = @as(*Connection, @ptrCast(@alignCast(ud.?)));
    conn.exit_comp = .{
        .op = .{ .cancel = .{ .c = &conn.keep_alive_comp } },
        .userdata = conn,
        .callback = cancelCallback,
    };
    loop.add(&conn.exit_comp);
    return .disarm;
}

fn cancelCallback(
    ud: ?*anyopaque,
    _: *xev.Loop,
    _: *xev.Completion,
    result: xev.Result,
) xev.CallbackAction {
    std.log.info("[ cancel] result: {any}", .{result});
    const conn = @as(*Connection, @ptrCast(@alignCast(ud.?)));
    conn.deinit();
    return .disarm;
}
