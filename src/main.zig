const std = @import("std");
const flags = @import("flags");
const xev = @import("xev");
const mqtt = @import("mqtt");

const Instant = std.time.Instant;
const Utf8View = std.unicode.Utf8View;
const mem = std.mem;
const net = std.net;
const posix = std.posix;
const assert = std.debug.assert;
const Packet = mqtt.v3.Packet;
const Connect = mqtt.v3.Connect;

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

    var conn = Connection.init(gpa, client_conn);
    // Accept
    conn.comp = .{
        .op = .{ .connect = .{ .socket = client_conn, .addr = addr } },
        .userdata = conn,
        .callback = connectCallback,
    };
    loop.add(&conn.comp);

    // Run the loop until there are no more completions.
    try loop.run(.until_done);
}

const Cli = struct {
    pub const name = "mqtt-bench";
    pub const help =
        \\A MQTT benchmark tool written in Zig.
        \\Based on io_uring, which is blazing fast!
    ;

    host: []const u8 = "test.mosquitto.org",
    port: u16 = 1883,

    pub const descriptions = .{
        .host = "Host address",
        .port = "Host port",
    };
};

const Connection = struct {
    fd: posix.socket_t,
    read_buf: [256]u8,
    write_buf: [256]u8,
    comp: xev.Completion,
    write_len: usize = 0,

    pub fn init(allocator: mem.Allocator, new_fd: posix.socket_t) *Connection {
        var conn = allocator.create(Connection) catch unreachable;
        conn.fd = new_fd;
        return conn;
    }

    pub fn deinit(self: *Connection) void {
        self.server_state.allocator.destroy(self);
    }

    pub fn read(self: *Connection, loop: *xev.Loop) void {
        self.comp = .{
            .op = .{
                .recv = .{
                    .fd = self.fd,
                    .buffer = .{ .slice = self.read_buf[0..] },
                },
            },
            .userdata = self,
            .callback = recvCallback,
        };
        loop.add(&self.comp);
    }

    pub fn write(self: *Connection, loop: *xev.Loop, buf: []u8) void {
        self.comp = .{
            .op = .{
                .send = .{
                    .fd = self.fd,
                    .buffer = .{ .slice = buf },
                },
            },
            .userdata = self,
            .callback = sendCallback,
        };
        loop.add(&self.comp);
    }

    pub fn close(self: *Connection, loop: *xev.Loop) void {
        self.comp = .{
            .op = .{ .close = .{ .fd = self.fd } },
            .userdata = self,
            .callback = closeCallback,
        };
        loop.add(&self.comp);
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
        .keep_alive = 120,
        .client_id = Utf8View.initUnchecked("sample"),
    } };
    var len: usize = 0;
    pkt.encode(conn.write_buf[0..], &len) catch unreachable;
    conn.write(loop, conn.write_buf[0..len]);
    return .disarm;
}

fn recvCallback(
    ud: ?*anyopaque,
    loop: *xev.Loop,
    comp: *xev.Completion,
    result: xev.Result,
) xev.CallbackAction {
    std.log.info("[   recv] result: {any}", .{result});

    const recv = comp.op.recv;
    const conn = @as(*Connection, @ptrCast(@alignCast(ud.?)));
    const read_len = result.recv catch {
        conn.close(loop);
        return .disarm;
    };
    const pkt = Packet.decode(recv.buffer.slice[0..read_len]) catch unreachable;
    std.log.info(
        "Recv from {} ({} bytes): {any}, {any}",
        .{ recv.fd, read_len, recv.buffer.slice[0..read_len], pkt },
    );
    conn.close(loop);
    return .disarm;
}

fn sendCallback(
    ud: ?*anyopaque,
    loop: *xev.Loop,
    comp: *xev.Completion,
    result: xev.Result,
) xev.CallbackAction {
    std.log.info("[   send] result: {any}", .{result});

    const send = comp.op.send;
    const conn = @as(*Connection, @ptrCast(@alignCast(ud.?)));
    const send_len = result.send catch {
        conn.close(loop);
        return .disarm;
    };
    std.log.info(
        "Send   to {} ({} bytes): {any}",
        .{ send.fd, send_len, send.buffer.slice[0..send_len] },
    );

    conn.write_len += send_len;
    if (conn.write_len >= send.buffer.slice.len) {
        conn.read(loop);
        conn.write_len = 0;
        return .disarm;
    }
    return .rearm;
}

fn closeCallback(
    _: ?*anyopaque,
    _: *xev.Loop,
    _: *xev.Completion,
    result: xev.Result,
) xev.CallbackAction {
    std.log.info("[  close] result: {any}", .{result});
    return .disarm;
}
