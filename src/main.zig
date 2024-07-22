const std = @import("std");
const xev = @import("xev");
const flags = @import("flags");

const Instant = std.time.Instant;
const mem = std.mem;
const net = std.net;
const posix = std.posix;
const assert = std.debug.assert;

pub fn main() !void {
    var general_purpose_allocator = std.heap.GeneralPurposeAllocator(.{}){};
    const gpa = general_purpose_allocator.allocator();

    var args = try std.process.argsWithAllocator(gpa);
    defer args.deinit();

    const cli = flags.parse(&args, Cli, .{});
    std.log.info("Server: {s}:{}", .{ cli.host, cli.port });

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    // Create a TCP server socket
    const address = try net.Address.parseIp4("127.0.0.1", 3131);
    const kernel_backlog = 1;
    const ln = try posix.socket(address.any.family, posix.SOCK.STREAM | posix.SOCK.CLOEXEC, 0);
    errdefer posix.close(ln);
    try posix.setsockopt(ln, posix.SOL.SOCKET, posix.SO.REUSEADDR, &mem.toBytes(@as(c_int, 1)));
    try posix.bind(ln, &address.any, address.getOsSockLen());
    try posix.listen(ln, kernel_backlog);

    std.log.info("Listen on {any}", .{address});

    var state = ServerState{
        .connections = ConnMap.init(gpa),
        .allocator = gpa,
    };
    // Accept
    var c_accept: xev.Completion = .{
        .op = .{ .accept = .{ .socket = ln } },
        .userdata = &state,
        .callback = acceptCallback,
    };
    loop.add(&c_accept);

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

const ServerState = struct {
    connections: ConnMap,
    allocator: mem.Allocator,

    pub fn remove(self: *ServerState, fd: posix.socket_t) void {
        var pair = self.connections.fetchRemove(fd).?;
        pair.value.deinit();
    }
};

const ConnMap = std.AutoHashMap(posix.socket_t, *Connection);

const Connection = struct {
    fd: posix.socket_t,
    buf: [512]u8,
    comp: xev.Completion,
    write_len: usize = 0,

    server_state: *ServerState,

    pub fn init(server_state: *ServerState, new_fd: posix.socket_t) *Connection {
        var conn = server_state.allocator.create(Connection) catch unreachable;
        conn.fd = new_fd;
        conn.server_state = server_state;
        return conn;
    }

    pub fn deinit(self: *Connection) void {
        self.server_state.allocator.destroy(self);
    }

    pub fn read_buf(self: *Connection) []u8 {
        return self.buf[0..256];
    }
    pub fn write_buf(self: *Connection) []u8 {
        return self.buf[256..512];
    }

    pub fn read(self: *Connection, loop: *xev.Loop) void {
        self.comp = .{
            .op = .{
                .recv = .{
                    .fd = self.fd,
                    .buffer = .{ .slice = self.read_buf() },
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

fn acceptCallback(
    ud: ?*anyopaque,
    loop: *xev.Loop,
    comp: *xev.Completion,
    result: xev.Result,
) xev.CallbackAction {
    std.log.info("Completion: {}, result: {any}", .{ comp.flags.state, result });

    const new_fd = result.accept catch unreachable;
    var state = @as(*ServerState, @ptrCast(@alignCast(ud.?)));
    const new_conn = Connection.init(state, new_fd);
    new_conn.read(loop);
    state.connections.put(new_fd, new_conn) catch unreachable;
    return .rearm;
}

fn recvCallback(
    ud: ?*anyopaque,
    loop: *xev.Loop,
    comp: *xev.Completion,
    result: xev.Result,
) xev.CallbackAction {
    std.log.info("Completion: {}, result: {any}", .{ comp.flags.state, result });

    const recv = comp.op.recv;
    const conn = @as(*Connection, @ptrCast(@alignCast(ud.?)));
    const read_len = result.recv catch {
        conn.close(loop);
        return .disarm;
    };
    std.log.info(
        "Recv from {} ({} bytes): {s}",
        .{ recv.fd, read_len, recv.buffer.slice[0..read_len] },
    );
    conn.write(loop, recv.buffer.slice[0..read_len]);
    return .disarm;
}

fn sendCallback(
    ud: ?*anyopaque,
    loop: *xev.Loop,
    comp: *xev.Completion,
    result: xev.Result,
) xev.CallbackAction {
    std.log.info("Completion: {}, result: {any}", .{ comp.flags.state, result });

    const send = comp.op.send;
    const conn = @as(*Connection, @ptrCast(@alignCast(ud.?)));
    const send_len = result.send catch {
        conn.close(loop);
        return .disarm;
    };
    std.log.info(
        "Send   to {} ({} bytes): {s}",
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
    ud: ?*anyopaque,
    _: *xev.Loop,
    comp: *xev.Completion,
    result: xev.Result,
) xev.CallbackAction {
    std.log.info("Completion: {}, result: {any}", .{ comp.flags.state, result });
    const conn = @as(*Connection, @ptrCast(@alignCast(ud.?)));
    conn.server_state.remove(comp.op.close.fd);
    return .disarm;
}
