const std = @import("std");
const xev = @import("xev");

const Instant = std.time.Instant;
const mem = std.mem;
const net = std.net;
const posix = std.posix;
const assert = std.debug.assert;

pub fn main() !void {
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

    var general_purpose_allocator = std.heap.GeneralPurposeAllocator(.{}){};
    const gpa = general_purpose_allocator.allocator();
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
    buf: [521]u8,
    read_comp: xev.Completion,
    write_comp: xev.Completion,
    write_idx: usize = 0,

    server_state: *ServerState,

    pub fn init(server_state: *ServerState) *Connection {
        var conn = server_state.allocator.create(Connection) catch unreachable;
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
    const new_conn = Connection.init(state);
    new_conn.read_comp = .{
        .op = .{
            .recv = .{
                .fd = new_fd,
                .buffer = .{ .slice = new_conn.read_buf() },
            },
        },
        .userdata = new_conn,
        .callback = recvCallback,
    };
    loop.add(&new_conn.read_comp);
    state.connections.put(new_fd, new_conn) catch unreachable;

    return .rearm;
}

fn recvCallback(
    ud: ?*anyopaque,
    _: *xev.Loop,
    comp: *xev.Completion,
    result: xev.Result,
) xev.CallbackAction {
    std.log.info("Completion: {}, result: {any}", .{ comp.flags.state, result });

    const recv = comp.op.recv;
    const conn = @as(*Connection, @ptrCast(@alignCast(ud.?)));
    const read_len = result.recv catch {
        conn.server_state.remove(recv.fd);
        return .disarm;
    };
    std.log.info(
        "Recv from {} ({} bytes): {s}",
        .{ recv.fd, read_len, recv.buffer.slice[0..read_len] },
    );
    return .rearm;
}
