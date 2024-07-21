const std = @import("std");
const xev = @import("xev");

const Instant = std.time.Instant;
const mem = std.mem;
const net = std.net;
const posix = std.posix;
const assert = std.debug.assert;

const ConnMap = std.AutoHashMap(posix.socket_t, Connection);

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
};

const Connection = struct {
    completion: *xev.Completion,
    read_buffer: []u8,

    pub fn init(state: *ServerState, new_fd: posix.socket_t) Connection {
        const read_buffer = state.allocator.alloc(u8, 512) catch unreachable;
        const c_recv: *xev.Completion = state.allocator.create(xev.Completion) catch unreachable;
        c_recv.* = .{
            .op = .{
                .recv = .{
                    .fd = new_fd,
                    .buffer = .{ .slice = read_buffer },
                },
            },
            .userdata = state,
            .callback = recvCallback,
        };
        return .{ .completion = c_recv, .read_buffer = read_buffer };
    }

    pub fn deinit(self: *Connection, allocator: mem.Allocator) void {
        allocator.destroy(self.completion);
        allocator.free(self.read_buffer);
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
    loop.add(new_conn.completion);
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

    const fd = comp.op.recv.fd;
    const state = @as(*ServerState, @ptrCast(@alignCast(ud.?)));
    const read_len = result.recv catch {
        var pair = state.connections.fetchRemove(fd).?;
        pair.value.deinit(state.allocator);
        return .disarm;
    };
    const buffer = comp.op.recv.buffer.slice;
    std.log.info("Recv from {} ({}/{}): {s}", .{ fd, read_len, buffer.len, buffer[0..read_len] });
    return .rearm;
}
