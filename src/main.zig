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
    read_comp: *xev.Completion,
    read_buf: []u8,
    write_comp: *xev.Completion,
    write_buf: []u8,
    write_idx: usize = 0,

    pub fn init(state: *ServerState, new_fd: posix.socket_t) Connection {
        const read_buf = state.allocator.alloc(u8, 256) catch unreachable;
        const write_buf = state.allocator.alloc(u8, 256) catch unreachable;
        const read_comp: *xev.Completion = state.allocator.create(xev.Completion) catch unreachable;
        const write_comp: *xev.Completion = state.allocator.create(xev.Completion) catch unreachable;
        read_comp.* = .{
            .op = .{
                .recv = .{
                    .fd = new_fd,
                    .buffer = .{ .slice = read_buf },
                },
            },
            .userdata = state,
            .callback = recvCallback,
        };
        return .{
            .read_comp = read_comp,
            .read_buf = read_buf,
            .write_comp = write_comp,
            .write_buf = write_buf,
        };
    }

    pub fn deinit(self: *Connection, allocator: mem.Allocator) void {
        allocator.destroy(self.read_comp);
        allocator.destroy(self.write_comp);
        allocator.free(self.read_buf);
        allocator.free(self.write_buf);
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
    loop.add(new_conn.read_comp);
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
    const state = @as(*ServerState, @ptrCast(@alignCast(ud.?)));
    const read_len = result.recv catch {
        var pair = state.connections.fetchRemove(recv.fd).?;
        pair.value.deinit(state.allocator);
        return .disarm;
    };
    std.log.info(
        "Recv from {} ({} bytes): {s}",
        .{ recv.fd, read_len, recv.buffer.slice[0..read_len] },
    );
    return .rearm;
}
