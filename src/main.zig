const std = @import("std");
const flags = @import("flags");
const xev = @import("xev");
const client = @import("./client.zig");

const Instant = std.time.Instant;
const net = std.net;
const posix = std.posix;
const assert = std.debug.assert;
const Connection = client.Connection;

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
            .callback = client.connectCallback,
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
    keep_alive: u16 = 10,
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
