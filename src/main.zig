const std = @import("std");
const flags = @import("flags");
const zio = @import("zio");
const mqtt = @import("mqtt");
const deque = @import("deque.zig");
const Client = @import("./client.zig");

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

const tick_ms = 10;
const tick_ns = tick_ms * std.time.ns_per_ms;

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

    var io = try zio.IO.init(32, 0);
    defer io.deinit();

    for (0..cli.connections) |idx| {
        // Create a TCP client socket
        const client_conn = try posix.socket(
            addr.any.family,
            posix.SOCK.NONBLOCK | posix.SOCK.STREAM | posix.SOCK.CLOEXEC,
            posix.IPPROTO.TCP,
        );
        errdefer posix.close(client_conn);
        std.log.info("[{}] id={} connect to {any}", .{ client_conn, idx, addr });
        const client = Client.init(gpa, &io, client_conn, cli.keep_alive);
        client.connect(addr);
        // Accept
        for (0..32) |_| {
            try io.run_for_ns(50 * 1000);
            if (client.connected) {
                std.log.info("[{}] id={} connected", .{ client_conn, idx });
                break;
            }
        }
    }

    // Run the loop until there are no more completions.
    while (true) try io.run_for_ns(tick_ns);
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
