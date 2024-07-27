const std = @import("std");
const zio = @import("zio");
const mqtt = @import("mqtt");
const deque = @import("./deque.zig");

const posix = std.posix;
const log = std.log;
const mem = std.mem;
const Address = std.net.Address;
const Utf8View = std.unicode.Utf8View;
const IO = zio.IO;
const Packet = mqtt.v3.Packet;
const Connect = mqtt.v3.Connect;

const Self = @This();
const PacketQueue = deque.Deque(Packet);

// Connection data
io: *IO,
sock: posix.socket_t,
send_buf: [256]u8 = undefined,
recv_buf: [256]u8 = undefined,
send_len: usize = 0,
recv_completion: IO.Completion = undefined,
send_completion: IO.Completion = undefined,
ctrl_completion: IO.Completion = undefined,
// Client data
keep_alive: u16,
client_id: [64]u8 = undefined,
closing: bool = false,
connected: bool = false,
pending_packets: PacketQueue,
allocator: mem.Allocator,

pub fn init(
    allocator: mem.Allocator,
    io: *IO,
    sock: posix.socket_t,
    keep_alive: u16,
) *Self {
    const self = allocator.create(Self) catch unreachable;
    self.* = .{
        .io = io,
        .sock = sock,
        .keep_alive = keep_alive,
        .pending_packets = PacketQueue.init(allocator) catch unreachable,
        .allocator = allocator,
    };
    return self;
}

fn recv(self: *Self) void {
    self.io.recv(*Self, self, recv_callback, &self.recv_completion, self.sock, self.recv_buf[0..]);
}

fn send(self: *Self, pkt: Packet) void {
    if (self.send_len > 0) {
        log.info("[{}] add to pending: {any}, {}", .{ self.sock, pkt, self.send_len });
        self.pending_packets.pushBack(pkt) catch unreachable;
    } else {
        var len: usize = 0;
        pkt.encode(self.send_buf[0..], &len) catch unreachable;
        log.debug("[{}] send({}): {any}", .{ self.sock, len, pkt });
        self.io.send(
            *Self,
            self,
            send_callback,
            &self.send_completion,
            self.sock,
            self.send_buf[0..len],
        );
        if (len == 0) {
            @panic("zero len");
        }
        self.send_len = len;
    }
}

fn keep_alive_ns(self: *Self) u64 {
    return @as(u64, self.keep_alive) * 1000 * std.time.ns_per_ms;
}

fn timeout(self: *Self) void {
    const ns: u63 = @intCast(self.keep_alive_ns());
    self.io.timeout(*Self, self, timeout_callback, &self.ctrl_completion, ns);
}

pub fn connect(self: *Self, address: Address) void {
    self.io.connect(*Self, self, connect_callback, &self.ctrl_completion, self.sock, address);
    self.recv();
}

pub fn close(self: *Self) void {
    self.closing = true;
    self.io.close_socket(self.sock);
}

fn connect_callback(
    self: *Self,
    _: *IO.Completion,
    result: IO.ConnectError!void,
) void {
    _ = result catch @panic("connect error");
    const ts_ms = std.time.milliTimestamp();
    const client_id = std.fmt.bufPrint(
        self.client_id[0..],
        "{}-{}",
        .{ ts_ms, self.sock },
    ) catch unreachable;
    const pkt = Packet{ .connect = Connect{
        .protocol = .V311,
        .clean_session = true,
        .keep_alive = self.keep_alive,
        .client_id = Utf8View.initUnchecked(client_id),
    } };
    self.send(pkt);
    log.info("[{}] connected", .{self.sock});
}

fn send_callback(
    self: *Self,
    _: *IO.Completion,
    result: IO.SendError!usize,
) void {
    if (self.closing) {
        log.info("[{}] connection already closed (send)", .{self.sock});
        return;
    }
    const sent = result catch @panic("send error");
    if (self.send_len == 0) {
        log.err("[{}] self.send_len == 0, sent={}", .{ self.sock, sent });
        @panic("invalid sent len");
    }
    self.send_len -= sent;
    if (self.send_len == 0) {
        if (self.pending_packets.popFront()) |pkt| {
            self.send(pkt);
        }
    }
}

fn recv_callback(
    self: *Self,
    _: *IO.Completion,
    result: IO.RecvError!usize,
) void {
    if (self.closing) {
        log.info("[{}] connection already closed (recv)", .{self.sock});
        return;
    }

    const received = result catch @panic("recv error");
    const pkt = Packet.decode(self.recv_buf[0..received]) catch unreachable;
    log.debug("[{}] recv({}): {any}", .{ self.sock, received, pkt });
    if (!self.connected) {
        if (pkt.connack.code != .accepted) {
            log.info("[{}] Invalid connack: {}", .{ self.sock, pkt.connack.code });
            self.close();
        }
        log.info("[{}] accepted", .{self.sock});
        self.timeout();
        self.connected = true;
    }
    self.recv();
}

fn timeout_callback(
    self: *Self,
    completion: *IO.Completion,
    result: IO.TimeoutError!void,
) void {
    if (self.closing) {
        log.info("[{}] connection already closed (timeout)", .{self.sock});
        return;
    }

    _ = completion;
    _ = result catch @panic("timeout error");
    log.debug("[{}] timeout", .{self.sock});
    self.send(.pingreq);
    self.timeout();
}
