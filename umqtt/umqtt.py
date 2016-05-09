import usocket as socket
import ustruct as struct
from ubinascii import hexlify
import time

class MQTTClient:

    def __init__(self, client_id, server, port=1883):
        self.client_id = client_id
        self.sock = socket.socket()
        self.addr = socket.getaddrinfo(server, port)[0][-1]
        self.pid = 0

    def send_str(self, s):
        self.sock.send(struct.pack("!H", len(s)))
        self.sock.send(s)

    def connect(self):
        self.sock.connect(self.addr)
        msg = bytearray(b"\x10\0\0\x04MQTT\x04\x02\0\0")
        msg[1] = 10 + 2 + len(self.client_id)
        self.sock.send(msg)
        print(hex(len(msg)), hexlify(msg, ":"))
        self.send_str(self.client_id)
        resp = self.sock.recv(4)
        assert resp == b"\x20\x02\0\0", resp

    def disconnect(self):
        self.sock.send(b"\xe0\0")
        self.sock.close()

    def publish(self, topic, msg, qos=0, retain=False):
        assert qos == 0
        pkt = bytearray(b"\x30\0")
        pkt[0] |= qos << 1 | retain
        pkt[1] = 2 + len(topic) + len(msg)
        print(hex(len(pkt)), hexlify(pkt, ":"))
        self.sock.send(pkt)
        self.send_str(topic)
        self.sock.send(msg)

    def subscribe(self, topic):
        pkt = bytearray(b"\x82\0\0\0")
        self.pid += 1
        struct.pack_into("!BH", pkt, 1, 2 + 2 + len(topic) + 1, self.pid)
        print(hex(len(pkt)), hexlify(pkt, ":"))
        self.sock.send(pkt)
        self.send_str(topic)
        self.sock.send(b"\0")
        resp = self.sock.recv(5)
        print(resp)
        assert resp[0] == 0x90
        assert resp[2] == pkt[2] and resp[3] == pkt[3]
        assert resp[4] == 0

    def wait_msg(self):
        res = self.sock.read(1)
        if res is None:
            return None
        self.sock.setblocking(True)
        assert res == b"\x30"
        sz = self.sock.recv(1)[0]
        topic_len = self.sock.recv(2)
        topic_len = (topic_len[0] << 8) | topic_len[1]
        topic = self.sock.recv(topic_len)
        msg = self.sock.recv(sz - topic_len - 2)
        return (topic, msg)

    def check_msg(self):
        self.sock.setblocking(False)
        return self.wait_msg()
