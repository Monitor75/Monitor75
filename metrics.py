import socket
import threading
import time
import random
import json
from typing import Dict, List, Optional

class GossipMember:
    def __init__(self, host: str, port: int, incarnation: int = 0, state: str = 'alive'):
        self.host = host
        self.port = port
        self.incarnation = incarnation
        self.state = state # 'alive', 'suspect', 'dead'

    def key(self) -> str:
        return f"{self.host}:{self.port}"

class GossipProtocol:
    def __init__(self, host: str, port: int, seed_nodes: Optional[List[str]] = None):
        self.host = host
        self.port = port
        self.members: Dict[str, GossipMember] = {
            f"{host}:{port}": GossipMember(host, port, 0, 'alive')
        }
        self.seed_nodes = seed_nodes or []
        self.lock = threading.Lock()
        self.running = True

        # UDP socket for receiving gossip
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((host, port))
        self.sock.settimeout(1.0)

        # Start threads
        threading.Thread(target=self._gossip_loop, daemon=True).start()
        threading.Thread(target=self._listen_loop, daemon=True).start()

    def stop(self):
        self.running = False
        self.sock.close()

    def _gossip_loop(self):
        while self.running:
            time.sleep(1) # gossip every second
            with self.lock:
                if len(self.members) <= 1:
                    # contact a seed node to join the cluster
                    for seed in self.seed_nodes:
                        self._send_membership(seed)
                else:
                    # pick a random member (excluding self)
                    targets = [k for k in self.members.keys() if k != f"{self.host}:{self.port}"]
                    if targets:
                        target = random.choice(targets)
                        self._send_membership(target)

    def _send_membership(self, target: str):
        """Send full membership list to target via UDP."""
        host, port = target.split(':')
        port = int(port)
        with self.lock:
            # Convert members to JSON-serializable format
            members_serializable = {
                k: {'host': v.host, 'port': v.port, 'incarnation': v.incarnation, 'state': v.state}
                for k, v in self.members.items()
            }
        data = json.dumps(members_serializable).encode()
        try:
            self.sock.sendto(data, (host, port))
        except Exception:
            pass

    def _listen_loop(self):
        while self.running:
            try:
                data, addr = self.sock.recvfrom(65535)
                self._handle_gossip(data, addr)
            except socket.timeout:
                continue
            except Exception:
                break

    def _handle_gossip(self, data: bytes, addr):
        """Merge received membership list with local view."""
        try:
            received = json.loads(data.decode())
        except:
            return
        with self.lock:
            for key, info in received.items():
                if key not in self.members:
                    # new node
                    self.members[key] = GossipMember(info['host'], info['port'],
                                                     info['incarnation'], info['state'])
                else:
                    local = self.members[key]
                    if info['incarnation'] > local.incarnation:
                        # remote has newer info
                        local.incarnation = info['incarnation']
                        local.state = info['state']
                    elif info['incarnation'] == local.incarnation and info['state'] != local.state:
                        # tie? propagate the more advanced state
                        if info['state'] == 'dead' and local.state != 'dead':
                            local.state = 'dead'
            # After merge, possibly broadcast changes
