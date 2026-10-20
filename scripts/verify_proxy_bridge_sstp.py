"""Optional real SSTP helper-process check; see docs/proxy-turn-sstp.md."""

import ipaddress
import json
import os
from pathlib import Path
import socket
import sys
import tempfile


def receive(conn, size):
    data = bytearray()
    while len(data) < size:
        chunk = conn.recv(size - len(data))
        if not chunk:
            raise RuntimeError("SOCKS5 connection closed before the full reply")
        data.extend(chunk)
    return bytes(data)


def main():
    config = json.loads(Path(os.environ["SSTP_TEST_CONFIG"]).read_text())
    host, port = os.environ["SSTP_TEST_TARGET"].rsplit(":", 1)
    target_ip = ipaddress.IPv4Address(host)
    with tempfile.TemporaryDirectory(prefix="v2o-sstp-process-") as directory:
        os.environ["STATE_DIR"] = directory
        root = Path(__file__).resolve().parents[1]
        sys.path.insert(0, str(root / "app"))
        from proxy_config import build_proxy_uri
        from proxy_resolver import ProxyBridgeManager, ProxyResolveError, ProxyUsage, current_proxy_usage

        options = {"sni": config.get("tls_server_name") or config["server"]}
        for source, option in (("ca_file", "ca"), ("mtu", "mtu"), ("insecure", "insecure")):
            if config.get(source):
                options[option] = config[source]

        def uri(password):
            return build_proxy_uri("sstp", config["server"], config["server_port"],
                                   config["username"], password, options=options)

        manager, usage = ProxyBridgeManager(), ProxyUsage()
        token = current_proxy_usage.set(usage)
        try:
            node = uri(config["password"])
            local = manager.get_local_socks5(node)
            assert manager.get_local_socks5(node) == local
            assert len(manager._instances) == 1
            instance = manager._instances[node]
            assert not Path(instance.config_path).exists()
            with socket.create_connection(("127.0.0.1", instance.port), timeout=5) as conn:
                conn.sendall(b"\x05\x01\x00")
                assert receive(conn, 2) == b"\x05\x00"
                conn.sendall(b"\x05\x01\x00\x01" + target_ip.packed + int(port).to_bytes(2, "big"))
                assert receive(conn, 10)[1] == 0
                payload = b"sstp-process-stream" * 1024
                conn.sendall(payload)
                assert receive(conn, len(payload)) == payload
                instance.last_used = 0
                assert manager.reap_idle() == 0
            try:
                manager.get_local_socks5(uri(config["password"] + "-incorrect"))
            except ProxyResolveError as exc:
                assert "MS-CHAPv2" in str(exc)
                assert config["password"] not in str(exc)
            else:
                raise AssertionError("Incorrect SSTP credentials were accepted")
        finally:
            current_proxy_usage.reset(token)
            usage.close()
            manager.shutdown_all()
        assert not instance.alive()
        assert not list(Path(directory).rglob("*.json"))
        print("SSTP helper process: TLS/PPP, SOCKS5 forwarding, reuse, active-use retention, rejected credentials and shutdown passed")


if __name__ == "__main__":
    main()
