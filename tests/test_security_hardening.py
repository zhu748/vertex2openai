"""2026-11 安全加固回归（审查报告 S-1 ~ S-4）：

- S-1 Gemini 原生端点拒绝模型路径点分段（路径归一化逃逸）
- S-2 控制台写接口的 Origin/Host 一致性校验（共享注册域 CSRF）
- S-3 保活 URL 拒绝内嵌凭据与云元数据地址段
- S-4 /healthz 未鉴权仅回 status，部署形态详情挂鉴权
"""
import os
from pathlib import Path
import sys
import tempfile
import unittest

_STATE = tempfile.TemporaryDirectory(prefix="v2o-security-hardening-")
os.environ["STATE_DIR"] = _STATE.name
# 单进程模式（python scripts/check_project.py）没有 conftest 注入：与
# 其他测试文件一致，导入 main 前钉死鉴权口令与离线模型清单。
os.environ["API_KEY"] = "test-key"
os.environ["MODELS_CONFIG_URL"] = ""
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from fastapi.testclient import TestClient

import main
from keepalive import validate_keepalive_url


class KeepaliveUrlHardeningTests(unittest.TestCase):
    def test_userinfo_and_metadata_addresses_are_rejected(self):
        # S-3：内嵌凭据会原样进日志；云元数据/链路本地段可被伪造会话盲探。
        for bad in (
            "http://user:pass@example.com/healthz",
            "https://token@monitor.example.com/",
            "http://169.254.169.254/computeMetadata/v1/",
            "http://169.254.170.2/v2/credentials",
            "https://metadata.google.internal/",
            "https://[fe80::1]/healthz",
        ):
            with self.subTest(url=bad):
                self.assertFalse(validate_keepalive_url(bad))

    def test_loopback_and_private_targets_remain_valid(self):
        # 文档化的「自我保活」用法必须保留（操作者已鉴权，目标是自己实例）。
        for good in (
            "https://example.com/healthz",
            "http://127.0.0.1:8050/healthz",
            "http://10.0.0.5:9090/ping",
            "https://my-monitor.internal.local/beat",
            "",
        ):
            with self.subTest(url=good):
                self.assertTrue(validate_keepalive_url(good))


class HealthzDisclosureTests(unittest.TestCase):
    def test_unauthenticated_healthz_returns_status_only(self):
        # S-4：未鉴权只回 status；通道模式/凭证数量/代理出口不再披露。
        with TestClient(main.app) as client:
            payload = client.get("/healthz").json()
        self.assertEqual(payload, {"status": "ok"})

    def test_authenticated_healthz_returns_full_payload(self):
        import console_auth
        token = console_auth.issue_session()
        with TestClient(main.app) as client:
            payload = client.get("/healthz", cookies={console_auth.AUTH_COOKIE: token}).json()
        self.assertEqual(payload["status"], "ok")
        self.assertIn("mode", payload)
        self.assertIn("proxy", payload)


class ConsoleCsrfTests(unittest.TestCase):
    def test_cross_site_write_is_rejected_before_body_read(self):
        # S-2：共享注册域下 Lax 不拦跨站 POST——Origin 与 Host 不一致必须 403。
        with TestClient(main.app) as client:
            response = client.post(
                "/api/settings", json={"retry_max": 5},
                headers={"Origin": "https://evil.example.com", "Host": "my.instance.example.com"})
        self.assertEqual(response.status_code, 403)

    def test_same_origin_write_passes_csrf_gate(self):
        # 同源（或反代 X-Forwarded-Host）写请求放行（后续鉴权自行处理）。
        with TestClient(main.app) as client:
            same = client.post("/api/settings", json={},
                               headers={"Origin": "http://testserver"})
            self.assertNotEqual(same.status_code, 403)
            forwarded = client.post(
                "/api/settings", json={},
                headers={"Origin": "https://public.example.com",
                         "X-Forwarded-Host": "public.example.com"})
            self.assertNotEqual(forwarded.status_code, 403)

    def test_non_browser_write_without_origin_passes(self):
        # curl/SDK 不带 Origin：不受 CSRF 防线影响（鉴权另有防线）。
        with TestClient(main.app) as client:
            response = client.post("/api/settings", json={})
        self.assertNotEqual(response.status_code, 403)


class NativePathTraversalTests(unittest.TestCase):
    def test_dot_segments_in_model_path_are_rejected(self):
        # S-1：模型段的 ../ 会被 httpx 归一化，逃逸到 aiplatform 其他资源。
        # 用 %2E 编码绕过客户端归一化，验证**服务端**的独立防线（裸 socket
        # 客户端仍可发送字面量点分段）。
        with TestClient(main.app) as client:
            for model in (
                "%2E%2E/v1/projects/attacker/locations/global",
                "projects/p/%2E%2E/%2E%2E/locations/global",
                "%2E/gemini-3.6-flash",
                "models//gemini-3.6-flash",
                "a%2F..%2Fb",
            ):
                with self.subTest(model=model):
                    response = client.post(
                        f"/v1beta/models/{model}:generateContent",
                        json={"contents": [{"parts": [{"text": "hi"}]}]},
                        headers={"x-goog-api-key": "test-key"})
                    self.assertEqual(response.status_code, 400, model)

    def test_normal_model_paths_are_unaffected(self):
        with TestClient(main.app) as client:
            for model in ("gemini-3.6-flash",
                          "projects/my-project/locations/global/publishers/google/models/gemini-3.6-flash"):
                with self.subTest(model=model):
                    response = client.post(
                        f"/v1beta/models/{model}:generateContent",
                        json={"contents": [{"parts": [{"text": "hi"}]}]},
                        headers={"x-goog-api-key": "test-key"})
                    self.assertNotEqual(response.status_code, 400, model)


if __name__ == "__main__":
    unittest.main(verbosity=2)
