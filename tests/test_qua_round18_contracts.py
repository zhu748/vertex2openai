"""第十八轮（收尾轮）契约测试：QUA 域 7 项 + 环境变量集中层。

覆盖 2026-11 全面审查收尾轮的行为保持型收敛：
- QUA-P2-1  request_errors 三协议分派单点化（形状对齐旧三函数）
- QUA-P2-2  register_call_signature 三态写入单点
- QUA-P2-3  merge_by_order 交错重组（重放/去重/越界/两通道兜底顺序）
- QUA-P2-4  estimate_text_tokens 统一到 usage 单实现（向上取整 + 下限 1）
- QUA-P2-7  config 环境读取集中层（布尔口径/数值容错/STATE_DIR 单点）
- QUA-P2-10 log 门面复用推流管线（级别前缀）
- print 基线闸的计数口径（注释/字符串不计）

Go 侧（ENG-P3-1/2/3/5/6）在 proxybridge/sstp_packets_test.go 与
socks_test.go 内验证，不在此文件重复。
"""

import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import config as app_config
from claude_protocol import estimate_text_tokens
from request_errors import (
    _error_builder_for,
    body_too_large_response,
    ingress_error_response,
    validation_error_response,
)
from signature_store import SignatureRecord, SignatureState, SignatureStore
from thought_signatures import merge_by_order
from usage import estimate_token_count


class MergeByOrderContracts(unittest.TestCase):
    """QUA-P2-3：part_order 交错重组的泛型单实现。"""

    def test_replay_interleaves_and_dedups(self):
        order = [
            {"type": "ordinary", "index": 0},
            {"type": "tool_call", "index": 1},
            {"type": "ordinary", "index": 1},
            {"type": "tool_call", "index": 1},   # 重复索引：剔除
            {"type": "tool_call", "index": 99},  # 越界：剔除
            {"type": "weird"},                   # 未知类型：剔除
            "not-a-dict",
        ]
        merged = merge_by_order(order, ["T0", "T1"], ["O0", "O1"],
                                remainder_calls_first=True)
        self.assertEqual(merged, ["O0", "T1", "O1", "T0"])
        # remainder_calls_first=False：剩余普通块在前（SDK 通道历史行为）
        merged = merge_by_order(order, ["T0", "T1"], ["O0", "O1"],
                                remainder_calls_first=False)
        self.assertEqual(merged, ["O0", "T1", "O1", "T0"])
        # 无剩余块时两种兜底顺序结果一致
        full = [{"type": "tool_call", "index": 0}, {"type": "ordinary", "index": 0}]
        self.assertEqual(merge_by_order(full, ["T"], ["O"], remainder_calls_first=True),
                         ["T", "O"])
        self.assertEqual(merge_by_order(full, ["T"], ["O"], remainder_calls_first=False),
                         ["T", "O"])

    def test_remainder_order_differs_between_channels(self):
        order = [{"type": "ordinary", "index": 0}]
        # Cookie 通道：剩余调用在前
        self.assertEqual(merge_by_order(order, ["T0", "T1"], ["O0"],
                                        remainder_calls_first=True),
                         ["O0", "T0", "T1"])
        # SDK 通道：剩余普通块在前（已消费的 O0 不重复出现）
        self.assertEqual(merge_by_order(order, ["T0"], ["O0", "O1"],
                                        remainder_calls_first=False),
                         ["O0", "O1", "T0"])

    def test_non_list_returns_none_for_caller_fallback(self):
        self.assertIsNone(merge_by_order(None, ["T"], ["O"]))
        self.assertIsNone(merge_by_order("x", ["T"], ["O"]))
        self.assertIsNone(merge_by_order({}, ["T"], ["O"]))


class RegisterCallSignatureContracts(unittest.TestCase):
    """QUA-P2-2：三态拓扑写入收敛到 SignatureStore 单点。"""

    def _fresh(self):
        return SignatureStore(ttl_seconds=60, max_entries=8)

    def test_three_states(self):
        store = self._fresh()
        store.register_call_signature("signed", b"\x01\x02")
        store.register_call_signature("follower", None, unsigned_follower=True)
        store.register_call_signature("unknown", None)
        store.register_call_signature("unknown-default", b"")  # 空签名 = 无签名
        self.assertEqual(store.get_record("signed"),
                         SignatureRecord(SignatureState.SIGNED, b"\x01\x02"))
        self.assertEqual(store.get_record("follower").state,
                         SignatureState.UNSIGNED_FOLLOWER)
        self.assertEqual(store.get_record("unknown").state, SignatureState.UNKNOWN)
        self.assertEqual(store.get_record("unknown-default").state,
                         SignatureState.UNKNOWN)

    def test_signed_wins_over_follower_flag(self):
        # 有签名字节时 unsigned_follower 不参与（与旧两处实现的 if/elif 顺序一致）
        store = self._fresh()
        store.register_call_signature("both", b"sig", unsigned_follower=True)
        self.assertEqual(store.get_record("both").state, SignatureState.SIGNED)


class EstimateTextTokensContracts(unittest.TestCase):
    """QUA-P2-4：CJK token 估算统一到 usage 单实现。"""

    def test_unified_formula(self):
        self.assertEqual(estimate_text_tokens(""), 0)
        self.assertEqual(estimate_text_tokens("ABCD"), 1)          # 4 ASCII / 4
        self.assertEqual(estimate_text_tokens("你好"), 2)          # 2 CJK = 2
        # 非 CJK 部分向上取整：5 字符 = 2（旧银行家舍入会给 1，两口径分叉点）
        self.assertEqual(estimate_text_tokens("ABCDE"), 2)
        self.assertEqual(estimate_text_tokens("ABCDE"), estimate_token_count("ABCDE"))
        # 下限 1 语义保留在本层
        self.assertEqual(estimate_text_tokens("A"), 1)


class EnvLayerContracts(unittest.TestCase):
    """QUA-P2-7：config 集中读取层的解析口径。"""

    def test_env_flag_matrix(self):
        with mock.patch.dict(app_config.os.environ, {
                "V1": "1", "V2": "True", "V3": " YES ", "V4": "on",
                "V5": "0", "V6": "false", "V7": "", "V8": "yes-please"}):
            for name in ("V1", "V2", "V3", "V4"):
                self.assertTrue(app_config.env_flag(name), name)
            for name in ("V5", "V6", "V7", "V8"):
                self.assertFalse(app_config.env_flag(name), name)
            self.assertFalse(app_config.env_flag("V_MISSING"))

    def test_env_numbers_tolerate_garbage(self):
        with mock.patch.dict(app_config.os.environ, {"N1": "12", "N2": "x",
                                                     "F1": "1.5", "F2": "bad"}):
            self.assertEqual(app_config.env_int("N1", 7), 12)
            self.assertEqual(app_config.env_int("N2", 7), 7)
            self.assertEqual(app_config.env_int("N_MISSING", 7), 7)
            self.assertEqual(app_config.env_float("F1", 30.0), 1.5)
            self.assertEqual(app_config.env_float("F2", 30.0), 30.0)

    def test_state_dir_single_point(self):
        # 四个历史调用点（runtime_state / image_delivery / proxy_resolver×2）
        # 全部经 config.state_dir；默认与显式空串都回落 "."。
        with mock.patch.dict(app_config.os.environ, {}, clear=False):
            app_config.os.environ.pop("STATE_DIR", None)
            self.assertEqual(app_config.state_dir(), ".")
        with mock.patch.dict(app_config.os.environ, {"STATE_DIR": "  "}):
            self.assertEqual(app_config.state_dir(), ".")
        with mock.patch.dict(app_config.os.environ, {"STATE_DIR": "/tmp/x "}):
            self.assertEqual(app_config.state_dir(), "/tmp/x")


class RequestErrorShapeContracts(unittest.TestCase):
    """QUA-P2-1：单点分派后三协议错误形状与旧实现逐字段一致。"""

    def test_builder_kinds(self):
        self.assertEqual(_error_builder_for("/v1/messages")[0], "anthropic")
        self.assertEqual(_error_builder_for("/v1/messages/count_tokens")[0], "anthropic")
        self.assertEqual(_error_builder_for("/v1/models/x:generateContent")[0], "gemini")
        self.assertEqual(_error_builder_for("/v1beta/models/x")[0], "gemini")
        self.assertEqual(_error_builder_for("/v1/chat/completions")[0], "openai")
        self.assertEqual(_error_builder_for("/api/whatever")[0], "openai")

    def test_openai_param_sentinel_serializes_null(self):
        # 有字段定位时 param = 字段路径；无 loc 时显式序列化 null（键保留）
        chat = validation_error_response("/v1/chat/completions", [
            {"type": "missing", "loc": ["messages", 0], "msg": "Field required"}])
        self.assertIn(b'"param":"messages.0"', chat.body)
        self.assertEqual(chat.status_code, 422)
        no_field = validation_error_response("/v1/chat/completions", [
            {"type": "value_error", "loc": [], "msg": "bad"}])
        self.assertIn(b'"param":null', no_field.body)
        # 非 chat 管理端点不附 error 对象
        other = validation_error_response("/api/settings", [
            {"type": "missing", "loc": ["x"], "msg": "Field required"}])
        self.assertNotIn(b'"error"', other.body)
        # anthropic / gemini 保持 400
        self.assertEqual(validation_error_response("/v1/messages", [
            {"type": "missing", "loc": [], "msg": "x"}]).status_code, 400)
        self.assertEqual(validation_error_response("/v1/models/m:gen", [
            {"type": "missing", "loc": [], "msg": "x"}]).status_code, 400)

    def test_ingress_headers_and_shapes(self):
        response = ingress_error_response("/v1/chat/completions", 503, "busy")
        self.assertEqual(response.headers["cache-control"], "no-store")
        self.assertEqual(response.headers["retry-after"], "1")
        self.assertIn(b'"overloaded_error"', response.body)
        # Gemini 408 显式 DEADLINE_EXCEEDED（默认映射无 408 项）
        gem = ingress_error_response("/v1/models/m:gen", 408, "slow")
        self.assertIn(b"DEADLINE_EXCEEDED", gem.body)
        # 413 三协议同码
        self.assertEqual(body_too_large_response("/v1/messages", 1024).status_code, 413)
        self.assertIn(b"RESOURCE_EXHAUSTED",
                      body_too_large_response("/v1/models/m:gen", 32 * 1024 * 1024).body)


class LogFacadeContracts(unittest.TestCase):
    """QUA-P2-10：log 门面只加级别前缀，复用既有 print 推流管线。"""

    def test_level_prefixes(self):
        import logger
        captured = []
        with mock.patch.object(logger, "custom_print", side_effect=captured.append):
            logger.log.debug("d")
            logger.log.info("i")
            logger.log.warn("w")
            logger.log.error("e")
        self.assertEqual(captured, ["[DEBUG] d", "[INFO] i", "[WARN] w", "[ERROR] e"])


class PrintBaselineCounterContracts(unittest.TestCase):
    """print 基线闸的计数口径：机制文件豁免、注释/字符串不计。"""

    def test_counter_skips_comments_and_string_literals(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "check_print_baseline",
            Path(__file__).resolve().parents[1] / "scripts" / "check_print_baseline.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "sample.py"
            p.write_text(
                "print('real')\n"
                "# print('comment only')\n"
                "msg = \"print('inside string')\"\n"
                "x = pprintrint(1)\n", encoding="utf-8")
            self.assertEqual(module.count_prints(p), 1)
            self.assertIn("app/logger.py", module.EXEMPT)


if __name__ == "__main__":
    unittest.main()
