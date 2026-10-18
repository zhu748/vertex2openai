"""Express SDK 与原生 REST 共用的模型资源路径和钉定失败判断。"""

from typing import Any

from runtime_state import app_state


def resolve_express_model_path(base_model_name: str, settings: dict, key_project_id: str = "") -> str:
    """保留完整资源路径，或按当前 Key 的项目和配置区域钉定裸模型名。

    项目优先级为 Key 自己的 Project ID、全局 Project ID；项目或区域为空时
    返回裸模型名，由 Express 后端路由，不给 SDK Client 传互斥的项目参数。
    """
    if base_model_name.startswith(("projects/", "publishers/", "models/", "tunedModels/", "endpoints/")):
        return base_model_name  # 客户端已自带完整路径，尊重它

    location = str(settings.get("express_location", "") or "").strip()
    if not location:
        return base_model_name

    project = (str(key_project_id or "").strip() or app_state.get_project_id() or "").strip()
    if not project:
        print(
            "⚠️ [上游端点] 已选择钉定 location，但该 Key 没有可用的 Project ID"
            "（请在控制台「通道与凭证」页触发探测或手动填写该 Key 的 Project ID，"
            "也可设环境变量 GOOGLE_PROJECT_ID 兜底），"
            "本次退回默认路由。"
        )
        return base_model_name

    return f"projects/{project}/locations/{location}/publishers/google/models/{base_model_name}"


def is_location_pin_failure(err: Any) -> bool:
    """错误是否像"钉定的 projects/locations 路径不对"（而非模型或网络本身的问题）。

    典型两种：
      404 `Publisher model projects/X/locations/Y/... was not found`  → 该项目/区域没有这个模型
      403 `This API method requires billing to be enabled ... project #X` → 项目没开计费/无权
    命中时可以把模型名退回**裸模型名**再试一次，让后端自行路由——
    这样即使用户填的 Project ID 与 API Key 不属于同一项目，也只是回到旧行为，不会更糟。
    """
    e = str(err).lower()
    if "requires billing" in e or "billing to be enabled" in e:
        return True
    if ("not found" in e or "404" in e) and "publisher model" in e:
        return True
    if "permission_denied" in e and "projects/" in e:
        return True
    return False
