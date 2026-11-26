"""Project standard Gemini Parts from Studio; never execute their payloads."""

from typing import Any

from response_validation import IncompleteResponseError


_INLINE = {"mimeType": str, "data": str, "displayName": str}
_FILE = {"fileUri": str, "mimeType": str, "displayName": str}
_FUNCTION_RESULT = {"id": str, "name": str, "response": dict, "parts": list,
                    "willContinue": bool, "scheduling": str}
_PAYLOAD_FIELDS = {
    "toolCall": {"id": str, "toolType": str, "args": dict},
    "toolResponse": {"id": str, "toolType": str, "response": dict},
    "executableCode": {"id": str, "language": str, "code": str},
    "codeExecutionResult": {"id": str, "outcome": str, "output": str},
}
_METADATA_FIELDS = {
    "videoMetadata": {"startOffset": str, "endOffset": str, "fps": (int, float)},
    "mediaResolution": {"level": str, "numTokens": int},
}


def _public_object(raw: Any, label: str, fields: dict, *, strict: bool = True) -> dict:
    """Only the named public fields; arguments/results remain opaque JSON maps."""
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        if not strict:
            return {}
        raise IncompleteResponseError(f"上游 {label} 必须是 JSON 对象。")
    result = {}
    for key, kind in fields.items():
        value = raw.get(key)
        if value is None:
            continue
        if not isinstance(value, kind) or (isinstance(value, bool) and kind is not bool):
            if not strict:
                continue
            raise IncompleteResponseError(f"上游 {label}.{key} 格式无效。")
        result[key] = value
    return result


def _keep_payload(key: str, value: dict) -> bool:
    if key in ("toolCall", "toolResponse"):
        tool_type = value.get("toolType", "")
        return bool(tool_type and tool_type != "TOOL_TYPE_UNSPECIFIED")
    if key == "executableCode":
        return bool(value.get("code"))
    outcome = value.get("outcome", "")
    return bool(outcome and outcome != "OUTCOME_UNSPECIFIED")


def _function_response(raw: Any) -> dict:
    value = _public_object(raw, "functionResponse", _FUNCTION_RESULT)
    if not (value.get("name") or value.get("id")):
        return {}
    if "parts" in value:
        media = []
        for entry in value["parts"]:
            if not isinstance(entry, dict):
                raise IncompleteResponseError("上游 functionResponse.parts 项必须是 JSON 对象。")
            blob = _public_object(entry.get("inlineData"), "functionResponse.parts.inlineData", _INLINE)
            file = _public_object(entry.get("fileData"), "functionResponse.parts.fileData", _FILE)
            item = {}
            if blob.get("data"):
                item["inlineData"] = blob
            if file.get("fileUri"):
                item["fileData"] = file
            if item:
                media.append(item)
        value["parts"] = media
    return value


def extend_native_payload(source: dict, part: dict) -> None:
    """Retain the owner of a genuine signature, not just the signature bytes."""
    if "inlineData" in part:
        part["inlineData"] = _public_object(source["inlineData"], "inlineData", _INLINE)
    if "fileData" in part:
        part["fileData"] = _public_object(source["fileData"], "fileData", _FILE)
    for key, fields in _PAYLOAD_FIELDS.items():
        value = _public_object(source.get(key), key, fields)
        if value and _keep_payload(key, value):
            part[key] = value
    if response := _function_response(source.get("functionResponse")):
        part["functionResponse"] = response


def extend_native_metadata(source: dict, part: dict) -> None:
    """Do not turn an otherwise empty private proto default into a new Part."""
    if not part:
        return
    # Studio may include private Duration objects/defaults on every Part. Only
    # actual media can own these fields, and only public scalar forms can be
    # echoed by native clients. Optional private metadata never breaks a Part.
    if "inlineData" in part or "fileData" in part:
        for key, fields in _METADATA_FIELDS.items():
            value = _public_object(source.get(key), key, fields, strict=False)
            if value:
                part[key] = value
    metadata = source.get("partMetadata")
    if isinstance(metadata, dict) and metadata:
        part["partMetadata"] = metadata
