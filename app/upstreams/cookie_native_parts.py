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
_SPEECH = {"speaker": str, "style": str}
_TRANSCRIPTION = {"text": str, "finished": bool, "languageCode": str, "speakerLabel": str,
                  "startOffset": str, "endOffset": str, "words": list}
_TRANSCRIPTION_WORD = {"word": str, "startOffset": str, "endOffset": str}
_MEDIA_PROCESSING_MODES = ("STATIC", "AGENTIC")


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


def _public_transcription(raw: Any) -> dict:
    transcript = _public_object(raw, "audioTranscription", _TRANSCRIPTION, strict=False)
    if "words" in transcript:
        transcript["words"] = [
            word for entry in transcript["words"]
            if (word := _public_object(entry, "audioTranscription.words", _TRANSCRIPTION_WORD, strict=False))
            and word.get("word")
        ]
    # Offset defaults alone do not identify an actual transcript. Keep false
    # finished and exact time strings when observed alongside a real update.
    return transcript if any(transcript.get(k) for k in (
        "text", "finished", "languageCode", "speakerLabel", "words")) else {}


def extend_native_metadata(source: dict, part: dict) -> None:
    """Do not turn an otherwise empty private proto default into a new Part."""
    if not part:
        return
    # Studio may include private Duration objects/defaults on every Part. Only
    # actual media can own these fields, and only public scalar forms can be
    # echoed by native clients. Optional private metadata never breaks a Part.
    media = part.get("inlineData") or part.get("fileData")
    if media:
        for key, fields in _METADATA_FIELDS.items():
            value = _public_object(source.get(key), key, fields, strict=False)
            if value:
                part[key] = value
        if (processing := source.get("mediaProcessing")) in _MEDIA_PROCESSING_MODES:
            part["mediaProcessing"] = processing
        mime = media.get("mimeType") or ""
        if mime.lower().startswith("audio/") and (transcript := _public_transcription(source.get("audioTranscription"))):
            part["audioTranscription"] = transcript
    if part.get("text"):
        speech = _public_object(source.get("speechMetadata"), "speechMetadata", _SPEECH, strict=False)
        if any(speech.values()):
            part["speechMetadata"] = speech
    metadata = source.get("partMetadata")
    if isinstance(metadata, dict) and metadata:
        part["partMetadata"] = metadata
