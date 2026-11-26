"""The SDK response's aggregate text is computed, not a stored field."""
from types import SimpleNamespace
import unittest
from unittest import mock

import httpx
from google import genai
from google.genai import types

from response_validation import IncompleteResponseError
from upstreams.express_response import is_gemini_response_valid


class SdkPreflightTests(unittest.IsolatedAsyncioTestCase):
    async def test_mock_transport_sdk_response_does_not_serialize_parts_again(self):
        async def upstream(_request):
            return httpx.Response(200, json={"candidates": [{
                "finishReason": "STOP", "content": {"role": "model", "parts": [{"text": "many parts"}] * 64}
            }]})

        client = genai.Client(vertexai=True, api_key="offline", http_options=types.HttpOptions(
            async_client_args={"transport": httpx.MockTransport(upstream)}))
        try:
            response = await client.aio.models.generate_content(model="gemini-3.6-flash", contents="hello")
        finally:
            client.close()
            await client.aio.aclose()
        with mock.patch.object(types.Part, "model_dump", side_effect=AssertionError("needless part serialization")):
            self.assertTrue(is_gemini_response_valid(response))

    def test_sdk_parts_are_authoritative_even_when_empty_or_filtered(self):
        for parts, finish, expected in (
            ([types.Part(text="   "), types.Part(thought_signature=b"fixture")], "STOP", False),
            ([], "MAX_TOKENS", False), ([], "SAFETY", True),
            ([types.Part(text="private reasoning", thought=True)], "STOP", True),
            ([types.Part(function_call=types.FunctionCall(name="weather"))], "STOP", True),
            ([types.Part(inline_data=types.Blob(data=b"fixture"))], "STOP", True),
            ([types.Part(file_data=types.FileData(file_uri="gs://bucket/file"))], "STOP", True),
            ([types.Part(file_data=types.FileData(file_uri="  "))], "STOP", False),
        ):
            with self.subTest(finish=finish, parts=parts):
                response = types.GenerateContentResponse(candidates=[types.Candidate(
                    content=types.Content(parts=parts), finish_reason=finish)])
                with mock.patch.object(types.GenerateContentResponse, "text", new_callable=mock.PropertyMock,
                                       side_effect=AssertionError("computed aggregate text")):
                    self.assertEqual(is_gemini_response_valid(response), expected)
        with self.assertRaises(IncompleteResponseError):
            is_gemini_response_valid(types.GenerateContentResponse(candidates=[
                types.Candidate(content=types.Content(parts=[]), finish_reason="OTHER")]))

    def test_later_candidate_and_legacy_fallbacks(self):
        empty = SimpleNamespace(content=SimpleNamespace(parts=[]))
        later = SimpleNamespace(content=SimpleNamespace(parts=[SimpleNamespace(text="later")]))
        class Wrapper:
            text = mock.PropertyMock(return_value=" independent legacy text ")
        wrapper = Wrapper()
        wrapper.candidates = [empty, later]
        self.assertTrue(is_gemini_response_valid(wrapper))
        Wrapper.__dict__["text"].assert_not_called()
        for value, expected in ((" independent legacy text ", True), (" \t", False), (23, False)):
            with self.subTest(value=value):
                wrapper.candidates = [empty]
                getter = Wrapper.__dict__["text"]
                getter.reset_mock()
                getter.return_value = value
                self.assertEqual(is_gemini_response_valid(wrapper), expected)
                getter.assert_called_once()
        class CandidateWrapper:
            text = mock.PropertyMock(return_value="fallback")
        self.assertTrue(is_gemini_response_valid(SimpleNamespace(candidates=[CandidateWrapper()])))
        CandidateWrapper.__dict__["text"].assert_called_once()
