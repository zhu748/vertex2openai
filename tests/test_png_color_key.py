"""PNG color-key transparency survives conversion to JPEG on both chat wires."""
import base64
import io
import unittest
from unittest.mock import patch

import image_processing
from message_processing import _sdk_image_part, _wire_image_part
from PIL import Image


class PngColorKeyTests(unittest.TestCase):
    def test_transparent_key_whitens_without_changing_opaque_pixels(self):
        for mode, key, fill in (("RGB", (214, 22, 83), (20, 55, 30)), ("L", 0, 210)):
            with self.subTest(mode=mode):
                buf = io.BytesIO()
                with Image.new(mode, (32, 16), key) as original:
                    original.paste(fill, (16, 0, 32, 16))
                    original.save(buf, format="PNG", transparency=key)
                raw = buf.getvalue()
                self.assertEqual(image_processing.optimize_image_bytes(raw, "image/png", len(raw)),
                                 (raw, "image/png"))  # Below threshold: no decode/re-encode.
                url = "data:image/png;base64," + base64.b64encode(raw).decode()
                with patch.object(image_processing.app_state, "get_setting",
                                  side_effect=lambda k, d=None: 0 if k == "img_compress_max_mb" else d):
                    sdk = _sdk_image_part(url).inline_data
                    cookie = _wire_image_part(url)["inlineData"]
                expected_fill = fill if mode == "RGB" else (fill,) * 3
                for mime, data in ((sdk.mime_type, sdk.data),
                                   (cookie["mimeType"], base64.b64decode(cookie["data"]))):
                    self.assertEqual(mime, "image/jpeg")
                    with Image.open(io.BytesIO(data)) as decoded:
                        self.assertTrue(all(c >= 250 for c in decoded.getpixel((5, 8))))
                        pixel = decoded.getpixel((23, 8))
                        self.assertTrue(all(abs(c - d) <= 6 for c, d in zip(pixel, expected_fill, strict=True)))
