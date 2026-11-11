import io, tracemalloc
from sys import path
path.insert(0, "/home/z/my-project/vertex2openai/app")
from json_encoding import iter_json_bytes

part = {"image": "A" * (8 * 1024 * 1024)}

def render_bytesio(content):
    with io.BytesIO() as output:
        for chunk in iter_json_bytes(content, compact=True, allow_nan=False):
            output.write(chunk)
        return output.getvalue()

def render_join(content):
    return b"".join(iter_json_bytes(content, compact=True, allow_nan=False))

for name, fn in (("bytesio", render_bytesio), ("join", render_join)):
    tracemalloc.start()
    body = fn(part)
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(f"{name}: peak={peak/1048576:.2f}MB current={current/1048576:.2f}MB len={len(body)}")

# join(list) 变体：先物化列表再 join
def render_join_list(content):
    return b"".join(list(iter_json_bytes(content, compact=True, allow_nan=False)))

tracemalloc.start()
body = render_join_list(part)
current, peak = tracemalloc.get_traced_memory()
tracemalloc.stop()
print(f"join_list: peak={peak/1048576:.2f}MB current={current/1048576:.2f}MB len={len(body)}")
