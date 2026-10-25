"""Check source ownership and import direction without importing the application."""

import ast
import sys
from pathlib import Path

APP = Path(__file__).resolve().parents[1] / "app"
ADAPTERS = (
    "routes",
    "api_helpers",
    "upstreams.cookie_proxy",
    "upstreams.cookie_native",
    "upstreams.express_sdk",
)
LEAF_BOUNDARIES = {
    "proxies.protocols": ("proxy_resolver", "proxy_failover", *ADAPTERS),
    "upstreams.cookie_transport": ADAPTERS,
    "upstreams.express_response": ADAPTERS,
}


def imported_modules(node, module, packages):
    if isinstance(node, ast.Import):
        return [item.name for item in node.names]
    if isinstance(node, ast.ImportFrom):
        target = node.module or ""
        if node.level:
            package = module if module in packages else module.rpartition(".")[0]
            parts = package.split(".")
            parent = parts[: len(parts) - node.level + 1]
            target = ".".join([*parent, *([target] if target else [])])
        return [target, *(f"{target}.{item.name}" for item in node.names)]
    return []


def eager_imports(tree, module, packages):
    """Imports executed while loading a module; function bodies are lazy."""
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            continue
        yield from imported_modules(node, module, packages)
        yield from eager_imports(node, module, packages)


def dependency_cycles(graph):
    seen, active, cycles = set(), [], []

    def visit(module):
        if module in active:
            cycles.append(active[active.index(module) :] + [module])
            return
        if module in seen:
            return
        seen.add(module)
        active.append(module)
        for target in sorted(graph[module]):
            visit(target)
        active.pop()

    for module in sorted(graph):
        visit(module)
    return cycles


def check_modules(trees, packages=frozenset()):
    errors = []
    for module, forbidden in LEAF_BOUNDARIES.items():
        if module not in trees:
            errors.append(f"Missing owned module: {module}")
            continue
        # Direction matters even for lazy imports: a leaf should not call its adapter.
        for node in ast.walk(trees[module]):
            for target in imported_modules(node, module, packages):
                if any(
                    target == prefix or target.startswith(prefix + ".")
                    for prefix in forbidden
                ):
                    errors.append(
                        f"{module}:{node.lineno} imports upper-layer {target}"
                    )
    graph = {
        # A package's `from . import child` names the package itself as well.
        # Reusing that module object is not an inter-module dependency cycle.
        name: (set(eager_imports(tree, name, packages)) & trees.keys()) - {name}
        for name, tree in trees.items()
    }
    errors.extend(
        "Import cycle: " + " -> ".join(cycle) for cycle in dependency_cycles(graph)
    )
    return errors


def main():
    trees = {}
    packages = set()
    for path in sorted(APP.rglob("*.py")):
        parts = list(path.relative_to(APP).with_suffix("").parts)
        if parts[-1] == "__init__":
            parts.pop()
            packages.add(".".join(parts))
        if parts:
            trees[".".join(parts)] = ast.parse(
                path.read_text(encoding="utf-8-sig"), filename=str(path)
            )
    errors = check_modules(trees, packages)
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(
        f"Module boundaries and eager imports checked: {len(trees)} modules, no cycles."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
