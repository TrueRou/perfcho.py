import ast
import io
import re
import tokenize
from pathlib import Path

PROJECT_ROOT = Path(__file__).parents[1]
MODULES_ROOT = PROJECT_ROOT / "src" / "perfcho" / "modules"
PACKET_MODELS = PROJECT_ROOT / "src" / "perfcho" / "api" / "stable" / "realtime" / "models.py"
PROTOCOL_TEXT = re.compile(r"(?i)(?<![a-z])(?:stable|lazer|bancho)")
LEGACY_BIT_TEXT = re.compile(r"(?i)legacy[^\n]{0,24}bits?|bits?[^\n]{0,24}legacy")
CLIENT_METADATA_NAMES = frozenset({"client_family", "protocol", "family", "source"})


def _module_files() -> tuple[Path, ...]:
    return tuple(sorted(MODULES_ROOT.rglob("*.py")))


def _module_paths() -> tuple[Path, ...]:
    return tuple(sorted(MODULES_ROOT.rglob("*")))


def _relative(path: Path) -> str:
    return path.relative_to(MODULES_ROOT).as_posix()


def _identifier_words(identifier: str) -> tuple[str, ...]:
    words: list[str] = []
    for segment in identifier.split("_"):
        words.extend(re.findall(r"[A-Z]+(?=[A-Z][a-z]|\d|$)|[A-Z]?[a-z]+|\d+", segment))
    return tuple(word.casefold() for word in words)


def _protocol_identifier(identifier: str) -> bool:
    return bool({"stable", "lazer", "bancho"} & set(_identifier_words(identifier)))


def _packet_symbols() -> frozenset[str]:
    tree = ast.parse(PACKET_MODELS.read_text(), filename=PACKET_MODELS)
    symbols: set[str] = {"ClientPacket", "ServerPacket"}
    for node in tree.body:
        if not isinstance(node, ast.ClassDef) or node.name not in {"ClientPacket", "ServerPacket"}:
            continue
        symbols.update(
            target.id
            for statement in node.body
            if isinstance(statement, ast.Assign)
            for target in statement.targets
            if isinstance(target, ast.Name)
        )
    return frozenset(symbols)


def _node_identifier(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
        return node.name
    if isinstance(node, ast.arg):
        return node.arg
    if isinstance(node, ast.alias):
        return node.asname or node.name.rpartition(".")[2]
    return None


def _expression_identifiers(node: ast.AST) -> frozenset[str]:
    return frozenset(
        identifier.casefold() for child in ast.walk(node) if (identifier := _node_identifier(child)) is not None
    )


def _contains_protocol_value(node: ast.AST) -> bool:
    return any(
        (isinstance(child, ast.Constant) and isinstance(child.value, str) and PROTOCOL_TEXT.search(child.value))
        or (isinstance(child, ast.Attribute) and _protocol_identifier(child.attr))
        for child in ast.walk(node)
    )


def _union_members(node: ast.AST) -> tuple[str, ...]:
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
        return (*_union_members(node.left), *_union_members(node.right))
    if isinstance(node, ast.Name):
        return (node.id,)
    raise AssertionError(f"RealtimeBubble contains a non-name union member: {ast.dump(node)}")


def test_modules_do_not_contain_protocol_specific_paths() -> None:
    violations = [
        _relative(path)
        for path in _module_paths()
        if any(_protocol_identifier(Path(part).stem) for part in path.relative_to(MODULES_ROOT).parts)
    ]
    assert not violations, "Protocol-specific module paths:\n" + "\n".join(violations)


def test_modules_do_not_import_api_adapters() -> None:
    violations: list[str] = []
    for path in _module_files():
        tree = ast.parse(path.read_text(), filename=path)
        for node in ast.walk(tree):
            imported_modules: tuple[str, ...] = ()
            if isinstance(node, ast.Import):
                imported_modules = tuple(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                imported_modules = (node.module,)
            if any(name == "perfcho.api" or name.startswith("perfcho.api.") for name in imported_modules):
                violations.append(f"{_relative(path)}:{getattr(node, 'lineno', 0)}")
    assert not violations, "Module imports of API adapters:\n" + "\n".join(violations)


def test_modules_do_not_use_protocol_names_or_legacy_bit_identifiers() -> None:
    violations: list[str] = []
    packet_symbols = _packet_symbols()
    for path in _module_files():
        tree = ast.parse(path.read_text(), filename=path)
        for node in ast.walk(tree):
            identifier = _node_identifier(node)
            if identifier is None:
                continue
            if _protocol_identifier(identifier):
                violations.append(f"{_relative(path)}:{getattr(node, 'lineno', 0)} protocol name {identifier}")
            if identifier in packet_symbols:
                violations.append(f"{_relative(path)}:{getattr(node, 'lineno', 0)} packet name {identifier}")
            if LEGACY_BIT_TEXT.search(identifier.replace("_", " ")) or identifier.casefold() == "mod_bits":
                violations.append(f"{_relative(path)}:{getattr(node, 'lineno', 0)} legacy bitset {identifier}")
    assert not violations, "Protocol-specific identifiers in modules:\n" + "\n".join(violations)


def test_modules_do_not_describe_protocol_adapters_or_legacy_bitsets() -> None:
    violations: list[str] = []
    for path in _module_files():
        source = path.read_text()
        for token in tokenize.generate_tokens(io.StringIO(source).readline):
            if token.type not in {tokenize.COMMENT, tokenize.STRING}:
                continue
            if PROTOCOL_TEXT.search(token.string) or LEGACY_BIT_TEXT.search(token.string):
                violations.append(f"{_relative(path)}:{token.start[0]} {tokenize.tok_name[token.type].lower()}")
    assert not violations, "Protocol-specific text in modules:\n" + "\n".join(violations)


def test_modules_do_not_branch_on_specific_client_protocols() -> None:
    violations: list[str] = []
    for path in _module_files():
        tree = ast.parse(path.read_text(), filename=path)
        for node in ast.walk(tree):
            if isinstance(node, ast.Compare):
                values = (node.left, *node.comparators)
                if CLIENT_METADATA_NAMES & set().union(*(_expression_identifiers(value) for value in values)) and any(
                    _contains_protocol_value(value) for value in values
                ):
                    violations.append(f"{_relative(path)}:{node.lineno} protocol comparison")
            elif isinstance(node, ast.Match):
                if CLIENT_METADATA_NAMES & _expression_identifiers(node.subject) and any(
                    _contains_protocol_value(case.pattern) for case in node.cases
                ):
                    violations.append(f"{_relative(path)}:{node.lineno} protocol match")
    assert not violations, "Client protocol branches in modules:\n" + "\n".join(violations)


def test_realtime_bubble_union_contains_only_module_types() -> None:
    definitions: dict[str, list[Path]] = {}
    bubble_alias: ast.TypeAlias | None = None
    for path in _module_files():
        tree = ast.parse(path.read_text(), filename=path)
        for node in tree.body:
            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                definitions.setdefault(node.name, []).append(path)
            elif (
                isinstance(node, ast.TypeAlias) and isinstance(node.name, ast.Name) and node.name.id == "RealtimeBubble"
            ):
                bubble_alias = node
    assert bubble_alias is not None, "RealtimeBubble type alias is missing"
    members = _union_members(bubble_alias.value)
    violations = [
        f"{member}: expected one modules/** definition, found {[str(path) for path in definitions.get(member, [])]}"
        for member in members
        if len(definitions.get(member, [])) != 1
    ]
    assert not violations, "Non-module RealtimeBubble members:\n" + "\n".join(violations)
