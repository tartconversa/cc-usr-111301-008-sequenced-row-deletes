"""删除适用性读取层。

按数据序列 (data_sequence) 与稳定文件身份 (file_id) 解释位置删除与等值删除：

* 位置删除只作用于 ``target_file_id`` 指定的不可变数据文件，行号为从 0 开始的
  物理行号；文件合并/重写产生新 file_id 后，旧位置删除不能按同一行号套用。
* 等值删除仅在 ``data_sequence < delete_sequence`` 时生效，同序列并发数据不受
  影响；键按类型与 null-safe 语义比较（NULL 匹配 NULL，不产生 SQL UNKNOWN）。
* 删除文件是否可见完全由所选快照清单决定，不扫描目录中的其他删除文件。
* 重写先对输入行应用本快照删除，再以新 file_id、新 data_sequence 输出，并在清单
  中移除被替代数据与只对旧文件有效的位置删除；等值删除仍影响其他文件时保留。

本模块只处理仓库内的合成湖资料，标准库实现，无第三方依赖。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

__all__ = [
    "LakeError",
    "ManifestError",
    "IntegrityError",
    "DeleteFormatError",
    "BudgetExceeded",
    "RewriteError",
    "RowLocation",
    "DeleteHit",
    "ScannedRow",
    "ScanResult",
    "DataFileSpec",
    "FieldSchema",
    "LakeManifest",
    "RewriteReport",
    "DEFAULT_BUCKET_COUNT",
    "DEFAULT_MEMORY_BUDGET",
    "load_manifest",
    "DeleteIndex",
    "scan_snapshot",
    "rewrite_snapshot",
    "record_multiset",
    "main",
]

DEFAULT_BUCKET_COUNT = 64
DEFAULT_MEMORY_BUDGET = 1_000_000
EQUALITY_SEQUENCE_RULE = "data_sequence < delete_sequence"


# --------------------------------------------------------------------------- #
# 异常类型：任何契约破坏都阻止读取，不静默忽略
# --------------------------------------------------------------------------- #
class LakeError(Exception):
    """读取层所有错误的基类。"""


class ManifestError(LakeError):
    """清单结构或快照引用非法。"""


class IntegrityError(LakeError):
    """文件哈希、物理行号或字节内容与登记不符。"""


class DeleteFormatError(LakeError):
    """删除文件结构、键类型或目标引用非法。"""


class BudgetExceeded(LakeError):
    """删除索引条目数超过内存预算。"""


class RewriteError(LakeError):
    """重写计划或提交协议失败。"""


# --------------------------------------------------------------------------- #
# 结果数据结构
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RowLocation:
    """一条记录在不可变数据文件中的原始物理位置。"""

    file_id: str
    data_sequence: int
    path: str
    line_number: int  # 从 0 开始的物理行号


@dataclass(frozen=True)
class DeleteHit:
    """一次删除命中的依据。"""

    delete_id: str
    kind: str  # "equality" | "position"
    delete_sequence: int | None
    reason: str


@dataclass(frozen=True)
class ScannedRow:
    """快照扫描的逐行结论，保留与删除都输出。"""

    location: RowLocation
    record: dict[str, Any]
    kept: bool
    hits: tuple[DeleteHit, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "location": {
                "file_id": self.location.file_id,
                "data_sequence": self.location.data_sequence,
                "path": self.location.path,
                "line_number": self.location.line_number,
            },
            "record": self.record,
            "kept": self.kept,
            "hits": [
                {
                    "delete_id": h.delete_id,
                    "kind": h.kind,
                    "delete_sequence": h.delete_sequence,
                    "reason": h.reason,
                }
                for h in self.hits
            ],
        }


@dataclass(frozen=True)
class ScanResult:
    snapshot: str
    rows: tuple[ScannedRow, ...]

    @property
    def kept_rows(self) -> tuple[ScannedRow, ...]:
        return tuple(r for r in self.rows if r.kept)

    @property
    def deleted_rows(self) -> tuple[ScannedRow, ...]:
        return tuple(r for r in self.rows if not r.kept)


@dataclass(frozen=True)
class DataFileSpec:
    file_id: str
    path: str
    data_sequence: int
    sha256: str


@dataclass(frozen=True)
class FieldSchema:
    field_id: int
    name: str
    type: str
    nullable: bool


@dataclass(frozen=True)
class SnapshotSpec:
    data: tuple[str, ...]
    deletes: tuple[str, ...]


@dataclass(frozen=True)
class LakeManifest:
    root: Path
    data_files: dict[str, DataFileSpec]
    fields_by_id: dict[int, FieldSchema]
    snapshots: dict[str, SnapshotSpec]
    current: str
    deletes_path: Path
    delete_documents: dict[str, dict[str, Any]]
    raw: dict[str, Any]


# --------------------------------------------------------------------------- #
# 类型感知、null-safe 的键比较与分桶
# --------------------------------------------------------------------------- #
def _type_tag(value: Any) -> str:
    if value is None:
        return "n"
    if isinstance(value, bool):
        return "b"
    if isinstance(value, int):
        return "i"
    if isinstance(value, float):
        return "f"
    if isinstance(value, str):
        return "s"
    raise DeleteFormatError(f"不支持的键值类型: {type(value).__name__}")


def values_equal(left: Any, right: Any) -> bool:
    """null-safe、类型感知的相等比较。

    NULL 与 NULL 相等；整数 1 与字符串 "1" 不相等；布尔值不与整数混同。
    """
    if left is None or right is None:
        return left is None and right is None
    if isinstance(left, bool) != isinstance(right, bool):
        return False
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return left == right
    return type(left) is type(right) and left == right


def _canonical_key(values: tuple[Any, ...]) -> tuple[tuple[str, Any], ...]:
    """带类型标签的可哈希键，供分桶使用。"""
    return tuple((_type_tag(v), v) for v in values)


def _check_value_type(value: Any, schema: FieldSchema, where: str) -> None:
    if value is None:
        if not schema.nullable:
            raise DeleteFormatError(f"{where}: 字段 {schema.name} 不可为 NULL")
        return
    if schema.type in ("integer", "long"):
        if not isinstance(value, int) or isinstance(value, bool):
            raise DeleteFormatError(
                f"{where}: 字段 {schema.name} 期望 {schema.type}，实际 {type(value).__name__}"
            )
    elif schema.type in ("float", "double"):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise DeleteFormatError(
                f"{where}: 字段 {schema.name} 期望 {schema.type}，实际 {type(value).__name__}"
            )
    elif schema.type == "string":
        if not isinstance(value, str):
            raise DeleteFormatError(
                f"{where}: 字段 {schema.name} 期望 string，实际 {type(value).__name__}"
            )
    elif schema.type == "boolean":
        if not isinstance(value, bool):
            raise DeleteFormatError(
                f"{where}: 字段 {schema.name} 期望 boolean，实际 {type(value).__name__}"
            )
    else:
        raise DeleteFormatError(f"{where}: 不支持的字段类型 {schema.type}")


def _key_bucket(canonical: tuple[tuple[str, Any], ...], bucket_count: int) -> int:
    return hash(canonical) % bucket_count


# --------------------------------------------------------------------------- #
# 清单加载与哈希核验
# --------------------------------------------------------------------------- #
def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 16), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_manifest(
    manifest_path: str | Path,
    *,
    verify_hashes: bool = True,
    deletes_path: str | Path | None = None,
) -> LakeManifest:
    """加载并核验清单。哈希不符、引用缺失或契约字段错误都抛出异常。"""
    manifest_path = Path(manifest_path).resolve()
    root = manifest_path.parent
    try:
        raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ManifestError(f"清单不存在: {manifest_path}") from exc
    if not isinstance(raw, dict):
        raise ManifestError("清单根节点必须是对象")

    rule = raw.get("equality_sequence_rule")
    if rule is not None and rule != EQUALITY_SEQUENCE_RULE:
        raise ManifestError(
            f"equality_sequence_rule 必须为 {EQUALITY_SEQUENCE_RULE!r}，得到 {rule!r}"
        )

    fields_by_id: dict[int, FieldSchema] = {}
    schemas = raw.get("schemas", {})
    if not isinstance(schemas, dict) or not schemas:
        raise ManifestError("清单缺少 schemas")
    for name, spec in schemas.items():
        if not isinstance(spec, dict) or "field_id" not in spec:
            raise ManifestError(f"schema {name!r} 结构非法")
        field_id = spec["field_id"]
        if not isinstance(field_id, int) or isinstance(field_id, bool):
            raise ManifestError(f"schema {name!r} 的 field_id 必须是整数")
        if field_id in fields_by_id:
            raise ManifestError(f"field_id {field_id} 重复登记")
        fields_by_id[field_id] = FieldSchema(
            field_id=field_id,
            name=name,
            type=spec.get("type", "string"),
            nullable=bool(spec.get("nullable", False)),
        )

    data_files: dict[str, DataFileSpec] = {}
    for entry in raw.get("data_files", []):
        if not isinstance(entry, dict):
            raise ManifestError("data_files 条目必须是对象")
        fid = entry.get("file_id")
        if not isinstance(fid, str) or not fid:
            raise ManifestError("data_files 缺少合法 file_id")
        if fid in data_files:
            raise ManifestError(f"file_id 重复登记: {fid}")
        seq = entry.get("data_sequence")
        if not isinstance(seq, int) or isinstance(seq, bool):
            raise ManifestError(f"data_files[{fid}] 的 data_sequence 必须是整数")
        path = entry.get("path")
        sha = entry.get("sha256")
        if not isinstance(path, str) or not isinstance(sha, str):
            raise ManifestError(f"data_files[{fid}] 缺少 path/sha256")
        spec = DataFileSpec(file_id=fid, path=path, data_sequence=seq, sha256=sha)
        if verify_hashes:
            actual = _sha256_file(root / path)
            if actual != sha:
                raise IntegrityError(
                    f"数据文件 {fid} ({path}) SHA256 不符: 期望 {sha}，实际 {actual}"
                )
        data_files[fid] = spec
    if not data_files:
        raise ManifestError("清单未登记任何数据文件")

    resolved_deletes = Path(deletes_path).resolve() if deletes_path else root / "deletes.json"
    try:
        deletes_doc = json.loads(resolved_deletes.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ManifestError(f"删除文件不存在: {resolved_deletes}") from exc
    if not isinstance(deletes_doc, dict) or not isinstance(deletes_doc.get("deletes"), list):
        raise DeleteFormatError("删除文件根结构非法，缺少 deletes 数组")
    delete_documents: dict[str, dict[str, Any]] = {}
    for doc in deletes_doc["deletes"]:
        if not isinstance(doc, dict) or not isinstance(doc.get("id"), str):
            raise DeleteFormatError("删除条目缺少合法 id")
        if doc["id"] in delete_documents:
            raise DeleteFormatError(f"删除 id 重复: {doc['id']}")
        delete_documents[doc["id"]] = doc

    snapshots: dict[str, SnapshotSpec] = {}
    raw_snaps = raw.get("snapshots", {})
    if not isinstance(raw_snaps, dict) or not raw_snaps:
        raise ManifestError("清单缺少 snapshots")
    for name, snap in raw_snaps.items():
        if not isinstance(snap, dict):
            raise ManifestError(f"快照 {name!r} 结构非法")
        data_ids = snap.get("data", [])
        del_ids = snap.get("deletes", [])
        if not isinstance(data_ids, list) or not isinstance(del_ids, list):
            raise ManifestError(f"快照 {name!r} 的 data/deletes 必须是数组")
        if len(set(data_ids)) != len(data_ids) or len(set(del_ids)) != len(del_ids):
            raise ManifestError(f"快照 {name!r} 存在重复文件 id")
        for fid in data_ids:
            if fid not in data_files:
                raise ManifestError(f"快照 {name!r} 引用未登记数据文件: {fid}")
        for did in del_ids:
            if did not in delete_documents:
                raise ManifestError(f"快照 {name!r} 引用未登记删除文件: {did}")
        snapshots[name] = SnapshotSpec(data=tuple(data_ids), deletes=tuple(del_ids))

    current = raw.get("current")
    if not isinstance(current, str) or current not in snapshots:
        raise ManifestError("current 快照未在 snapshots 中登记")

    return LakeManifest(
        root=root,
        data_files=data_files,
        fields_by_id=fields_by_id,
        snapshots=snapshots,
        current=current,
        deletes_path=resolved_deletes,
        delete_documents=delete_documents,
        raw=raw,
    )


# --------------------------------------------------------------------------- #
# 物理 JSONL 读取：行号契约强制
# --------------------------------------------------------------------------- #
def read_physical_jsonl(path: str | Path, expected_sha256: str) -> list[dict[str, Any]]:
    """按字节读取不可变数据文件并核验哈希，返回与物理行号对齐的记录列表。"""
    path = Path(path)
    try:
        data = path.read_bytes()
    except FileNotFoundError as exc:
        raise IntegrityError(f"数据文件缺失: {path}") from exc
    actual = hashlib.sha256(data).hexdigest()
    if actual != expected_sha256:
        raise IntegrityError(f"数据文件 {path.name} SHA256 不符: 期望 {expected_sha256}，实际 {actual}")
    text = data.decode("utf-8")
    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines.pop()  # 允许文件以单个换行结尾
    if any(line == "" for line in lines):
        raise IntegrityError(f"{path.name} 存在空行，破坏从 0 开始的连续物理行号契约")
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(lines):
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise IntegrityError(f"{path.name} 物理行 {line_number} 不是合法 JSON") from exc
        if not isinstance(record, dict):
            raise IntegrityError(f"{path.name} 物理行 {line_number} 不是 JSON 对象")
        rows.append(record)
    return rows


# --------------------------------------------------------------------------- #
# 删除索引：等值删除分桶 + 位置删除按文件身份登记，受内存预算约束
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class _EqualityDoc:
    delete_id: str
    delete_sequence: int
    fields: tuple[FieldSchema, ...]
    buckets: tuple[set, ...]


class DeleteIndex:
    """只索引快照清单中可见的删除文件；绝不扫描目录中的其他删除文件。"""

    def __init__(
        self,
        manifest: LakeManifest,
        visible_delete_ids: tuple[str, ...] | list[str],
        *,
        bucket_count: int = DEFAULT_BUCKET_COUNT,
        memory_budget: int = DEFAULT_MEMORY_BUDGET,
    ):
        if not isinstance(bucket_count, int) or bucket_count <= 0:
            raise DeleteFormatError("bucket_count 必须是正整数")
        if not isinstance(memory_budget, int) or memory_budget <= 0:
            raise DeleteFormatError("memory_budget 必须是正整数")

        equality_docs: list[_EqualityDoc] = []
        positions: dict[tuple[str, int], list[str]] = {}
        entry_count = 0

        for did in visible_delete_ids:
            doc = manifest.delete_documents[did]
            kind = doc.get("kind")
            if kind == "equality":
                equality_docs.append(self._build_equality(manifest, doc, bucket_count))
                entry_count += len(doc.get("keys", []))
            elif kind == "position":
                target = doc.get("target_file_id")
                if not isinstance(target, str) or target not in manifest.data_files:
                    raise DeleteFormatError(
                        f"位置删除 {did} 指向未登记数据文件: {target!r}"
                    )
                raw_positions = doc.get("positions", [])
                if not isinstance(raw_positions, list) or not raw_positions:
                    raise DeleteFormatError(f"位置删除 {did} 的 positions 必须是非空数组")
                seen: set[int] = set()
                for pos in raw_positions:
                    if not isinstance(pos, int) or isinstance(pos, bool):
                        raise DeleteFormatError(f"位置删除 {did} 的行号必须是整数")
                    if pos < 0:
                        raise DeleteFormatError(f"位置删除 {did} 使用了负行号 {pos}，行号从 0 开始")
                    if pos in seen:
                        raise DeleteFormatError(f"位置删除 {did} 中物理行 {pos} 重复")
                    seen.add(pos)
                    positions.setdefault((target, pos), []).append(did)
                    entry_count += 1
            else:
                raise DeleteFormatError(f"删除 {did} 的 kind 非法: {kind!r}")

            if entry_count > memory_budget:
                raise BudgetExceeded(
                    f"删除索引条目数 {entry_count} 超过内存预算 {memory_budget}"
                )

        self._equality_docs = tuple(equality_docs)
        self._positions = positions
        self.entry_count = entry_count
        self.bucket_count = bucket_count
        self.visible_delete_ids = tuple(visible_delete_ids)

    @staticmethod
    def _build_equality(
        manifest: LakeManifest, doc: dict[str, Any], bucket_count: int
    ) -> _EqualityDoc:
        did = doc["id"]
        seq = doc.get("delete_sequence")
        if not isinstance(seq, int) or isinstance(seq, bool):
            raise DeleteFormatError(f"等值删除 {did} 的 delete_sequence 必须是整数")
        field_ids = doc.get("field_ids", [])
        if not isinstance(field_ids, list) or not field_ids:
            raise DeleteFormatError(f"等值删除 {did} 缺少 field_ids")
        fields: list[FieldSchema] = []
        for fid in field_ids:
            schema = manifest.fields_by_id.get(fid)
            if schema is None:
                raise DeleteFormatError(f"等值删除 {did} 引用未登记 field_id: {fid}")
            fields.append(schema)
        keys = doc.get("keys", [])
        if not isinstance(keys, list) or not keys:
            raise DeleteFormatError(f"等值删除 {did} 的 keys 必须是非空数组")
        buckets: list[set] = [set() for _ in range(bucket_count)]
        for key in keys:
            if not isinstance(key, list) or len(key) != len(fields):
                raise DeleteFormatError(f"等值删除 {did} 的键元组维度与 field_ids 不符")
            for value, schema in zip(key, fields):
                _check_value_type(value, schema, f"等值删除 {did}")
            canonical = _canonical_key(tuple(key))
            buckets[_key_bucket(canonical, bucket_count)].add(canonical)
        return _EqualityDoc(
            delete_id=did, delete_sequence=seq, fields=tuple(fields), buckets=tuple(buckets)
        )

    def position_hits(self, manifest: LakeManifest, file_id: str, line_number: int) -> list[DeleteHit]:
        return [
            DeleteHit(
                delete_id=did,
                kind="position",
                delete_sequence=None,
                reason=f"位置删除 {did} 命中目标文件 {file_id} 的物理行 {line_number}",
            )
            for did in self._positions.get((file_id, line_number), ())
        ]

    def validate_position_ranges(self, manifest: LakeManifest, visible_data_ids: tuple[str, ...]) -> None:
        """核验位置删除的行号不超出目标文件的实际物理行数；越界阻止读取。"""
        lengths: dict[str, int] = {}
        for (target, pos), delete_ids in self._positions.items():
            if target not in visible_data_ids:
                continue  # 目标文件在本快照不可见，该位置删除在本快照不适用
            if target not in lengths:
                spec = manifest.data_files[target]
                lengths[target] = len(
                    read_physical_jsonl(manifest.root / spec.path, spec.sha256)
                )
            if pos >= lengths[target]:
                raise DeleteFormatError(
                    f"位置删除 {delete_ids[0]} 的物理行 {pos} 超出文件 {target} "
                    f"（共 {lengths[target]} 行，行号从 0 开始）"
                )

    def equality_hits(
        self, data_sequence: int, record: dict[str, Any]
    ) -> list[DeleteHit]:
        hits: list[DeleteHit] = []
        for eq in self._equality_docs:
            # 严格小于：同序列的并发数据不被删除
            if not data_sequence < eq.delete_sequence:
                continue
            values: list[Any] = []
            for schema in eq.fields:
                if schema.name not in record:
                    raise DeleteFormatError(
                        f"数据行缺少等值删除 {eq.delete_id} 需要的键字段 {schema.name}"
                    )
                value = record[schema.name]
                _check_value_type(value, schema, f"等值删除 {eq.delete_id} 匹配")
                values.append(value)
            canonical = _canonical_key(tuple(values))
            bucket = eq.buckets[_key_bucket(canonical, self.bucket_count)]
            if canonical in bucket:
                hits.append(
                    DeleteHit(
                        delete_id=eq.delete_id,
                        kind="equality",
                        delete_sequence=eq.delete_sequence,
                        reason=(
                            f"等值删除 {eq.delete_id}: data_sequence({data_sequence}) < "
                            f"delete_sequence({eq.delete_sequence}) 且键 null-safe 匹配"
                        ),
                    )
                )
        return hits


# --------------------------------------------------------------------------- #
# 快照扫描
# --------------------------------------------------------------------------- #
def scan_snapshot(
    manifest: LakeManifest,
    snapshot: str | None = None,
    *,
    bucket_count: int = DEFAULT_BUCKET_COUNT,
    memory_budget: int = DEFAULT_MEMORY_BUDGET,
) -> ScanResult:
    """扫描快照，逐行给出保留/删除结论与命中依据。"""
    name = snapshot if snapshot is not None else manifest.current
    spec = manifest.snapshots.get(name)
    if spec is None:
        raise ManifestError(f"快照未登记: {name!r}")

    index = DeleteIndex(
        manifest, spec.deletes, bucket_count=bucket_count, memory_budget=memory_budget
    )
    index.validate_position_ranges(manifest, spec.data)

    rows: list[ScannedRow] = []
    for file_id in spec.data:
        data_file = manifest.data_files[file_id]
        records = read_physical_jsonl(manifest.root / data_file.path, data_file.sha256)
        for line_number, record in enumerate(records):
            location = RowLocation(
                file_id=file_id,
                data_sequence=data_file.data_sequence,
                path=data_file.path,
                line_number=line_number,
            )
            hits = index.position_hits(manifest, file_id, line_number)
            hits.extend(index.equality_hits(data_file.data_sequence, record))
            # 同一行命中多次只删除一次：按删除 id 去重，保留首次命中依据
            unique: dict[str, DeleteHit] = {}
            for hit in hits:
                unique.setdefault(hit.delete_id, hit)
            deduped = tuple(unique.values())
            rows.append(ScannedRow(location=location, record=record, kept=not deduped, hits=deduped))
    return ScanResult(snapshot=name, rows=tuple(rows))


def record_multiset(result: ScanResult, *, only_kept: bool = True) -> dict[str, int]:
    """把扫描结果归约为记录多重集（JSON 规范串 -> 次数），用于等价性核验。"""
    counter: dict[str, int] = {}
    for row in result.rows:
        if only_kept and not row.kept:
            continue
        key = json.dumps(row.record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        counter[key] = counter.get(key, 0) + 1
    return counter


# --------------------------------------------------------------------------- #
# 重写：先应用本快照删除，再提交新 file_id / 新 data_sequence
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class WrittenFile:
    file_id: str
    path: str
    data_sequence: int
    sha256: str
    record_count: int


@dataclass(frozen=True)
class RewriteReport:
    snapshot: str
    new_snapshot: str
    new_data_sequence: int
    written_files: tuple[WrittenFile, ...]
    removed_data_file_ids: tuple[str, ...]
    removed_position_delete_ids: tuple[str, ...]
    retained_equality_delete_ids: tuple[str, ...]
    retained_position_delete_ids: tuple[str, ...]
    output_manifest: Path
    scan: ScanResult

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot": self.snapshot,
            "new_snapshot": self.new_snapshot,
            "new_data_sequence": self.new_data_sequence,
            "written_files": [
                {
                    "file_id": f.file_id,
                    "path": f.path,
                    "data_sequence": f.data_sequence,
                    "sha256": f.sha256,
                    "record_count": f.record_count,
                }
                for f in self.written_files
            ],
            "removed_data_file_ids": list(self.removed_data_file_ids),
            "removed_position_delete_ids": list(self.removed_position_delete_ids),
            "retained_equality_delete_ids": list(self.retained_equality_delete_ids),
            "retained_position_delete_ids": list(self.retained_position_delete_ids),
            "output_manifest": str(self.output_manifest),
            "rows": [row.to_dict() for row in self.scan.rows],
        }


def rewrite_snapshot(
    manifest: LakeManifest,
    output_dir: str | Path,
    *,
    snapshot: str | None = None,
    rewrite_file_ids: tuple[str, ...] | list[str] | None = None,
    new_snapshot: str | None = None,
    new_data_sequence: int | None = None,
    merge: bool = False,
    id_prefix: str = "rw",
    bucket_count: int = DEFAULT_BUCKET_COUNT,
    memory_budget: int = DEFAULT_MEMORY_BUDGET,
) -> RewriteReport:
    """重写快照数据并提交新清单（暂存目录 + 原子替换，不改动原始资料）。

    ``rewrite_file_ids`` 缺省为重写快照中的全部数据文件；也可只重写子集，
    其余文件原样保留在新快照中。
    """
    name = snapshot if snapshot is not None else manifest.current
    spec = manifest.snapshots.get(name)
    if spec is None:
        raise RewriteError(f"快照未登记: {name!r}")
    if not isinstance(id_prefix, str) or not id_prefix:
        raise RewriteError("id_prefix 必须是非空字符串")
    if rewrite_file_ids is None:
        replaced_ids = tuple(spec.data)
    else:
        replaced_ids = tuple(rewrite_file_ids)
        if not replaced_ids:
            raise RewriteError("rewrite_file_ids 不能为空")
        unknown = [fid for fid in replaced_ids if fid not in spec.data]
        if unknown:
            raise RewriteError(f"重写目标不属于快照 {name!r}: {unknown}")
        if len(set(replaced_ids)) != len(replaced_ids):
            raise RewriteError("rewrite_file_ids 存在重复 id")

    # 1) 先对输入行应用本快照删除（扫描覆盖快照全部数据，重写只取被替代文件）
    scan = scan_snapshot(
        manifest, name, bucket_count=bucket_count, memory_budget=memory_budget
    )
    # 新序列必须严格高于快照内全部数据序列与等值删除序列，保证保留的等值删除
    # （按 data_sequence < delete_sequence 生效）绝不会误删重写产物
    max_sequence = max(
        [manifest.data_files[fid].data_sequence for fid in spec.data]
        + [
            manifest.delete_documents[did]["delete_sequence"]
            for did in spec.deletes
            if manifest.delete_documents[did].get("kind") == "equality"
            and isinstance(manifest.delete_documents[did].get("delete_sequence"), int)
        ]
    )
    if new_data_sequence is None:
        chosen_sequence = max_sequence + 1
    else:
        if not isinstance(new_data_sequence, int) or isinstance(new_data_sequence, bool):
            raise RewriteError("new_data_sequence 必须是整数")
        if new_data_sequence <= max_sequence:
            raise RewriteError(
                f"new_data_sequence={new_data_sequence} 必须严格大于现有最大序列 {max_sequence}，"
                "否则保留的等值删除会误删新数据"
            )
        chosen_sequence = new_data_sequence

    # 2) 计划新文件（只收集被替代文件的保留行，其余文件原样保留）
    kept_by_file: dict[str, list[ScannedRow]] = {fid: [] for fid in replaced_ids}
    for row in scan.rows:
        if row.kept and row.location.file_id in kept_by_file:
            kept_by_file[row.location.file_id].append(row)

    if merge:
        merged_rows = [row for fid in replaced_ids for row in kept_by_file[fid]]
        plan = [(f"{id_prefix}-merged", f"data/{id_prefix}-merged.jsonl", merged_rows)]
    else:
        plan = [
            (
                f"{id_prefix}-{fid}",
                f"data/{id_prefix}-{fid}.jsonl",
                kept_by_file[fid],
            )
            for fid in replaced_ids
        ]
    new_ids = {item[0] for item in plan}
    collision = new_ids & set(manifest.data_files)
    if collision:
        raise RewriteError(f"新 file_id 与既有登记冲突: {sorted(collision)}")

    # 3) 删除保留策略
    retained_eq: list[str] = []
    retained_pos: list[str] = []
    removed_pos: list[str] = []
    unreplaced = [fid for fid in spec.data if fid not in replaced_ids]  # 全量重写时为空
    for did in spec.deletes:
        doc = manifest.delete_documents[did]
        if doc.get("kind") == "position":
            target = doc.get("target_file_id")
            if target in replaced_ids:
                removed_pos.append(did)  # 只对旧文件有效的位置删除随旧文件移除
            else:
                retained_pos.append(did)
        else:
            still_live = any(
                manifest.data_files[fid].data_sequence < doc["delete_sequence"]
                for fid in unreplaced
            )
            if still_live:
                retained_eq.append(did)  # 仍影响其他文件的等值删除保留

    new_snapshot_name = new_snapshot or f"{name}-rewritten"
    if new_snapshot_name in manifest.snapshots:
        raise RewriteError(f"新快照名已存在: {new_snapshot_name}")
    new_data_list = [plan_item[0] for plan_item in plan] + list(unreplaced)
    retained_delete_ids = set(retained_eq) | set(retained_pos)
    new_delete_list = [d for d in spec.deletes if d in retained_delete_ids]

    # 4) 暂存 + 原子提交
    output_dir = Path(output_dir).resolve()
    if output_dir.exists() and not output_dir.is_dir():
        raise RewriteError(f"输出路径不是目录: {output_dir}")
    if output_dir.exists() and any(output_dir.iterdir()):
        raise RewriteError(f"输出目录非空，拒绝覆盖: {output_dir}")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    pre_existing_empty = output_dir.exists()
    staging = Path(
        tempfile.mkdtemp(prefix=f".{output_dir.name}.staging-", dir=output_dir.parent)
    )
    try:
        written: list[WrittenFile] = []
        for new_id, rel_path, rows_out in plan:
            payload = "".join(
                json.dumps(row.record, ensure_ascii=False, separators=(",", ":")) + "\n"
                for row in rows_out
            ).encode("utf-8")
            target = staging / rel_path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(payload)
            written.append(
                WrittenFile(
                    file_id=new_id,
                    path=rel_path,
                    data_sequence=chosen_sequence,
                    sha256=hashlib.sha256(payload).hexdigest(),
                    record_count=len(rows_out),
                )
            )

        # 原样复制所有既有数据文件，保证老快照仍能解释老文件与行位置
        for spec_file in manifest.data_files.values():
            src = manifest.root / spec_file.path
            dst = staging / spec_file.path
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.write_bytes(src.read_bytes())

        # 删除文件内容不变；可见性由快照清单表达
        try:
            deletes_rel = manifest.deletes_path.resolve().relative_to(manifest.root)
        except ValueError as exc:
            raise RewriteError(
                f"删除文件 {manifest.deletes_path} 不在清单目录 {manifest.root} 内，无法自包含输出"
            ) from exc
        staging_deletes = staging / deletes_rel
        staging_deletes.parent.mkdir(parents=True, exist_ok=True)
        staging_deletes.write_bytes(manifest.deletes_path.read_bytes())

        new_manifest = json.loads(json.dumps(manifest.raw))  # 深拷贝可 JSON 结构
        new_manifest["data_files"].extend(
            {
                "file_id": w.file_id,
                "path": w.path,
                "data_sequence": w.data_sequence,
                "sha256": w.sha256,
            }
            for w in written
        )
        new_manifest["snapshots"][new_snapshot_name] = {
            "data": new_data_list,
            "deletes": new_delete_list,
        }
        new_manifest["current"] = new_snapshot_name
        out_manifest_path = staging / "manifest.json"
        tmp_manifest = staging / ".manifest.tmp"
        tmp_manifest.write_text(
            json.dumps(new_manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        os.replace(tmp_manifest, out_manifest_path)

        if pre_existing_empty:
            output_dir.rmdir()  # 已核验为空，移除后再原子换入
        os.rename(staging, output_dir)
        staging = None  # type: ignore[assignment]
    finally:
        if staging is not None and staging.exists():
            shutil.rmtree(staging)

    return RewriteReport(
        snapshot=name,
        new_snapshot=new_snapshot_name,
        new_data_sequence=chosen_sequence,
        written_files=tuple(written),
        removed_data_file_ids=replaced_ids,
        removed_position_delete_ids=tuple(removed_pos),
        retained_equality_delete_ids=tuple(retained_eq),
        retained_position_delete_ids=tuple(retained_pos),
        output_manifest=output_dir / "manifest.json",
        scan=scan,
    )


# --------------------------------------------------------------------------- #
# 命令行
# --------------------------------------------------------------------------- #
def _cli_scan(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    result = scan_snapshot(
        manifest,
        args.snapshot,
        bucket_count=args.bucket_count,
        memory_budget=args.memory_budget,
    )
    payload = {
        "snapshot": result.snapshot,
        "kept": len(result.kept_rows),
        "deleted": len(result.deleted_rows),
        "multiset": record_multiset(result),
        "rows": [row.to_dict() for row in result.rows],
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def _cli_rewrite(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    rewrite_ids = (
        [part.strip() for part in args.rewrite_file_ids.split(",") if part.strip()]
        if args.rewrite_file_ids
        else None
    )
    report = rewrite_snapshot(
        manifest,
        args.output,
        snapshot=args.snapshot,
        rewrite_file_ids=rewrite_ids,
        new_snapshot=args.new_snapshot,
        new_data_sequence=args.new_data_sequence,
        merge=args.merge,
        id_prefix=args.id_prefix,
        bucket_count=args.bucket_count,
        memory_budget=args.memory_budget,
    )
    print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="湖表删除适用性读取层")
    parser.add_argument("manifest", help="manifest.json 路径")
    parser.add_argument("--snapshot", default=None, help="快照名，默认清单 current")
    parser.add_argument("--bucket-count", type=int, default=DEFAULT_BUCKET_COUNT)
    parser.add_argument("--memory-budget", type=int, default=DEFAULT_MEMORY_BUDGET)
    sub = parser.add_subparsers(dest="command", required=True)

    scan_p = sub.add_parser("scan", help="扫描快照并输出逐行保留/删除依据")
    scan_p.set_defaults(func=_cli_scan)

    rewrite_p = sub.add_parser("rewrite", help="应用删除后重写并提交新清单")
    rewrite_p.add_argument("--output", required=True, help="输出目录（必须不存在或为空）")
    rewrite_p.add_argument("--new-snapshot", default=None)
    rewrite_p.add_argument("--new-data-sequence", type=int, default=None)
    rewrite_p.add_argument("--merge", action="store_true", help="合并为单个新数据文件")
    rewrite_p.add_argument("--id-prefix", default="rw")
    rewrite_p.add_argument(
        "--rewrite-file-ids",
        default=None,
        help="逗号分隔的待重写 file_id；缺省重写快照全部数据文件",
    )
    rewrite_p.set_defaults(func=_cli_rewrite)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
