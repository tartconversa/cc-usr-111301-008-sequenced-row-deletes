import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from src.delete_layer import (
    BudgetExceeded,
    DeleteFormatError,
    DeleteIndex,
    IntegrityError,
    LakeError,
    ManifestError,
    RewriteError,
    load_manifest,
    read_physical_jsonl,
    record_multiset,
    rewrite_snapshot,
    scan_snapshot,
    values_equal,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_MANIFEST = ROOT / "fixtures" / "manifest.json"


def write_lake(base, data_files, deletes, snapshots, current, schemas=None):
    """在 base 目录组装一个合成湖并返回 manifest 路径。

    data_files: [{file_id, path, data_sequence, rows:[dict,...]}]
    deletes:    删除条目列表（结构同 deletes.json 的元素）
    snapshots:  {name: {"data": [...], "deletes": [...]}}
    """
    base = Path(base)
    manifest_data_files = []
    for entry in data_files:
        path = base / entry["path"]
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in entry["rows"])
        path.write_text(payload, encoding="utf-8")
        manifest_data_files.append(
            {
                "file_id": entry["file_id"],
                "path": entry["path"],
                "data_sequence": entry["data_sequence"],
                "sha256": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
            }
        )
    (base / "deletes.json").write_text(
        json.dumps(
            {
                "deletes": deletes,
                "equality_sequence_rule": "data_sequence < delete_sequence",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    manifest = {
        "data_files": manifest_data_files,
        "schemas": schemas
        or {"k": {"field_id": 1, "type": "integer", "nullable": True}},
        "snapshots": snapshots,
        "current": current,
        "equality_sequence_rule": "data_sequence < delete_sequence",
    }
    manifest_path = base / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    return manifest_path


def decisions(result):
    """{(file_id, line): (kept, [delete_id,...])}"""
    return {
        (r.location.file_id, r.location.line_number): (
            r.kept,
            [h.delete_id for h in r.hits],
        )
        for r in result.rows
    }


class NullSafeEqualityTests(unittest.TestCase):
    def test_null_matches_null(self):
        self.assertTrue(values_equal(None, None))

    def test_null_never_matches_value(self):
        for value in (1, 0, "1", 1.0, True):
            with self.subTest(value=value):
                self.assertFalse(values_equal(None, value))
                self.assertFalse(values_equal(value, None))

    def test_type_is_part_of_identity(self):
        self.assertFalse(values_equal(1, "1"))
        self.assertFalse(values_equal(1, True))
        self.assertFalse(values_equal(1, "1"))
        self.assertTrue(values_equal(1, 1.0))  # 数值族内部按值比较
        self.assertTrue(values_equal(True, True))
        self.assertTrue(values_equal("a", "a"))


class FixtureScenarioTests(unittest.TestCase):
    """题面固定场景：old seq=5 含 k=1,2；new seq=6 含 k=1,null。"""

    @classmethod
    def setUpClass(cls):
        cls.manifest = load_manifest(FIXTURE_MANIFEST)

    def test_before_snapshot_sees_all_old_rows(self):
        result = scan_snapshot(self.manifest, "before")
        self.assertEqual(
            decisions(result),
            {
                ("old", 0): (True, []),
                ("old", 1): (True, []),
            },
        )

    def test_equality_delete_hits_only_older_sequence(self):
        result = scan_snapshot(self.manifest, "after")
        verdict = decisions(result)
        # old seq=5 < delete seq=6：旧 k=1 被等值删除
        self.assertFalse(verdict[("old", 0)][0])
        self.assertEqual(verdict[("old", 0)][1], ["eq6"])
        # new seq=6 与删除同序列：新 k=1 必须保留，删除不是永久封禁
        self.assertTrue(verdict[("new", 0)][0])
        self.assertEqual(verdict[("new", 0)][1], [])

    def test_position_delete_scoped_to_target_file_identity(self):
        result = scan_snapshot(self.manifest, "after")
        verdict = decisions(result)
        # 位置删除 pos-old1 只作用于 old 文件物理行 1（k=2）
        self.assertFalse(verdict[("old", 1)][0])
        self.assertIn("pos-old1", verdict[("old", 1)][1])
        # new 文件物理行 1（k=null）即使行号相同也绝不被删
        self.assertTrue(verdict[("new", 1)][0])
        self.assertEqual(verdict[("new", 1)][1], [])

    def test_multiple_hits_delete_row_only_once(self):
        # old 行 0 同时构造等值+位置两条删除，仍只给一次删除结论
        deletes = [
            {"id": "eq6", "kind": "equality", "delete_sequence": 6,
             "field_ids": [1], "keys": [[1]]},
            {"id": "pos-old0", "kind": "position", "target_file_id": "old",
             "positions": [0]},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            mpath = write_lake(
                base,
                [
                    {"file_id": "old", "path": "data/old.jsonl", "data_sequence": 5,
                     "rows": [{"k": 1, "value": 10}, {"k": 2, "value": 20}]},
                    {"file_id": "new", "path": "data/new.jsonl", "data_sequence": 6,
                     "rows": [{"k": 1, "value": 30}, {"k": None, "value": 40}]},
                ],
                deletes,
                {"s": {"data": ["old", "new"], "deletes": ["eq6", "pos-old0"]}},
                "s",
            )
            result = scan_snapshot(load_manifest(mpath), "s")
            old0 = next(
                r for r in result.rows
                if r.location.file_id == "old" and r.location.line_number == 0
            )
            self.assertFalse(old0.kept)
            self.assertEqual([h.delete_id for h in old0.hits], ["pos-old0", "eq6"])
            # old0 只被移除一次：保留多重集仍有 old1、new0、new1 共 3 条
            self.assertEqual(sum(record_multiset(result).values()), 3)

    def test_retained_multiset_after_snapshot(self):
        result = scan_snapshot(self.manifest, "after")
        kept = record_multiset(result)
        # old0(k=1) 被 eq6 删，old1(k=2) 被 pos-old1 删；仅 new 两行保留
        expected = {
            json.dumps(rec, sort_keys=True, separators=(",", ":")): 1
            for rec in (
                {"k": 1, "value": 30},
                {"k": None, "value": 40},
            )
        }
        self.assertEqual(kept, expected)
        self.assertEqual(sum(kept.values()), 2)


class NullCompositeKeyTests(unittest.TestCase):
    def test_null_matches_null_in_composite_key(self):
        deletes = [
            {"id": "eq-null", "kind": "equality", "delete_sequence": 9,
             "field_ids": [1, 2], "keys": [[None, "x"]]},
        ]
        schemas = {
            "k": {"field_id": 1, "type": "integer", "nullable": True},
            "tag": {"field_id": 2, "type": "string", "nullable": True},
        }
        with tempfile.TemporaryDirectory() as tmp:
            mpath = write_lake(
                Path(tmp),
                [
                    {"file_id": "f", "path": "data/f.jsonl", "data_sequence": 3,
                     "rows": [
                         {"k": None, "tag": "x", "v": 1},   # NULL,NULL-safe 命中
                         {"k": None, "tag": "y", "v": 2},   # 第二分量不同
                         {"k": 1, "tag": "x", "v": 3},      # 第一分量 NULL vs 1 不命中
                     ]},
                ],
                deletes,
                {"s": {"data": ["f"], "deletes": ["eq-null"]}},
                "s",
                schemas=schemas,
            )
            result = scan_snapshot(load_manifest(mpath), "s")
            verdict = decisions(result)
            self.assertFalse(verdict[("f", 0)][0])
            self.assertEqual(verdict[("f", 0)][1], ["eq-null"])
            self.assertTrue(verdict[("f", 1)][0])
            self.assertTrue(verdict[("f", 2)][0])

    def test_duplicate_delete_keys_deduplicated_in_index(self):
        deletes = [
            {"id": "eq-dup", "kind": "equality", "delete_sequence": 9,
             "field_ids": [1], "keys": [[5], [5], [None]]},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            mpath = write_lake(
                Path(tmp),
                [
                    {"file_id": "f", "path": "data/f.jsonl", "data_sequence": 1,
                     "rows": [{"k": 5}, {"k": None}, {"k": 6}]},
                ],
                deletes,
                {"s": {"data": ["f"], "deletes": ["eq-dup"]}},
                "s",
            )
            result = scan_snapshot(load_manifest(mpath), "s")
            verdict = decisions(result)
            self.assertFalse(verdict[("f", 0)][0])
            self.assertFalse(verdict[("f", 1)][0])
            self.assertTrue(verdict[("f", 2)][0])
            # 每条行只有一条命中依据，不随键重复而重复
            f0 = result.rows[0]
            self.assertEqual(len(f0.hits), 1)


class SameSequenceTests(unittest.TestCase):
    def test_same_sequence_data_not_deleted(self):
        deletes = [
            {"id": "eq7", "kind": "equality", "delete_sequence": 7,
             "field_ids": [1], "keys": [[42]]},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            mpath = write_lake(
                Path(tmp),
                [
                    {"file_id": "older", "path": "data/older.jsonl", "data_sequence": 6,
                     "rows": [{"k": 42}]},
                    {"file_id": "peer", "path": "data/peer.jsonl", "data_sequence": 7,
                     "rows": [{"k": 42}]},
                    {"file_id": "newer", "path": "data/newer.jsonl", "data_sequence": 8,
                     "rows": [{"k": 42}]},
                ],
                deletes,
                {"s": {"data": ["older", "peer", "newer"], "deletes": ["eq7"]}},
                "s",
            )
            verdict = decisions(scan_snapshot(load_manifest(mpath), "s"))
            self.assertFalse(verdict[("older", 0)][0])   # 6 < 7
            self.assertTrue(verdict[("peer", 0)][0])     # 7 == 7：同序列并发数据保留
            self.assertTrue(verdict[("newer", 0)][0])    # 8 > 7：未来数据也保留

    def test_delete_for_never_seen_key_is_not_a_permanent_ban(self):
        deletes = [
            {"id": "eq-ghost", "kind": "equality", "delete_sequence": 2,
             "field_ids": [1], "keys": [[999]]},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            mpath = write_lake(
                Path(tmp),
                [
                    {"file_id": "f", "path": "data/f.jsonl", "data_sequence": 3,
                     "rows": [{"k": 999}, {"k": 1}]},
                ],
                deletes,
                {"s": {"data": ["f"], "deletes": ["eq-ghost"]}},
                "s",
            )
            # 序列 3 的新数据不拦截从未出现过的键的历史删除
            verdict = decisions(scan_snapshot(load_manifest(mpath), "s"))
            self.assertTrue(verdict[("f", 0)][0])
            self.assertTrue(verdict[("f", 1)][0])


class SnapshotVisibilityTests(unittest.TestCase):
    def test_same_row_hit_by_two_delete_documents_deleted_once(self):
        deletes = [
            {"id": "eq", "kind": "equality", "delete_sequence": 9,
             "field_ids": [1], "keys": [[7]]},
            {"id": "pos", "kind": "position", "target_file_id": "f",
             "positions": [0]},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            mpath = write_lake(
                Path(tmp),
                [{"file_id": "f", "path": "data/f.jsonl", "data_sequence": 1,
                  "rows": [{"k": 7}, {"k": 8}]}],
                deletes,
                {"s": {"data": ["f"], "deletes": ["eq", "pos"]}},
                "s",
            )
            result = scan_snapshot(load_manifest(mpath), "s")
            row0 = result.rows[0]
            self.assertFalse(row0.kept)
            self.assertEqual(sorted(h.delete_id for h in row0.hits), ["eq", "pos"])
            # 多重集中该行只少一次
            self.assertEqual(sum(record_multiset(result).values()), 1)
            self.assertTrue(result.rows[1].kept)

    def test_deletes_not_listed_are_not_read_from_directory(self):
        deletes = [
            {"id": "listed", "kind": "equality", "delete_sequence": 9,
             "field_ids": [1], "keys": [[1]]},
            {"id": "unlisted", "kind": "equality", "delete_sequence": 9,
             "field_ids": [1], "keys": [[2]]},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            mpath = write_lake(
                Path(tmp),
                [
                    {"file_id": "f", "path": "data/f.jsonl", "data_sequence": 1,
                     "rows": [{"k": 1}, {"k": 2}]},
                ],
                deletes,
                {"only-one": {"data": ["f"], "deletes": ["listed"]}},
                "only-one",
            )
            verdict = decisions(scan_snapshot(load_manifest(mpath), "only-one"))
            self.assertFalse(verdict[("f", 0)][0])
            self.assertTrue(verdict[("f", 1)][0])  # 目录里存在但快照未列出 => 不可见

    def test_snapshot_referencing_unknown_delete_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            mpath = write_lake(
                Path(tmp),
                [{"file_id": "f", "path": "data/f.jsonl", "data_sequence": 1,
                  "rows": [{"k": 1}]}],
                [{"id": "d", "kind": "equality", "delete_sequence": 9,
                  "field_ids": [1], "keys": [[1]]}],
                {"s": {"data": ["f"], "deletes": ["ghost"]}},
                "s",
            )
            with self.assertRaises(ManifestError):
                load_manifest(mpath)


class ContractEnforcementTests(unittest.TestCase):
    def test_tampered_hash_blocks_read(self):
        manifest = load_manifest(FIXTURE_MANIFEST)
        good = manifest.data_files["old"]
        with self.assertRaises(IntegrityError):
            read_physical_jsonl(
                FIXTURE_MANIFEST.parent / good.path, "0" * 64
            )

    def test_manifest_tampered_hash_blocks_load(self):
        with tempfile.TemporaryDirectory() as tmp:
            mpath = write_lake(
                Path(tmp),
                [{"file_id": "f", "path": "data/f.jsonl", "data_sequence": 1,
                  "rows": [{"k": 1}]}],
                [],
                {"s": {"data": ["f"], "deletes": []}},
                "s",
            )
            doc = json.loads(mpath.read_text(encoding="utf-8"))
            doc["data_files"][0]["sha256"] = "f" * 64
            mpath.write_text(json.dumps(doc), encoding="utf-8")
            with self.assertRaises(IntegrityError):
                load_manifest(mpath)

    def test_negative_position_blocks_read(self):
        with tempfile.TemporaryDirectory() as tmp:
            mpath = write_lake(
                Path(tmp),
                [{"file_id": "f", "path": "data/f.jsonl", "data_sequence": 1,
                  "rows": [{"k": 1}, {"k": 2}]}],
                [{"id": "bad", "kind": "position", "target_file_id": "f",
                  "positions": [-1]}],
                {"s": {"data": ["f"], "deletes": ["bad"]}},
                "s",
            )
            with self.assertRaises(DeleteFormatError):
                scan_snapshot(load_manifest(mpath), "s")

    def test_position_out_of_range_blocks_read(self):
        with tempfile.TemporaryDirectory() as tmp:
            mpath = write_lake(
                Path(tmp),
                [{"file_id": "f", "path": "data/f.jsonl", "data_sequence": 1,
                  "rows": [{"k": 1}]}],
                [{"id": "bad", "kind": "position", "target_file_id": "f",
                  "positions": [5]}],
                {"s": {"data": ["f"], "deletes": ["bad"]}},
                "s",
            )
            with self.assertRaises(DeleteFormatError):
                scan_snapshot(load_manifest(mpath), "s")

    def test_unknown_position_target_blocks_read(self):
        with tempfile.TemporaryDirectory() as tmp:
            mpath = write_lake(
                Path(tmp),
                [{"file_id": "f", "path": "data/f.jsonl", "data_sequence": 1,
                  "rows": [{"k": 1}]}],
                [{"id": "bad", "kind": "position", "target_file_id": "ghost",
                  "positions": [0]}],
                {"s": {"data": ["f"], "deletes": ["bad"]}},
                "s",
            )
            with self.assertRaises(DeleteFormatError):
                scan_snapshot(load_manifest(mpath), "s")

    def test_wrong_key_type_blocks_read(self):
        # 删除键声明为整数，数据行给字符串：类型不符必须阻止读取
        with tempfile.TemporaryDirectory() as tmp:
            mpath = write_lake(
                Path(tmp),
                [{"file_id": "f", "path": "data/f.jsonl", "data_sequence": 1,
                  "rows": [{"k": "1"}]}],
                [{"id": "eq", "kind": "equality", "delete_sequence": 9,
                  "field_ids": [1], "keys": [[1]]}],
                {"s": {"data": ["f"], "deletes": ["eq"]}},
                "s",
            )
            with self.assertRaises(DeleteFormatError):
                scan_snapshot(load_manifest(mpath), "s")

    def test_blank_physical_line_breaks_row_contract(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "data").mkdir()
            (base / "data/f.jsonl").write_text('{"k": 1}\n\n{"k": 2}\n', encoding="utf-8")
            with self.assertRaises(IntegrityError):
                read_physical_jsonl(base / "data/f.jsonl",
                                    hashlib.sha256((base / "data/f.jsonl").read_bytes())
                                    .hexdigest())


class MemoryBudgetTests(unittest.TestCase):
    def test_budget_exceeded_blocks_index_build(self):
        deletes = [
            {"id": "eq", "kind": "equality", "delete_sequence": 9,
             "field_ids": [1], "keys": [[1], [2]]},
            {"id": "pos", "kind": "position", "target_file_id": "f",
             "positions": [0]},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            mpath = write_lake(
                Path(tmp),
                [{"file_id": "f", "path": "data/f.jsonl", "data_sequence": 1,
                  "rows": [{"k": 1}]}],
                deletes,
                {"s": {"data": ["f"], "deletes": ["eq", "pos"]}},
                "s",
            )
            manifest = load_manifest(mpath)
            with self.assertRaises(BudgetExceeded):
                DeleteIndex(manifest, ("eq", "pos"), memory_budget=2)
            with self.assertRaises(BudgetExceeded):
                scan_snapshot(manifest, "s", memory_budget=2)
            # 预算足够时正常工作
            result = scan_snapshot(manifest, "s", memory_budget=3)
            self.assertEqual(len(result.rows), 1)


class RewriteTests(unittest.TestCase):
    def _full_lake(self, tmp):
        deletes = [
            {"id": "eq6", "kind": "equality", "delete_sequence": 6,
             "field_ids": [1], "keys": [[1]]},
            {"id": "pos-old1", "kind": "position", "target_file_id": "old",
             "positions": [1]},
        ]
        return write_lake(
            Path(tmp),
            [
                {"file_id": "old", "path": "data/old.jsonl", "data_sequence": 5,
                 "rows": [{"k": 1, "value": 10}, {"k": 2, "value": 20}]},
                {"file_id": "new", "path": "data/new.jsonl", "data_sequence": 6,
                 "rows": [{"k": 1, "value": 30}, {"k": None, "value": 40}]},
            ],
            deletes,
            {
                "before": {"data": ["old"], "deletes": []},
                "after": {"data": ["old", "new"], "deletes": ["eq6", "pos-old1"]},
            },
            "after",
        )

    def test_rewrite_applies_deletes_then_emits_new_identity_and_sequence(self):
        with tempfile.TemporaryDirectory() as tmp:
            mpath = self._full_lake(tmp)
            manifest = load_manifest(mpath)
            before = scan_snapshot(manifest, "after")
            out = Path(tmp) / "out"
            report = rewrite_snapshot(manifest, out)

            self.assertEqual(report.new_data_sequence, 7)
            self.assertEqual(report.removed_data_file_ids, ("old", "new"))
            # 位置删除只对 old 有效 => 移除；等值删除在全量重写后不再影响任何文件 => 不保留
            self.assertEqual(report.removed_position_delete_ids, ("pos-old1",))
            self.assertEqual(report.retained_equality_delete_ids, ())
            self.assertEqual(report.retained_position_delete_ids, ())

            new_manifest = load_manifest(report.output_manifest)
            # 老快照原封不动，仍解释老文件与行位置
            old_scan = scan_snapshot(new_manifest, "before")
            self.assertEqual(record_multiset(old_scan),
                             record_multiset(scan_snapshot(manifest, "before")))
            # 新快照扫描多重集与重写前保留多重集完全等价
            new_scan = scan_snapshot(new_manifest, report.new_snapshot)
            self.assertEqual(record_multiset(new_scan), record_multiset(before))
            # 新文件使用新 file_id 与新序列
            for written in report.written_files:
                self.assertGreaterEqual(written.data_sequence, 7)
                self.assertTrue(written.file_id.startswith("rw-"))
            # 新快照数据清单不再含被替代文件
            self.assertNotIn("old", new_manifest.snapshots[report.new_snapshot].data)
            self.assertNotIn("new", new_manifest.snapshots[report.new_snapshot].data)
            self.assertEqual(new_manifest.current, report.new_snapshot)

    def test_position_delete_not_reapplied_to_rewritten_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            mpath = self._full_lake(tmp)
            out = Path(tmp) / "out"
            report = rewrite_snapshot(load_manifest(mpath), out)
            new_manifest = load_manifest(report.output_manifest)
            snap = new_manifest.snapshots[report.new_snapshot]
            self.assertEqual(snap.deletes, ())
            # old 文件两行均被删除，重写产物为空文件但仍以新身份登记；
            # 没有任何位置删除可按旧行号作用于新文件
            rw_old = next(w for w in report.written_files if w.file_id == "rw-old")
            self.assertEqual(rw_old.record_count, 0)
            rw_new = next(w for w in report.written_files if w.file_id == "rw-new")
            self.assertEqual(rw_new.record_count, 2)
            rows = read_physical_jsonl(
                report.output_manifest.parent / rw_new.path, rw_new.sha256
            )
            self.assertEqual(rows, [{"k": 1, "value": 30}, {"k": None, "value": 40}])

    def test_merge_rewrite_single_file_equivalent_multiset(self):
        with tempfile.TemporaryDirectory() as tmp:
            mpath = self._full_lake(tmp)
            manifest = load_manifest(mpath)
            before = record_multiset(scan_snapshot(manifest, "after"))
            out = Path(tmp) / "merged"
            report = rewrite_snapshot(manifest, out, merge=True)
            self.assertEqual(len(report.written_files), 1)
            merged = report.written_files[0]
            self.assertEqual(merged.file_id, "rw-merged")
            new_manifest = load_manifest(report.output_manifest)
            after = record_multiset(scan_snapshot(new_manifest, report.new_snapshot))
            self.assertEqual(after, before)

    def test_equality_delete_retained_when_other_file_still_affected(self):
        # 只重写 old；另一个更老的 keep 文件仍受等值删除影响
        deletes = [
            {"id": "eq9", "kind": "equality", "delete_sequence": 9,
             "field_ids": [1], "keys": [[1]]},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            mpath = write_lake(
                Path(tmp),
                [
                    {"file_id": "old", "path": "data/old.jsonl", "data_sequence": 5,
                     "rows": [{"k": 1}, {"k": 2}]},
                    {"file_id": "keep", "path": "data/keep.jsonl", "data_sequence": 4,
                     "rows": [{"k": 1}]},
                ],
                deletes,
                {"s": {"data": ["old", "keep"], "deletes": ["eq9"]}},
                "s",
            )
            manifest = load_manifest(mpath)
            # 只重写 old；更老的 keep(seq=4) 文件仍受等值删除 eq9 影响
            out = Path(tmp) / "partial"
            report = rewrite_snapshot(
                manifest, out, snapshot="s", rewrite_file_ids=["old"]
            )
            self.assertIn("eq9", report.retained_equality_delete_ids)
            self.assertEqual(report.removed_data_file_ids, ("old",))
            new_manifest = load_manifest(report.output_manifest)
            new_snap = new_manifest.snapshots[report.new_snapshot]
            self.assertIn("keep", new_snap.data)  # 未重写文件原样保留
            self.assertIn("eq9", new_snap.deletes)
            # 保留的等值删除不会误删新序列文件（new_data_sequence=10 > 9）
            new_scan = scan_snapshot(new_manifest, report.new_snapshot)
            verdict = decisions(new_scan)
            rw_file = report.written_files[0].file_id
            self.assertTrue(verdict[(rw_file, 0)][0])  # k=1 的新文件行保留
            # 未重写的 keep 文件 k=1 仍被 eq9 删除
            self.assertFalse(verdict[("keep", 0)][0])

    def test_partial_rewrite_ignores_kept_rows_of_unreplaced_files(self):
        # 回归：被跳过文件的保留行不能进入重写计划（曾触发 KeyError）
        with tempfile.TemporaryDirectory() as tmp:
            mpath = write_lake(
                Path(tmp),
                [
                    {"file_id": "a", "path": "data/a.jsonl", "data_sequence": 5,
                     "rows": [{"k": 1}, {"k": 2}]},
                    {"file_id": "b", "path": "data/b.jsonl", "data_sequence": 6,
                     "rows": [{"k": 3}]},
                ],
                [],
                {"s": {"data": ["a", "b"], "deletes": []}},
                "s",
            )
            report = rewrite_snapshot(
                load_manifest(mpath), Path(tmp) / "out",
                snapshot="s", rewrite_file_ids=["b"],
            )
            self.assertEqual([w.file_id for w in report.written_files], ["rw-b"])
            new_manifest = load_manifest(report.output_manifest)
            self.assertEqual(
                new_manifest.snapshots[report.new_snapshot].data, ("rw-b", "a")
            )
            # 重写前后全量多重集一致（无删除时）
            before = record_multiset(scan_snapshot(load_manifest(mpath), "s"))
            after = record_multiset(scan_snapshot(new_manifest, report.new_snapshot))
            self.assertEqual(before, after)

    def test_new_sequence_must_strictly_exceed_existing(self):
        with tempfile.TemporaryDirectory() as tmp:
            mpath = self._full_lake(tmp)
            manifest = load_manifest(mpath)
            with self.assertRaises(RewriteError):
                rewrite_snapshot(manifest, Path(tmp) / "x", new_data_sequence=6)

    def test_rewrite_refuses_nonempty_output_and_keeps_inputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            mpath = self._full_lake(tmp)
            manifest = load_manifest(mpath)
            out = Path(tmp) / "out"
            out.mkdir()
            (out / "occupied").write_text("x")
            with self.assertRaises(RewriteError):
                rewrite_snapshot(manifest, out)
            # 原始资料未被改动
            self.assertEqual(
                load_manifest(mpath).raw["data_files"],
                manifest.raw["data_files"],
            )

    def test_rewrite_accepts_preexisting_empty_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            mpath = self._full_lake(tmp)
            out = Path(tmp) / "empty"
            out.mkdir()
            report = rewrite_snapshot(load_manifest(mpath), out)
            self.assertTrue((report.output_manifest).exists())
            self.assertTrue(any((out / "data").iterdir()))

    def test_old_snapshot_remains_unchanged_in_rewritten_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            mpath = self._full_lake(tmp)
            manifest = load_manifest(mpath)
            out = Path(tmp) / "out"
            report = rewrite_snapshot(manifest, out)
            new_raw = json.loads(report.output_manifest.read_text(encoding="utf-8"))
            self.assertEqual(
                new_raw["snapshots"]["before"], manifest.raw["snapshots"]["before"]
            )
            # 老数据文件字节被原样复制
            old_spec = manifest.data_files["old"]
            copied = report.output_manifest.parent / old_spec.path
            self.assertEqual(
                hashlib.sha256(copied.read_bytes()).hexdigest(), old_spec.sha256
            )


class CliTests(unittest.TestCase):
    SCRIPT = ROOT / "src" / "delete_layer.py"

    def test_scan_cli_emits_verdicts(self):
        proc = subprocess.run(
            [sys.executable, "-I", str(self.SCRIPT),
             str(FIXTURE_MANIFEST), "--snapshot", "after", "scan"],
            cwd=ROOT, capture_output=True, text=True, check=True,
        )
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["snapshot"], "after")
        self.assertEqual(payload["kept"], 2)
        self.assertEqual(payload["deleted"], 2)
        hits = {(r["location"]["file_id"], r["location"]["line_number"]):
                [h["delete_id"] for h in r["hits"]] for r in payload["rows"]}
        self.assertEqual(hits[("old", 0)], ["eq6"])
        self.assertEqual(hits[("old", 1)], ["pos-old1"])
        self.assertEqual(hits[("new", 0)], [])
        self.assertEqual(hits[("new", 1)], [])

    def test_rewrite_cli_then_equivalent_scan(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "bundle"
            proc = subprocess.run(
                [sys.executable, "-I", str(self.SCRIPT),
                 str(FIXTURE_MANIFEST), "rewrite", "--output", str(out)],
                cwd=ROOT, capture_output=True, text=True, check=True,
            )
            report = json.loads(proc.stdout)
            scan = subprocess.run(
                [sys.executable, "-I", str(self.SCRIPT),
                 str(out / "manifest.json"),
                 "--snapshot", report["new_snapshot"], "scan"],
                cwd=ROOT, capture_output=True, text=True, check=True,
            )
            payload = json.loads(scan.stdout)
            original = load_manifest(FIXTURE_MANIFEST)
            self.assertEqual(
                payload["multiset"], record_multiset(scan_snapshot(original, "after"))
            )


if __name__ == "__main__":
    unittest.main()
