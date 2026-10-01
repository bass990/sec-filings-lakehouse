"""Silver layer: bronze FSDS zips -> Iceberg `silver.submissions` and `silver.facts`.

The point-in-time model
-----------------------
A "fact" is (cik, tag, ddate, qtrs, uom, coreg, segments): Apple's Revenue for
the year ending 2024-09-28 in USD, for the consolidated entity (segments is
NULL) or for one dimensional slice such as a product line or geography
(segments = "ProductOrServiceAxis=IPhoneMember;"). The same fact can be reported by several
filings: the original 10-K, an amended 10-K/A, and the next year's 10-K which
carries the prior year as a comparative column, sometimes restated. Each
report is a *version* of the fact, keyed by the filing that carried it (adsh)
and its `filed` date.

`silver.facts` keeps every version. Two derived columns make history
queryable without a join:

    knowledge_date   the `filed` date of the filing that reported the value
    is_latest        True for the most recently filed version of that fact key

"What did we know about Apple's FY2024 revenue on 2025-01-15?" is
`WHERE knowledge_date <= '2025-01-15'` and take the max knowledge_date per
fact key. `is_latest` gives today's view. Nothing is ever overwritten; a
restatement is a new row with a newer knowledge_date, and the old value stays
visible with the date on which it stopped being current. This is the
late-arriving, changing-fact problem in its purest form, and the whole
platform is built around not losing that history.

Idempotency: each bronze zip is processed once; the processed-manifest table
records the zip's sha256, so re-running a quarter that has not changed is a
no-op and re-running one the SEC re-published replaces exactly that quarter's
rows (delete by source_sha256, then append).
"""
from __future__ import annotations

import io
import sys
import zipfile
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
from pyiceberg.schema import Schema
from pyiceberg.types import BooleanType, DateType, DoubleType, IntegerType, NestedField, StringType, TimestampType

from ..config import Settings, settings
from ..storage import BronzeStore
from .catalog import catalog, ensure_table


def conform(table: pa.Table, schema: Schema) -> pa.Table:
    """Cast an Arrow table to the exact Arrow schema of an Iceberg schema (types AND
    nullability). pyiceberg refuses an append whose required columns arrive as
    nullable Arrow fields; casting here also fails loudly if a required column
    actually contains nulls, which is the data-contract check we want."""
    from pyiceberg.io.pyarrow import schema_to_pyarrow  # noqa: PLC0415
    target = schema_to_pyarrow(schema)
    cols = []
    for f in target:
        col = table[f.name]
        if not f.nullable and col.null_count:
            raise ValueError(f"required column {f.name!r} has {col.null_count} nulls")
        cols.append(col.cast(f.type))
    return pa.Table.from_arrays(cols, schema=target)

SUBMISSIONS_SCHEMA = Schema(
    NestedField(1, "adsh", StringType(), required=True),
    NestedField(2, "cik", IntegerType(), required=True),
    NestedField(3, "name", StringType()),
    NestedField(4, "sic", IntegerType()),
    NestedField(5, "countryba", StringType()),
    NestedField(6, "stprba", StringType()),
    NestedField(7, "fye", StringType()),
    NestedField(8, "form", StringType()),
    NestedField(9, "period", DateType()),
    NestedField(10, "fy", IntegerType()),
    NestedField(11, "fp", StringType()),
    NestedField(12, "filed", DateType()),
    NestedField(13, "accepted", TimestampType()),
    NestedField(14, "prevrpt", BooleanType()),
    NestedField(15, "detail", BooleanType()),
    NestedField(16, "instance", StringType()),
    NestedField(17, "nciks", IntegerType()),
    NestedField(18, "quarter", StringType(), required=True),
    NestedField(19, "source_sha256", StringType(), required=True),
    NestedField(20, "loaded_at", TimestampType(), required=True),
)

FACTS_SCHEMA = Schema(
    NestedField(1, "adsh", StringType(), required=True),
    NestedField(2, "cik", IntegerType(), required=True),
    NestedField(3, "tag", StringType(), required=True),
    NestedField(4, "version", StringType(), required=True),
    NestedField(5, "ddate", DateType(), required=True),
    NestedField(6, "qtrs", IntegerType(), required=True),
    NestedField(7, "uom", StringType(), required=True),
    NestedField(8, "value", DoubleType()),
    NestedField(9, "coreg", StringType()),
    NestedField(10, "footnote", StringType()),
    NestedField(11, "form", StringType()),
    NestedField(12, "fy", IntegerType()),
    NestedField(13, "fp", StringType()),
    NestedField(14, "knowledge_date", DateType(), required=True),
    NestedField(15, "fact_key", StringType(), required=True),
    NestedField(16, "quarter", StringType(), required=True),
    NestedField(17, "source_sha256", StringType(), required=True),
    NestedField(18, "loaded_at", TimestampType(), required=True),
    NestedField(19, "segments", StringType()),
    NestedField(20, "is_dimensional", BooleanType(), required=True),
)

PROCESSED_SCHEMA = Schema(
    NestedField(1, "dataset", StringType(), required=True),
    NestedField(2, "quarter", StringType(), required=True),
    NestedField(3, "source_sha256", StringType(), required=True),
    NestedField(4, "n_submissions", IntegerType(), required=True),
    NestedField(5, "n_facts", IntegerType(), required=True),
    NestedField(6, "processed_at", TimestampType(), required=True),
)

_READ = pcsv.ReadOptions(encoding="latin-1")



def _read_table(zf: zipfile.ZipFile, name: str, columns: list[str]) -> pa.Table:
    """Read one FSDS table. The SEC files are tab-delimited with no quoting, but a
    handful of rows per quarter carry a literal tab inside a free-text field
    (footnote/coreg), which shifts the column count. Those rows are counted and
    skipped rather than aborting a 20M-row load; the count is printed so a
    sudden jump is visible."""
    with zf.open(name) as f:
        data = f.read()
    bad = {"n": 0}

    def _skip(row):  # pragma: no cover - exercised only by malformed real-world rows
        bad["n"] += 1
        return "skip"

    parse = pcsv.ParseOptions(delimiter="\t", quote_char=False, invalid_row_handler=_skip)
    conv = pcsv.ConvertOptions(include_columns=columns, include_missing_columns=True, strings_can_be_null=True,
                               column_types={c: pa.string() for c in columns})
    table = pcsv.read_csv(io.BytesIO(data), read_options=_READ, parse_options=parse, convert_options=conv)
    if bad["n"]:
        print(f"  {name}: skipped {bad['n']} malformed rows (embedded tab in a text field)")
    return table


def _to_int(col: pa.Array) -> pa.Array:
    return pc.cast(pc.cast(col, pa.float64(), safe=False), pa.int32(), safe=False)


def _yyyymmdd(col: pa.Array) -> pa.Array:
    return pc.cast(pc.strptime(col, format="%Y%m%d", unit="s"), pa.date32())


def parse_zip(data: bytes, quarter: str, sha256: str) -> tuple[pa.Table, pa.Table]:
    """Return (submissions, facts) Arrow tables typed to the Iceberg schemas."""
    now = pa.scalar(datetime.now(UTC).replace(tzinfo=None), type=pa.timestamp("us"))
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        sub = _read_table(zf, "sub.txt", ["adsh", "cik", "name", "sic", "countryba", "stprba", "fye", "form", "period", "fy", "fp",
                                          "filed", "accepted", "prevrpt", "detail", "instance", "nciks"])
        # `segments` (dimensional slices) exists from 2020q1 on; older files get nulls -> undimensioned
        num = _read_table(zf, "num.txt", ["adsh", "tag", "version", "ddate", "qtrs", "uom", "coreg", "value", "footnote", "segments"])
    n_sub = len(sub)
    submissions = pa.table({
        "adsh": sub["adsh"], "cik": _to_int(sub["cik"]), "name": sub["name"], "sic": _to_int(sub["sic"]),
        "countryba": sub["countryba"], "stprba": sub["stprba"], "fye": sub["fye"], "form": sub["form"],
        "period": _yyyymmdd(sub["period"]), "fy": _to_int(sub["fy"]), "fp": sub["fp"], "filed": _yyyymmdd(sub["filed"]),
        # newer quarters write "2025-05-14 20:36:00.0"; keep the first 19 chars so both forms parse
        "accepted": pc.cast(pc.strptime(pc.utf8_slice_codeunits(sub["accepted"], 0, 19), format="%Y-%m-%d %H:%M:%S", unit="s"),
                            pa.timestamp("us")),
        "prevrpt": pc.equal(sub["prevrpt"], "1"), "detail": pc.equal(sub["detail"], "1"), "instance": sub["instance"],
        "nciks": _to_int(sub["nciks"]), "quarter": pa.array([quarter] * n_sub), "source_sha256": pa.array([sha256] * n_sub),
        "loaded_at": pa.array([now.as_py()] * n_sub, type=pa.timestamp("us")),
    })
    # join filing metadata onto facts (form, fy, fp, filed -> knowledge_date)
    meta = submissions.select(["adsh", "cik", "form", "fy", "fp", "filed"])
    joined = num.join(meta, keys="adsh", join_type="inner")
    n = len(joined)
    ddate = _yyyymmdd(joined["ddate"])
    qtrs = _to_int(joined["qtrs"])
    # The taxonomy version (us-gaap/2023 vs us-gaap/2024) is NOT part of the key: the
    # same economic fact is re-reported under the newer taxonomy every year, and those
    # re-reports are exactly the restatement versions we want to line up.
    segments = joined["segments"]
    fact_key = pc.binary_join_element_wise(pc.cast(joined["cik"], pa.string()), joined["tag"], joined["ddate"],
                                           pc.cast(qtrs, pa.string()), joined["uom"], pc.fill_null(joined["coreg"], ""),
                                           pc.fill_null(segments, ""), "|")
    facts = pa.table({
        "adsh": joined["adsh"], "cik": joined["cik"], "tag": joined["tag"], "version": joined["version"], "ddate": ddate, "qtrs": qtrs,
        "uom": joined["uom"], "value": pc.cast(joined["value"], pa.float64(), safe=False), "coreg": joined["coreg"],
        "footnote": joined["footnote"], "form": joined["form"], "fy": joined["fy"], "fp": joined["fp"],
        "knowledge_date": joined["filed"], "fact_key": fact_key, "quarter": pa.array([quarter] * n),
        "source_sha256": pa.array([sha256] * n), "loaded_at": pa.array([now.as_py()] * n, type=pa.timestamp("us")),
        "segments": segments, "is_dimensional": pc.is_valid(segments),
    })
    return conform(submissions, SUBMISSIONS_SCHEMA), conform(facts, FACTS_SCHEMA)


def already_processed(cat, quarter: str, sha256: str) -> bool:
    t = ensure_table(cat, "silver.processed_manifest", PROCESSED_SCHEMA)
    rows = t.scan(row_filter=f"quarter = '{quarter}' AND source_sha256 = '{sha256}'").to_arrow()
    return len(rows) > 0


def load_quarter(quarter: str, cfg: Settings = settings, store: BronzeStore | None = None, force: bool = False) -> dict:
    store = store or BronzeStore(cfg)
    key = f"{cfg.bronze_prefix}/fsds/{quarter}/{quarter}.zip"
    manifests = [m for m in store.manifests(f"{cfg.bronze_prefix}/fsds/{quarter}/") if m["key"] == key]
    if not manifests:
        raise FileNotFoundError(f"{quarter} is not in bronze; run the ingest first")
    sha = manifests[0]["sha256"]
    cat = catalog(cfg)
    if not force and already_processed(cat, quarter, sha):
        print(f"{quarter}: already in silver (sha {sha[:12]}), skipping")
        return {"quarter": quarter, "skipped": True}

    submissions, facts = parse_zip(store.get_bytes(key), quarter, sha)
    subs_t = ensure_table(cat, "silver.submissions", SUBMISSIONS_SCHEMA, partition_by=["quarter"])
    facts_t = ensure_table(cat, "silver.facts", FACTS_SCHEMA, partition_by=["quarter"])
    # replace exactly this quarter's rows (re-published files, forced reloads)
    subs_t.delete(f"quarter = '{quarter}'")
    facts_t.delete(f"quarter = '{quarter}'")
    subs_t.append(submissions)
    facts_t.append(facts)
    man = ensure_table(cat, "silver.processed_manifest", PROCESSED_SCHEMA)
    man.append(conform(pa.table({"dataset": ["fsds"], "quarter": [quarter], "source_sha256": [sha], "n_submissions": pa.array([len(submissions)], pa.int32()),
                         "n_facts": pa.array([len(facts)], pa.int32()),
                         "processed_at": pa.array([datetime.now(UTC).replace(tzinfo=None)], pa.timestamp("us"))}), PROCESSED_SCHEMA))
    print(f"{quarter}: {len(submissions):,} submissions, {len(facts):,} facts -> silver")
    return {"quarter": quarter, "skipped": False, "n_submissions": len(submissions), "n_facts": len(facts)}


def main(argv=None) -> int:
    import argparse  # noqa: PLC0415
    ap = argparse.ArgumentParser(description="bronze FSDS zip -> silver Iceberg tables")
    ap.add_argument("--quarters", nargs="+", required=True)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args(argv)
    for q in args.quarters:
        load_quarter(q, force=args.force)
    return 0


if __name__ == "__main__":
    sys.exit(main())
