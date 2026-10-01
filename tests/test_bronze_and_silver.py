"""Bronze idempotency (moto-mocked S3) and silver parsing/point-in-time logic (pure Arrow)."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import boto3
import pyarrow.compute as pc
import pytest
from moto import mock_aws

from sec_lakehouse.config import Settings
from sec_lakehouse.ingest.edgar_fsds import latest_quarters, quarter_id
from sec_lakehouse.lakehouse.facts import parse_zip
from sec_lakehouse.storage import BronzeStore


def test_quarter_helpers():
    assert quarter_id(date(2025, 5, 1)) == "2025q2"
    assert latest_quarters(2, today=date(2025, 9, 23)) == ["2025q1", "2025q2"]
    assert latest_quarters(1, today=date(2025, 1, 5)) == ["2024q4"]


@mock_aws
def test_bronze_put_is_idempotent(tmp_path: Path, tiny_zip: bytes):
    cfg = Settings(target="local", bucket="test-bucket", s3_endpoint_url=None, s3_access_key=None, s3_secret_key=None, aws_region="us-east-1")
    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-bucket")
    store = BronzeStore(cfg, client=boto3.client("s3", region_name="us-east-1"))
    local = tmp_path / "2024q4.zip"
    local.write_bytes(tiny_zip)
    first = store.put_file(local, "bronze/fsds/2024q4/2024q4.zip", "https://example/2024q4.zip", extra={"quarter": "2024q4"})
    second = store.put_file(local, "bronze/fsds/2024q4/2024q4.zip", "https://example/2024q4.zip")
    assert not first.skipped and second.skipped and first.sha256 == second.sha256
    manifest = json.loads(store.get_bytes("bronze/fsds/2024q4/2024q4.zip.manifest.json"))
    assert manifest["sha256"] == first.sha256 and manifest["quarter"] == "2024q4"
    # a changed file (SEC re-publish) is written again with a new hash
    local.write_bytes(tiny_zip + b"\n")
    third = store.put_file(local, "bronze/fsds/2024q4/2024q4.zip", "https://example/2024q4.zip")
    assert not third.skipped and third.sha256 != first.sha256
    assert len(store.manifests("bronze/fsds/")) == 1


def test_parse_zip_types_and_point_in_time(tiny_zip: bytes):
    subs, facts = parse_zip(tiny_zip, "2025q1", "abc")
    assert len(subs) == 2 and len(facts) == 5
    assert subs.column("filed").to_pylist() == [date(2024, 2, 1), date(2025, 2, 1)]
    assert subs.column("prevrpt").to_pylist() == [False, False] and subs.column("cik").to_pylist() == [1000, 1000]
    # FY2023 revenue has two versions with different knowledge dates and the same fact_key
    rev23 = facts.filter(pc.and_(pc.equal(facts["tag"], "Revenues"), pc.equal(facts["ddate"], date(2023, 12, 31))))
    assert sorted(rev23.column("value").to_pylist()) == [100.0, 105.0]
    assert len(set(rev23.column("fact_key").to_pylist())) == 1
    # "as known on 2024-06-30" -> the original 100; "as known today" -> the restated 105
    known = rev23.filter(pc.less_equal(rev23["knowledge_date"], date(2024, 6, 30)))
    assert known.column("value").to_pylist() == [100.0]
    latest = rev23.filter(pc.equal(rev23["knowledge_date"], pc.max(rev23["knowledge_date"])))
    assert latest.column("value").to_pylist() == [105.0] and latest.column("footnote").to_pylist() == ["restated"]
    assert facts.column("quarter").to_pylist() == ["2025q1"] * 5 and facts.column("source_sha256")[0].as_py() == "abc"
    # the segment slice is its own fact, flagged dimensional, and never collides with the consolidated one
    fy24 = facts.filter(pc.equal(facts["ddate"], date(2024, 12, 31)))
    assert sorted(fy24.column("value").to_pylist()) == [70.0, 120.0]
    assert len(set(fy24.column("fact_key").to_pylist())) == 2
    assert sorted(fy24.column("is_dimensional").to_pylist()) == [False, True]


def test_parse_zip_without_segments_column(tiny_zip: bytes):
    """Pre-2020 datasets have no segments column; they must load as undimensioned facts."""
    from conftest import SUB, make_zip  # noqa: PLC0415
    header = "\t".join(["adsh", "tag", "version", "ddate", "qtrs", "uom", "coreg", "value", "footnote"])
    row = "\t".join(["0001-24-000001", "Assets", "us-gaap/2023", "20231231", "0", "USD", "", "500", ""])
    _, facts = parse_zip(make_zip(SUB, header + "\n" + row), "2019q4", "old")
    assert facts.column("segments").to_pylist() == [None] and facts.column("is_dimensional").to_pylist() == [False]


def test_parse_zip_rejects_missing_tables():
    import io  # noqa: PLC0415
    import zipfile  # noqa: PLC0415
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("sub.txt", "adsh\n")
    with pytest.raises(KeyError):
        parse_zip(buf.getvalue(), "2025q1", "x")
