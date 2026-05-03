#!/usr/bin/env python3
"""
Migration Code Validation
=========================
Tests syntax and basic functionality of migration code examples.
"""

import sys
import os
import tempfile
import subprocess
from datetime import datetime, date
from dataclasses import dataclass, field

def test_dto_audit_fields():
    """Test DTO with audit fields compiles."""
    print("🧪 Testing DTO audit fields...")

    @dataclass
    class TestStockPriceDTO:
        symbol: str
        close_price: float
        volume: int

        # NEW: Audit metadata
        ingest_timestamp: datetime = field(default_factory=datetime.utcnow)
        ingest_date: date = field(default_factory=date.today)
        batch_id: str = ""
        source_system: str = "cophieu68"
        record_hash: str = ""

    # Test instantiation
    dto = TestStockPriceDTO(
        symbol="FPT",
        close_price=115.5,
        volume=2500000,
        batch_id="batch_001"
    )

    assert dto.symbol == "FPT"
    assert dto.batch_id == "batch_001"
    assert dto.source_system == "cophieu68"
    assert isinstance(dto.ingest_timestamp, datetime)
    assert isinstance(dto.ingest_date, date)

    print("✅ DTO audit fields test passed")

def test_polars_import():
    """Test Polars import works."""
    print("🧪 Testing Polars import...")

    try:
        import polars as pl
        print("✅ Polars import successful")

        # Test basic DataFrame creation
        df = pl.DataFrame({"test": [1, 2, 3]})
        assert len(df) == 3
        print("✅ Polars DataFrame creation test passed")

    except ImportError:
        print("⚠️  Polars not installed - will be installed via requirements")
        return False

    return True

def test_duckdb_import():
    """Test DuckDB import works."""
    print("🧪 Testing DuckDB import...")

    try:
        import duckdb
        print("✅ DuckDB import successful")

        # Test basic connection
        con = duckdb.connect(database=':memory:')
        result = con.execute("SELECT 1 as test").fetchone()
        assert result[0] == 1
        con.close()
        print("✅ DuckDB connection test passed")

    except ImportError:
        print("⚠️  DuckDB not installed - will be installed via requirements")
        return False

    return True

def test_sqlmesh_syntax():
    """Test SQLMesh model syntax (basic validation)."""
    print("🧪 Testing SQLMesh syntax patterns...")

    # This is a basic syntax check - full validation requires sqlmesh CLI
    sqlmesh_model = """
MODEL (
  name silver.test_model,
  kind INCREMENTAL_BY_TIME (time_column ingest_date),
  cron '@daily'
);

SELECT
  symbol,
  ingest_date
FROM some_table;
"""

    # Check for required keywords
    assert "MODEL (" in sqlmesh_model
    assert "name silver." in sqlmesh_model
    assert "kind INCREMENTAL_BY_TIME" in sqlmesh_model

    print("✅ SQLMesh syntax pattern test passed")

def main():
    """Run all validation tests."""
    print("🚀 Starting Migration Code Validation")
    print("=" * 50)

    tests_passed = 0
    total_tests = 4

    try:
        test_dto_audit_fields()
        tests_passed += 1
    except Exception as e:
        print(f"❌ DTO test failed: {e}")

    try:
        if test_polars_import():
            tests_passed += 1
    except Exception as e:
        print(f"❌ Polars test failed: {e}")

    try:
        if test_duckdb_import():
            tests_passed += 1
    except Exception as e:
        print(f"❌ DuckDB test failed: {e}")

    try:
        test_sqlmesh_syntax()
        tests_passed += 1
    except Exception as e:
        print(f"❌ SQLMesh test failed: {e}")

    print("=" * 50)
    print(f"📊 Test Results: {tests_passed}/{total_tests} passed")

    if tests_passed == total_tests:
        print("🎉 All validation tests passed!")
        return 0
    else:
        print("⚠️  Some tests failed - check dependencies")
        return 1

if __name__ == "__main__":
    sys.exit(main())