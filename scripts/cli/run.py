"""
ETL Pipeline CLI Entry Point
============================
Unified command-line interface for running the Lakehouse ETL pipeline.

Usage:
    python scripts/cli/run.py full --sources cophieu68 --backend polars
    python scripts/cli/run.py bronze --sources cophieu68,crypto_binance --backend polars
    python scripts/cli/run.py silver --backend sqlmesh
    python scripts/cli/run.py gold --backend spark
    python scripts/cli/run.py full --dry-run  # Validate without executing
    
Examples:
    # Run full pipeline with default config
    $ python scripts/cli/run.py full
    
    # Run just bronze ingestion with Polars
    $ python scripts/cli/run.py bronze --sources cophieu68 --backend polars
    
    # Run silver transformations with SQLMesh
    $ python scripts/cli/run.py silver --backend sqlmesh
    
    # Dry run to validate configuration
    $ python scripts/cli/run.py full --dry-run
"""

import sys
import json
import argparse
from pathlib import Path
from typing import List, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from platforms.orchestration.master_etl_pipeline import MasterETLPipeline, run_master_pipeline


def parse_arguments() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Lakehouse ETL Pipeline Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    
    # Positional: execution mode
    parser.add_argument(
        "mode",
        choices=["full", "bronze", "silver", "gold", "custom"],
        help="Execution mode: full pipeline or specific layer",
    )
    
    # Optional arguments
    parser.add_argument(
        "--sources",
        type=str,
        default="cophieu68",
        help="Comma-separated list of data sources (default: cophieu68)",
    )
    
    parser.add_argument(
        "--layers",
        type=str,
        default=None,
        help="Comma-separated list of layers to execute (default: all relevant for mode)",
    )
    
    parser.add_argument(
        "--backend",
        type=str,
        choices=["polars", "spark", "sqlmesh", "duckdb", "dbt"],
        default="polars",
        help="Processing backend to use (default: polars)",
    )
    
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to configuration YAML file",
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration without executing",
    )
    
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )
    
    parser.add_argument(
        "--output",
        type=str,
        choices=["json", "text", "summary"],
        default="summary",
        help="Output format for results",
    )
    
    return parser.parse_args()


def format_output(result: dict, format_type: str = "summary") -> str:
    """Format execution result for display."""
    if format_type == "json":
        return json.dumps(result, indent=2, default=str)
    
    elif format_type == "text":
        lines = []
        lines.append("=" * 70)
        lines.append(f"ETL Pipeline Execution Result")
        lines.append("=" * 70)
        lines.append(f"Run ID:      {result.get('run_id', 'N/A')}")
        lines.append(f"Status:      {result.get('status', 'N/A')}")
        
        if "phases" in result:
            lines.append("\nPhases:")
            for phase_name, phase_result in result.get("phases", {}).items():
                lines.append(f"  {phase_name:12} → {phase_result.get('phase', 'N/A')}")
        
        if "metrics" in result:
            lines.append("\nMetrics:")
            metrics = result["metrics"]
            lines.append(f"  Duration:          {metrics.get('total_duration_seconds', 0):.1f}s")
            lines.append(f"  Total Errors:      {metrics.get('total_errors', 0)}")
        
        if "error_events" in result and result["error_events"]:
            lines.append("\nErrors:")
            for err in result["error_events"][:5]:  # Show first 5
                lines.append(f"  - [{err.get('error_level', 'ERROR')}] {err.get('error_message', 'Unknown')}")
            if len(result["error_events"]) > 5:
                lines.append(f"  ... and {len(result['error_events']) - 5} more")
        
        if "error" in result:
            lines.append(f"\nError Details:")
            lines.append(f"  {result['error']}")
        
        lines.append("=" * 70)
        return "\n".join(lines)
    
    else:  # summary (default)
        lines = []
        lines.append(f"✓ Pipeline Execution Summary")
        lines.append(f"  Status:    {result.get('status', 'UNKNOWN')}")
        lines.append(f"  Run ID:    {result.get('run_id', 'N/A')[:8]}...")
        if "metrics" in result:
            lines.append(f"  Duration:  {result['metrics'].get('total_duration_seconds', 0):.1f}s")
        return "\n".join(lines)


def main():
    """Main CLI entry point."""
    args = parse_arguments()
    
    # Parse sources
    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    
    # Parse layers if provided
    layers = None
    if args.layers:
        layers = [l.strip() for l in args.layers.split(",") if l.strip()]
    
    # Run pipeline
    try:
        result = run_master_pipeline(
            mode=args.mode,
            sources=sources,
            backend=args.backend,
            config_path=args.config,
            dry_run=args.dry_run,
        )
        
        # Format and print output
        output = format_output(result, format_type=args.output)
        print(output)
        
        # Exit with appropriate code
        status = result.get("status", "UNKNOWN")
        if status in ("SUCCESS", "COMPLETED", "VALIDATION_PASSED"):
            sys.exit(0)
        else:
            sys.exit(1)
    
    except Exception as exc:
        print(f"❌ Pipeline execution failed: {exc}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
