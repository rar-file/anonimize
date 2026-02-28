#!/usr/bin/env python3
"""Anonimize CLI - Command line interface for data anonymization.

Usage:
    anonimize FILE [--output OUT] [--strategy STRATEGY] [--dry-run]
    anonimize --wizard
    anonimize detect FILE
    anonimize preview FILE
    anonimize config --generate

Examples:
    # Simple anonymization
    anonimize customers.csv
    
    # With options
    anonimize users.json --output users_safe.json --strategy mask
    
    # Preview changes
    anonimize data.csv --dry-run
    
    # Interactive wizard
    anonimize --wizard
"""

import sys
import argparse
import json
from pathlib import Path
from typing import Optional

from anonimize import anonymize, anonymize_data, detect_pii, preview, __version__
from anonimize.cli.wizard import run_wizard


def print_error(message: str):
    """Print an error message."""
    print(f"✗ {message}", file=sys.stderr)


def print_success(message: str):
    """Print a success message."""
    print(f"✓ {message}")


def print_info(message: str):
    """Print an info message."""
    print(f"ℹ {message}")


def cmd_detect(args) -> int:
    """Handle the detect command."""
    file_path = Path(args.file)
    
    if not file_path.exists():
        print_error(f"File not found: {file_path}")
        print_info("Hint: Check the path and ensure the file exists.")
        return 1
    
    try:
        detected = detect_pii(file_path)
        
        if args.format == "json":
            print(json.dumps(detected, indent=2))
        else:
            if not detected:
                print_info("No PII detected in this file")
                return 0
            
            print(f"\nDetected PII in {file_path.name}:\n")
            print(f"  {'Column':<20} {'Type':<15} {'Confidence'}")
            print(f"  {'-'*20} {'-'*15} {'-'*10}")
            
            for col, info in detected.items():
                pii_type = info.get("type", "unknown") if isinstance(info, dict) else info
                confidence = info.get("confidence", "auto") if isinstance(info, dict) else "auto"
                conf_str = f"{confidence:.0%}" if isinstance(confidence, float) else str(confidence)
                print(f"  {col:<20} {pii_type:<15} {conf_str}")
            
            print(f"\n  Total: {len(detected)} PII field(s) detected")
        
        return 0
        
    except Exception as e:
        print_error(f"Detection failed: {e}")
        return 1


def cmd_preview_cmd(args) -> int:
    """Handle the preview command."""
    file_path = Path(args.file)
    
    if not file_path.exists():
        print_error(f"File not found: {file_path}")
        return 1
    
    try:
        preview_data = preview(file_path, num_rows=args.num_rows, strategy=args.strategy)
        
        if not preview_data:
            print_info("No data to preview")
            return 0
        
        print(f"\nPreview ({args.strategy} strategy, first {len(preview_data)} rows):\n")
        
        # Get headers from first row
        headers = list(preview_data[0].keys())
        
        # Print table
        col_widths = {h: max(len(h), 15) for h in headers}
        for row in preview_data:
            for h in headers:
                col_widths[h] = max(col_widths[h], len(str(row.get(h, ""))) + 2)
        
        header_row = " | ".join(h.ljust(col_widths[h]) for h in headers[:6])  # Limit cols
        print(f"  {header_row}")
        print(f"  {'-' * len(header_row)}")
        
        for row in preview_data:
            values = [str(row.get(h, ""))[:col_widths[h]-2].ljust(col_widths[h]) 
                     for h in headers[:6]]
            print(f"  {' | '.join(values)}")
        
        print()
        return 0
        
    except Exception as e:
        print_error(f"Preview failed: {e}")
        return 1


def cmd_config(args) -> int:
    """Handle the config command."""
    if args.generate:
        config_content = '''# Anonimize Configuration File
# This is a sample configuration for anonimize

# Global settings
global:
  locale: "en_US"           # Locale for fake data generation
  seed: 42                  # Random seed for reproducibility
  preserve_relationships: true  # Keep same fake value for same real value

# Column-specific settings
# Each column can have its own strategy and options
columns:
  email:
    strategy: "mask"        # Options: replace, mask, hash, remove
    type: "email"
    
  name:
    strategy: "replace"
    type: "name"
    
  ssn:
    strategy: "hash"
    type: "ssn"
    options:
      algorithm: "sha256"
      
  phone:
    strategy: "mask"
    type: "phone"
    options:
      preserve_last: 4
      mask_char: "*"

# Detection settings
detection:
  confidence_threshold: 0.7
  check_field_names: true
'''
        output_path = Path(args.output)
        
        if output_path.exists():
            print_error(f"File already exists: {output_path}")
            if input("Overwrite? [y/N]: ").lower() not in ("y", "yes"):
                return 1
        
        with open(output_path, "w") as f:
            f.write(config_content)
        
        print_success(f"Generated config file: {output_path}")
        print_info("Edit this file and use with: anonimize data.csv --config " + str(output_path))
        return 0
    
    else:
        print_info("Use --generate to create a sample config file")
        return 0


def cmd_anonymize(args) -> int:
    """Handle the main anonymize command."""
    file_path = Path(args.file)
    
    if not file_path.exists():
        print_error(f"File not found: {file_path}")
        print_info("Hint: Check the path and ensure the file exists.")
        print_info("      Run `anonimize --wizard` for guided setup.")
        return 1
    
    # Parse columns if specified
    columns = None
    if args.columns:
        columns = [c.strip() for c in args.columns.split(",")]
    
    try:
        result = anonymize(
            file_path,
            args.output,
            strategy=args.strategy,
            dry_run=args.dry_run,
            progress=not args.no_progress,
            columns=columns,
            locale=args.locale,
            seed=args.seed
        )
        
        if args.dry_run:
            print(f"\n🔍 DRY RUN - No changes made\n")
            
            if isinstance(result, dict):
                if "would_anonymize" in result:
                    cols = result["would_anonymize"]
                    if cols:
                        print(f"Would anonymize {len(cols)} column(s): {', '.join(cols)}")
                    else:
                        print("No columns would be anonymized")
                
                if "detected_pii" in result:
                    print(f"\nDetected PII:")
                    for col, info in result["detected_pii"].items():
                        pii_type = info.get("type", "unknown") if isinstance(info, dict) else info
                        print(f"  - {col}: {pii_type}")
                
                if "preview" in result and result["preview"]:
                    print(f"\nPreview:")
                    for row in result["preview"][:3]:
                        print(f"  {row}")
            
            print(f"\nTo apply changes, run without --dry-run")
        else:
            print()
            print_success("Anonymization complete!")
            print()
            print(f"  Input:  {file_path}")
            print(f"  Output: {result}")
            
            if isinstance(result, str):
                output_path = Path(result)
                if output_path.exists():
                    size = output_path.stat().st_size
                    print(f"  Size:   {size:,} bytes")
        
        return 0
        
    except Exception as e:
        print_error(f"Anonymization failed: {e}")
        print_info("Common solutions:")
        print("  - Check that the input file isn't open in another program")
        print("  - Ensure you have write permissions for the output directory")
        print("  - Verify the file format matches the extension")
        return 1


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser."""
    parser = argparse.ArgumentParser(
        prog="anonimize",
        description="""
Anonimize - Simple data anonymization for everyone.

Quick Start:
    anonimize data.csv                    # Auto-anonymize a CSV file
    anonimize data.json --dry-run         # Preview changes first
    anonimize --wizard                    # Interactive guided mode
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
For more help: https://github.com/rar-file/anonimize
Report issues: https://github.com/rar-file/anonimize/issues
        """
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}"
    )
    
    # Subparsers for commands
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    
    # Main anonymize command
    anonymize_parser = subparsers.add_parser(
        "anonymize",
        aliases=["anon"],
        help="Anonymize a file (default if no command specified)"
    )
    anonymize_parser.add_argument(
        "file",
        help="Input file to anonymize (CSV, JSON, JSONL)"
    )
    anonymize_parser.add_argument(
        "-o", "--output",
        help="Output file path (default: input.anonymized.ext)"
    )
    anonymize_parser.add_argument(
        "-s", "--strategy",
        choices=["replace", "mask", "hash", "remove"],
        default="replace",
        help="Anonymization strategy (default: replace)"
    )
    anonymize_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be anonymized without making changes"
    )
    anonymize_parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable progress bar for large files"
    )
    anonymize_parser.add_argument(
        "-c", "--columns",
        help="Comma-separated list of columns to anonymize (default: auto-detect)"
    )
    anonymize_parser.add_argument(
        "--locale",
        default="en_US",
        help="Locale for fake data generation (default: en_US)"
    )
    anonymize_parser.add_argument(
        "--seed",
        type=int,
        help="Random seed for reproducible results"
    )
    
    # Detect command
    detect_parser = subparsers.add_parser(
        "detect",
        help="Detect PII in a file without anonymizing"
    )
    detect_parser.add_argument("file", help="File to analyze")
    detect_parser.add_argument(
        "--format",
        choices=["table", "json"],
        default="table",
        help="Output format"
    )
    
    # Preview command
    preview_parser = subparsers.add_parser(
        "preview",
        help="Preview anonymization on first few rows"
    )
    preview_parser.add_argument("file", help="File to preview")
    preview_parser.add_argument(
        "-n", "--num-rows",
        type=int,
        default=3,
        help="Number of rows to preview (default: 3)"
    )
    preview_parser.add_argument(
        "-s", "--strategy",
        choices=["replace", "mask", "hash", "remove"],
        default="replace",
        help="Strategy to preview"
    )
    
    # Config command
    config_parser = subparsers.add_parser(
        "config",
        help="Configuration file utilities"
    )
    config_parser.add_argument(
        "--generate",
        action="store_true",
        help="Generate a sample config file"
    )
    config_parser.add_argument(
        "-o", "--output",
        default="anonimize.yaml",
        help="Output config file name (default: anonimize.yaml)"
    )
    
    # Wizard flag (top level)
    parser.add_argument(
        "-w", "--wizard",
        action="store_true",
        help="Launch interactive wizard"
    )
    
    return parser


def main():
    """Main entry point."""
    parser = create_parser()
    args = parser.parse_args()
    
    # Launch wizard if requested
    if args.wizard:
        return run_wizard()
    
    # Handle subcommands
    if args.command in ("anonymize", "anon"):
        return cmd_anonymize(args)
    
    elif args.command == "detect":
        return cmd_detect(args)
    
    elif args.command == "preview":
        return cmd_preview_cmd(args)
    
    elif args.command == "config":
        return cmd_config(args)
    
    # No command specified - check if a file path was given directly
    if len(sys.argv) > 1 and not sys.argv[1].startswith("-"):
        # Treat first argument as a file path
        file_path = Path(sys.argv[1])
        if file_path.exists():
            # Create a mock args object for anonymize
            class MockArgs:
                pass
            mock_args = MockArgs()
            mock_args.file = str(file_path)
            mock_args.output = None
            mock_args.strategy = "replace"
            mock_args.dry_run = False
            mock_args.no_progress = False
            mock_args.columns = None
            mock_args.locale = "en_US"
            mock_args.seed = None
            
            # Parse remaining args
            i = 2
            while i < len(sys.argv):
                arg = sys.argv[i]
                if arg in ("-o", "--output") and i + 1 < len(sys.argv):
                    mock_args.output = sys.argv[i + 1]
                    i += 2
                elif arg in ("-s", "--strategy") and i + 1 < len(sys.argv):
                    mock_args.strategy = sys.argv[i + 1]
                    i += 2
                elif arg == "--dry-run":
                    mock_args.dry_run = True
                    i += 1
                elif arg == "--no-progress":
                    mock_args.no_progress = True
                    i += 1
                elif arg in ("-c", "--columns") and i + 1 < len(sys.argv):
                    mock_args.columns = sys.argv[i + 1]
                    i += 2
                elif arg == "--locale" and i + 1 < len(sys.argv):
                    mock_args.locale = sys.argv[i + 1]
                    i += 2
                elif arg == "--seed" and i + 1 < len(sys.argv):
                    mock_args.seed = int(sys.argv[i + 1])
                    i += 2
                else:
                    i += 1
            
            return cmd_anonymize(mock_args)
    
    # No valid command or file, show help
    parser.print_help()
    print()
    print("Quick start:")
    print("  anonimize data.csv                    # Anonymize a file")
    print("  anonimize detect data.csv             # Detect PII only")
    print("  anonimize --wizard                    # Interactive wizard")
    return 0


if __name__ == "__main__":
    sys.exit(main())
