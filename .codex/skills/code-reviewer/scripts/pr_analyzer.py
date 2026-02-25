#!/usr/bin/env python3
import argparse
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze a PR target path and emit basic review metadata.")
    parser.add_argument("project_path", nargs="?", default=".")
    parser.add_argument("--format", choices=["text", "json"], default="text")
    args = parser.parse_args()

    path = Path(args.project_path).resolve()
    files = [p for p in path.rglob("*") if p.is_file()]
    payload = {
        "project_path": str(path),
        "file_count": len(files),
    }

    if args.format == "json":
        print(json.dumps(payload, indent=2))
    else:
        print(f"Project: {payload['project_path']}")
        print(f"Files: {payload['file_count']}")
        print("Next: run code_quality_checker.py and review_report_generator.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
