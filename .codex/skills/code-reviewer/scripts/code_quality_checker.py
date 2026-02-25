#!/usr/bin/env python3
import argparse
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Run lightweight quality checks on a target path.")
    parser.add_argument("target_path", nargs="?", default=".")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    path = Path(args.target_path).resolve()
    files = [p for p in path.rglob("*") if p.is_file()]
    exts = {}
    for f in files:
        ext = f.suffix.lower() or "<no_ext>"
        exts[ext] = exts.get(ext, 0) + 1

    print(f"Target: {path}")
    print(f"Total files: {len(files)}")
    print("File extension summary:")
    for ext, count in sorted(exts.items(), key=lambda item: (-item[1], item[0]))[:20]:
        print(f"- {ext}: {count}")

    if args.verbose:
        print("Verbose mode enabled. Integrate project-specific linters/tests here.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
