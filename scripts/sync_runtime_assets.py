#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from visu2.config import get_settings
from visu2.hf_sync import ensure_runtime_assets_from_hf, load_hf_repo_config


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Prefetch runtime assets from a Hugging Face dataset repository."
    )
    parser.add_argument("--repo-id", type=str, default=None, help="HF dataset repo id (`org/name`).")
    parser.add_argument("--revision", type=str, default=None, help="Pinned HF revision/tag.")
    parser.add_argument(
        "--repo-type",
        type=str,
        default="dataset",
        help="HF repo type. Default: dataset.",
    )
    parser.add_argument(
        "--token-env",
        type=str,
        default="HF_TOKEN",
        help="Environment variable name containing HF token.",
    )
    parser.add_argument(
        "--allow-patterns-json",
        type=str,
        default=None,
        help="Optional JSON array override for allow_patterns.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero if no HF config is provided.",
    )
    args = parser.parse_args()

    env = dict(os.environ)
    if args.repo_id:
        env["VISU2_HF_REPO_ID"] = args.repo_id
    if args.revision:
        env["VISU2_HF_REVISION"] = args.revision
    if args.repo_type:
        env["VISU2_HF_REPO_TYPE"] = args.repo_type
    if args.allow_patterns_json is not None:
        env["VISU2_HF_ALLOW_PATTERNS_JSON"] = args.allow_patterns_json

    token = os.getenv(args.token_env)
    if token:
        env["HF_TOKEN"] = token

    try:
        config = load_hf_repo_config(environ=env)
    except Exception as err:
        print(f"Configuration error: {err}")
        return 1

    if config is None:
        print("No VISU2_HF_REPO_ID configured; skipping HF runtime sync.")
        return 1 if args.strict else 0

    settings = get_settings()
    try:
        result = ensure_runtime_assets_from_hf(settings, config)
    except Exception as err:
        print(f"HF sync failed: {err}")
        return 1

    print(
        json.dumps(
            {
                "mode": result.mode,
                "repo_id": result.repo_id,
                "revision": result.revision,
                "downloaded": result.downloaded,
                "files_checked": result.files_checked,
                "missing_files": list(result.missing_files),
                "message": result.message,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
