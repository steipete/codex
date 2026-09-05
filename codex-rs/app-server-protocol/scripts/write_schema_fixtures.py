#!/usr/bin/env python3

import argparse
import os
from pathlib import Path
import subprocess


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Regenerate vendored app-server schema fixtures"
    )
    parser.add_argument(
        "--schema-root",
        type=Path,
        help="root directory containing the schema fixtures",
    )
    parser.add_argument(
        "-p",
        "--prettier",
        help="optional Prettier executable used to format TypeScript files",
    )
    parser.add_argument(
        "--experimental",
        action="store_true",
        help="regenerate the precomputed experimental exports",
    )
    args = parser.parse_args()

    workspace_root = Path(__file__).resolve().parents[2]
    schema_root = (
        args.schema_root or workspace_root / "app-server-protocol" / "schema"
    ).resolve()

    env = os.environ.copy()
    env["CODEX_APP_SERVER_SCHEMA_ROOT"] = str(schema_root)
    env["CODEX_APP_SERVER_SCHEMA_EXPERIMENTAL"] = "1" if args.experimental else "0"
    if args.prettier:
        prettier = args.prettier
        if os.path.dirname(prettier):
            prettier = str(Path(prettier).resolve())
        env["CODEX_APP_SERVER_SCHEMA_PRETTIER"] = prettier

    subprocess.run(
        [
            "just",
            "test",
            "-p",
            "codex-app-server-protocol",
            "--lib",
            "--run-ignored",
            "ignored-only",
            "-E",
            "test(=schema_fixtures_tests::write_schema_fixtures_from_env)",
        ],
        cwd=workspace_root,
        env=env,
        check=True,
    )


if __name__ == "__main__":
    main()
