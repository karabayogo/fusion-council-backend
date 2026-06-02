"""Operator-facing CLI for clearing quarantine. See unquarantine.py for the function."""
from __future__ import annotations

import os
import sys

# Add src to path for in-cluster execution
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from fusion_council_service.db import new_session
from fusion_council_service.domain.unquarantine import unquarantine


def main() -> int:
    if len(sys.argv) != 4:
        print(
            "usage: python -m fusion_council_service.scripts.unquarantine_cli "
            "<provider> <provider_model> <reason>",
            file=sys.stderr,
        )
        return 2
    provider, provider_model, reason = sys.argv[1], sys.argv[2], sys.argv[3]
    db = new_session()
    try:
        unquarantine(db, provider, provider_model, reason)
    except ValueError as e:
        print(f"unquarantine failed: {e}", file=sys.stderr)
        return 1
    print(f"unquarantined ({provider}, {provider_model})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
