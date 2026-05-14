#!/usr/bin/env python3
"""Initialize the database schema."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from fusion_council_service.db import new_session, initialize_schema


def main():
    db_url = os.environ.get("DATABASE_URL", "")
    db_path = os.environ.get("DATABASE_PATH", "./data/fusion_council.db")
    target = db_url if db_url else db_path
    print(f"Initializing database at: {target}")
    db = new_session()
    initialize_schema(db)
    print("Database schema initialized successfully.")


if __name__ == "__main__":
    main()