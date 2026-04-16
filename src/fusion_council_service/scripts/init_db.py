#!/usr/bin/env python3
"""Initialize the database schema."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from fusion_council_service.db import open_db_connection, initialize_schema


def main():
    db_path = os.environ.get("DATABASE_PATH", "./data/fusion_council.db")
    print(f"Initializing database at: {db_path}")
    db = open_db_connection(db_path)
    initialize_schema(db)
    print("Database schema initialized successfully.")


if __name__ == "__main__":
    main()