# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Removed

- **`raw_answer` field from candidate API responses and database schema**  
  The `raw_answer` field has been removed after 2 releases of deprecation.
  All consumers must use `normalized_answer` instead.
  The `raw_answer` column has been removed from the `run_candidates` table,
  and all code references have been updated to use `normalized_answer`.
  See `council-candidate-detail-design.md` Epic F for details.
