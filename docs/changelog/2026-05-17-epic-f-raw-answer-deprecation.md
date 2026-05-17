# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Deprecated

- **`raw_answer` field in candidate API responses**  
  The `raw_answer` field is deprecated and will be removed after 2 releases.
  Consumers should migrate to using `normalized_answer` instead.
  A `logger.warning` is now emitted when `raw_answer` is accessed via the
  `/v1/runs/{run_id}/answers` endpoint to aid in detecting remaining consumers.
  See `council-candidate-detail-design.md` Epic F for details.