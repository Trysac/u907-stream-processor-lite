# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Added event-time stream processor (`Tumbling`, `Sliding`, `Session`, `Pipeline`, `JsonlSource`, `batch_compute`, `read_output`, `StreamError`, and CLI) with watermarks, late-data side outputs, and crash-safe atomic checkpointing.
- Initial project structure from the factory utility template.
