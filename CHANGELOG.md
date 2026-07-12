# Changelog

All notable changes are documented here. Releases use semantic versioning: `vMAJOR.MINOR.PATCH`.

- `MAJOR`: incompatible changes.
- `MINOR`: backwards-compatible features.
- `PATCH`: backwards-compatible fixes and small improvements.

Every release must update this file, `backend/global/version.go`, and the Dockerfile version argument before its matching Git tag is pushed.

## v0.21.2 - 2026-07-12

### Fixed

- The in-app update action now returns the updater setup error to the interface instead of the unhelpful `HTTP 400` message.
- Documented the one-time Compose upgrade required to start the Watchtower updater sidecar.

## v0.21.1 - 2026-07-12

### Changed

- Improved dashboard, gateway scheduling, health-check batching, key management, and settings interactions.
