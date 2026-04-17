"""
PULSE — Platform for Unified Live Signal Exploration.

Config-driven, AI-powered natural-language interface for Azure Data
Explorer (Kusto). The LLM writes narrow WHERE / SUMMARIZE clauses; the
engine owns clusters, schemas, metrics, dimensions, and auth via YAML.

See ``docs/ARCHITECTURE.md`` at the repository root for the system
walkthrough: request flow, caching layers, module map, and extension
points for new backends or clients.
"""
