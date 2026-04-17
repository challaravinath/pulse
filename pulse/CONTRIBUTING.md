# Contributing to PULSE

Thanks for taking the time to look at PULSE. This document covers how to file
issues, how to submit patches, and what to expect from the review process.

This repository is currently published as a **showcase / reference**
implementation. Issues and PRs are welcome, but active development is driven
by the maintainers' own roadmap — please open an issue to discuss larger
changes before sending a PR.

---

## Reporting Issues

Before opening a new issue, please search existing issues to avoid duplicates.

A good bug report includes:

- What you were trying to do
- What you expected to happen
- What actually happened (error messages, stack traces, screenshots)
- Your environment: OS, Python version, how you're authenticating to Kusto
- A minimal YAML config that reproduces the problem, with any secrets removed

For feature requests, describe the use case first — what you want to achieve
— before proposing a specific implementation.

---

## Development Setup

```bash
# Clone and install
git clone https://github.com/<your-org>/pulse.git
cd pulse
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install -r requirements.txt

# Configure
cp .env.example .env
# Edit .env with your Azure OpenAI credentials

# Point at your own Kusto cluster
cp configs/example.yaml configs/my-source.yaml
# Edit configs/my-source.yaml with your cluster URL, database, table

# Run the Streamlit UI
streamlit run src/pulse/ui/app.py

# …or the FastAPI server
uvicorn pulse.api.app:app --reload --app-dir src
```

You will need your own Azure Data Explorer cluster. PULSE cannot run against
public data out of the box today.

---

## Pull Requests

- Branch from `main`. Keep PRs focused — one concern per PR.
- Match the style of surrounding code. This codebase is typed-ish Python;
  add type hints on new public functions.
- Keep the config-driven principle intact. If you find yourself hardcoding
  cluster URLs, table names, or metric definitions into Python, stop and
  push the value into YAML instead.
- Don't commit secrets. Run `git diff` before pushing; make sure no real
  cluster URLs, emails, or API keys leaked in.
- Update relevant docs (`README.md`, `docs/ARCHITECTURE.md`) when your
  change alters behavior or adds a new module.

---

## Coding Guidelines

- Python 3.10+.
- Prefer small, composable modules over large ones. The `src/pulse/core/`
  tree already leans this direction — one concern per file.
- LLM prompts belong near the code that uses them, not in a single giant
  prompts file.
- New LLM-assisted behavior must be constrained by config. The LLM should
  never be the one choosing clusters, tables, or metric definitions.
- Avoid adding new external services as hard dependencies without opening
  an issue first.

---

## Questions

Open a discussion or an issue. Please do not email the maintainers directly
about general questions — keeping conversations public helps other users
find answers later.
