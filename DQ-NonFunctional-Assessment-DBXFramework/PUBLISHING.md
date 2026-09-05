# Publishing to PyPI

Package name: **`databricks-dq-framework`**
PyPI page: https://pypi.org/project/databricks-dq-framework

---

## Prerequisites (one-time setup)

1. Install build tools:
   ```bash
   pip install build twine
   ```

2. Create an account at https://pypi.org

3. Generate an API token:
   - PyPI → Account Settings → API tokens → Add API token
   - Scope: "Entire account" for first upload; "Project: databricks-dq-framework" for subsequent uploads
   - Copy the token (starts with `pypi-`) — it is shown only once

---

## Publishing a new version

### Step 1 — Bump the version in `pyproject.toml`

```toml
[project]
version = "1.1.0"   # increment this
```

Follow semantic versioning:
- **Patch** `1.0.x` — bug fixes
- **Minor** `1.x.0` — new features, backwards compatible
- **Major** `x.0.0` — breaking changes

### Step 2 — Build

Run from inside this folder (`DQ-NonFunctional-Assessment-DBXFramework/`):

```bash
python -m build
```

This creates two files in `dist/`:
- `databricks_dq_framework-1.x.x-py3-none-any.whl` — wheel (preferred by pip)
- `databricks_dq_framework-1.x.x.tar.gz` — source distribution

### Step 3 — Upload

```bash
python -m twine upload dist/*
```

When prompted:
- **Username:** `__token__`
- **Password:** paste your `pypi-...` API token

### Step 4 — Clean up build artifacts

```bash
cd "...path..."
# Windows
rmdir /s /q dist
rmdir /s /q build
rmdir /s /q dq_framework.egg-info

# Mac / Linux
rm -rf dist/ build/ dq_framework.egg-info/
```

### Step 5 — Commit and tag the release

```bash
git add pyproject.toml
git commit -m "Release v1.x.x"
git tag v1.x.x
git push && git push --tags
```

---

## Testing before publishing (recommended)

Upload to TestPyPI first to verify everything looks correct:

```bash
python -m twine upload --repository testpypi dist/*
```

Install from TestPyPI to verify:
```bash
pip install --index-url https://test.pypi.org/simple/ databricks-dq-framework
```

---

## User installation

```python
# In a Databricks notebook
%pip install databricks-dq-framework
```

```python
from dq_framework import DQFramework

dq = DQFramework(spark, catalog="your_catalog", schema="dq")
dq.setup()
dq.generate_rule_functions()
exec_id = dq.run_assessment(schema_name="Curated")
```

---

## Notes

- `dq-framework` is rejected by PyPI as too similar to an existing package — always use `databricks-dq-framework`
- PyPI does not allow re-uploading the same version number — always bump the version before rebuilding
- The `dist/` and `build/` folders are excluded from git via `.gitignore`
