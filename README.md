# Data Engineering MCP Server

A [Model Context Protocol](https://modelcontextprotocol.io) server that gives AI assistants (Claude, Ollama) live access to CSV datasets via DuckDB and a Snowflake data warehouse. Supports three deployment modes: local stdio, Docker, and Kubernetes (SSE transport).

---

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| Python 3.12+ | `python --version` |
| Snowflake account | Credentials in `.env` (see below) |
| Ollama | Optional — for local LLM chat |
| Docker | Optional — for containerised dev/prod |
| kubectl | Optional — for Kubernetes deployment |

---

## Environment variables

Copy and fill in your credentials:

```bash
cp .env.example .env   # then edit .env
```

| Variable | Description |
|----------|-------------|
| `SNOWFLAKE_ACCOUNT` | Account identifier (e.g. `abc123-xy12345`) |
| `SNOWFLAKE_USER` | Snowflake username |
| `SNOWFLAKE_PASSWORD` | Snowflake password |
| `SNOWFLAKE_WAREHOUSE` | Warehouse name (e.g. `COMPUTE_WH`) |
| `SNOWFLAKE_DATABASE` | Default database |
| `SNOWFLAKE_SCHEMA` | Default schema (e.g. `PUBLIC`) |
| `SNOWFLAKE_ROLE` | Snowflake role (e.g. `ACCOUNTADMIN`) |
| `MCP_TRANSPORT` | `stdio` (default) or `sse` (Kubernetes) |
| `PORT` | HTTP port when `MCP_TRANSPORT=sse` (default `8000`) |
| `OLLAMA_MODEL` | Default Ollama model (default `llama3.2`) |

---

## Quick start — Claude Code (MCP)

The server is registered in `~/.claude.json` under the project path. Claude Code spawns it automatically when you open the project.

**To register it manually** (one-time):

```bash
claude mcp add data-engineering python /Users/amber/Developer/data-mcp-server/server.py
```

**To restart after code changes** — open a new Claude Code conversation, or run `/mcp` in the chat to reconnect.

**Verify it's working** — ask Claude:
> "List the Snowflake tables" or "Load the sales dataset and show me a preview"

---

## Run locally (Python)

```bash
pip install -r requirements.txt
python server.py          # stdio mode — used by Claude Code
```

SSE mode (HTTP, needed for Ollama --sse or Kubernetes):

```bash
MCP_TRANSPORT=sse python server.py
# Listening on http://0.0.0.0:8000
```

---

## Run with Docker

```bash
# Build
docker build -t data-mcp-server .

# stdio (Claude Code)
docker run -i --rm --env-file .env data-mcp-server

# SSE (Ollama / remote)
docker run --rm --env-file .env -e MCP_TRANSPORT=sse -p 8000:8000 data-mcp-server
```

**docker-compose** (local dev, SSE mode):

```bash
docker-compose up
```

To use the Docker image with Claude Code, update the MCP entry in `~/.claude.json`:

```json
"data-engineering": {
  "type": "stdio",
  "command": "docker",
  "args": ["run", "-i", "--rm",
    "-e", "SNOWFLAKE_ACCOUNT", "-e", "SNOWFLAKE_USER", "-e", "SNOWFLAKE_PASSWORD",
    "-e", "SNOWFLAKE_WAREHOUSE", "-e", "SNOWFLAKE_DATABASE", "-e", "SNOWFLAKE_SCHEMA",
    "-e", "SNOWFLAKE_ROLE", "ghcr.io/<your-github-username>/data-mcp-server:latest"],
  "env": { "SNOWFLAKE_ACCOUNT": "...", "SNOWFLAKE_USER": "...", ... }
}
```

---

## Chat via Ollama

Run a local LLM that can query your data through the MCP tools.

```bash
# Install Ollama — https://ollama.com
ollama pull llama3.2

# stdio (server spawned automatically)
python ollama_client.py

# SSE (connect to running server)
MCP_TRANSPORT=sse python server.py &          # terminal 1
python ollama_client.py --sse http://localhost:8000/sse   # terminal 2

# Use a different model
python ollama_client.py --model mistral
OLLAMA_MODEL=llama3.2 python ollama_client.py
```

---

## Kubernetes deployment

### 1. Build and push the image

```bash
docker build -t ghcr.io/<your-github-username>/data-mcp-server:latest .
docker push ghcr.io/<your-github-username>/data-mcp-server:latest
```

### 2. Create the Snowflake secret

```bash
kubectl create secret generic snowflake-credentials \
  --from-literal=account="$SNOWFLAKE_ACCOUNT" \
  --from-literal=user="$SNOWFLAKE_USER" \
  --from-literal=password="$SNOWFLAKE_PASSWORD" \
  --from-literal=warehouse="$SNOWFLAKE_WAREHOUSE" \
  --from-literal=database="$SNOWFLAKE_DATABASE" \
  --from-literal=schema="$SNOWFLAKE_SCHEMA" \
  --from-literal=role="$SNOWFLAKE_ROLE"
```

### 3. Deploy

```bash
# Update the image tag in deployment.yaml first, then:
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml

kubectl rollout status deployment/data-mcp-server
```

### 4. Test via port-forward

```bash
kubectl port-forward svc/data-mcp-server 8000:80
python ollama_client.py --sse http://localhost:8000/sse
```

### 5. External access (optional)

Uncomment and configure `k8s/ingress.yaml`, then:

```bash
kubectl apply -f k8s/ingress.yaml
```

---

## GitHub CI/CD setup

### 1. Initialise the repo and push

```bash
cd /Users/amber/Developer/data-mcp-server
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/<your-username>/data-mcp-server.git
git push -u origin main
```

### 2. Set GitHub Secrets

Go to **Settings → Secrets and variables → Actions** in your GitHub repo and add:

| Secret | Value |
|--------|-------|
| `KUBE_CONFIG` | `cat ~/.kube/config \| base64` |
| `SNOWFLAKE_ACCOUNT` | e.g. `szsnrvm-ls09988` |
| `SNOWFLAKE_USER` | e.g. `AMBER99` |
| `SNOWFLAKE_PASSWORD` | your password |
| `SNOWFLAKE_WAREHOUSE` | e.g. `COMPUTE_WH` |
| `SNOWFLAKE_DATABASE` | e.g. `SEMANTIC_LAYER_DEV` |
| `SNOWFLAKE_SCHEMA` | e.g. `PUBLIC` |
| `SNOWFLAKE_ROLE` | e.g. `ACCOUNTADMIN` |

### 3. What runs automatically

| Event | Workflow | What it does |
|-------|----------|-------------|
| Push / PR (any branch) | `ci.yml` | Lint (ruff) + run all tests |
| Push to `main` | `release.yml` | Build Docker image → push to `ghcr.io` → `kubectl apply` |

### 4. Run tests locally

```bash
pip install -r requirements.txt
pytest
```

---

## Available MCP tools

| Tool | Description |
|------|-------------|
| `load_dataset` | Load a CSV file into memory and return a preview |
| `get_schema` | Column names, types, and null counts for a loaded dataset |
| `get_statistics` | Descriptive statistics (min/max/mean/std/quartiles) for numeric columns |
| `run_sql` | Run DuckDB SQL against a loaded dataset |
| `list_loaded_datasets` | List all datasets currently in memory |
| `snowflake_query` | Run arbitrary SQL against Snowflake |
| `snowflake_list_tables` | List tables in the connected Snowflake database/schema |
| `snowflake_describe_table` | Column names and types for a Snowflake table |
