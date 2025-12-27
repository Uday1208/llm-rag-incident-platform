# Alembic Database Migrations

This directory contains database migrations for the LLM-RAG Incident Platform.

## Setup

```bash
cd apps/rag-worker
pip install -r requirements.txt
```

## Environment Variables

Required for migration commands:
```bash
export PG_HOST=<your-pg-host>.postgres.database.azure.com
export PG_USER=<username>
export PG_PASS=<password>
export PG_DB=ragincdb
export PG_PORT=5432
export PG_SSLMODE=require
```

## Commands

### Check Current Version
```bash
alembic current
```

### Generate Migration Script (preview SQL)
```bash
alembic upgrade head --sql > migration.sql
```

### Apply Migrations
```bash
alembic upgrade head
```

### Rollback One Version
```bash
alembic downgrade -1
```

### Rollback All
```bash
alembic downgrade base
```

### Auto-generate from Model Changes
```bash
alembic revision --autogenerate -m "description"
```

## Migration Files

| Version | Description |
|---------|-------------|
| `001_initial` | Core tables: incident_bundles, resolutions, agent_sessions, embedding_cache, service_health |

## Working with Azure Postgres

If you don't have local psql, you can use Azure Cloud Shell:
1. Go to Azure Portal → your Postgres resource
2. Click "Cloud Shell" (top bar)
3. Run: `psql -h $PG_HOST -U $PG_USER -d $PG_DB`

Or use Azure Data Studio with the PostgreSQL extension.
