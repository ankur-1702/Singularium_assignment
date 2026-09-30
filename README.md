# CatalogIQ

A small product-enrichment pipeline with a plain HTML/JS review UI, SQLite persistence, background thread workers, bounded LLM calls, retries, and content-level caching.

## Requirements and setup

Python 3.10+; no third-party packages are needed.

```sh
python3 app.py
```

Open [http://localhost:8000](http://localhost:8000). The default mock provider works offline. The SQLite database is created as `catalogiq.db` in the project directory; set `CATALOGIQ_DB` to choose a different location.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `LLM_PROVIDER` | `mock` | `mock`, `ollama`, or `groq` |
| `LLM_CONCURRENCY` | `5` | Maximum simultaneous provider calls across jobs |
| `MOCK_LATENCY_MS` | `200` | Mock call delay |
| `MOCK_FAILURE_RATE` | `0.1` | Mock transient-failure probability, from 0 to 1 |
| `OLLAMA_URL` | `http://localhost:11434/api/generate` | Ollama endpoint |
| `OLLAMA_MODEL` | `llama3.2:3b` | Local model name |
| `GROQ_API_KEY` | unset | Groq key, read from the environment only |
| `GROQ_MODEL` | `llama-3.1-8b-instant` | Groq model name |

For local inference, start Ollama and pull a compatible model, then run `LLM_PROVIDER=ollama python3 app.py`. For Groq, set `GROQ_API_KEY` in your shell and run `LLM_PROVIDER=groq python3 app.py`. Keys are not stored in the repository. Provider free-tier terms and model availability can change; verify them before using the service. The built-in mock needs no account or network.

## API

- `GET /api/health`, `GET /api/metrics`
- `POST /api/jobs` with `{"products":[{"sku":"A1","raw_title":"...","raw_description":"..."}]}`
- `GET /api/jobs/{job_id}`
- `GET /api/products?page=1&page_size=20&category=Groceries&q=butter`
- `GET /api/products/{sku}`
- `PATCH /api/products/{sku}` with any of `clean_title`, `category`, `tags`

All error responses have an `error` field. CSV uploads need `sku` and `raw_title` headers; `raw_description` is optional. A repeated SKU updates its current catalogue row.

## Tests

```sh
python3 -m unittest discover -s tests -v
```

The tests exercise the global call cap, four-attempt retry behavior, and same-time content de-duplication using patched local provider behavior.

## Prompt

The real-provider prompt is defined by `PROMPT` in `app.py`: return JSON only with a tidy `clean_title`, one allowed category, `brand` as a string or null, and up to five lowercase tags; do not invent details. The mock shares the same validation boundary and uses simple keyword rules.

## Assumptions and unfinished work

- The in-memory queue is suitable for this single-process assignment demo; a server crash can interrupt an active job. `DESIGN.md` describes durable queue recovery.
- Groq and Ollama are supported as optional examples; the default interview path is mock. No provider SDK is required.
- Product search uses SQLite `LIKE`; at large scale it should move to FTS and keyset pagination.
- Mock output is deterministic except for configured transient failures and does not measure model confidence.
- Authentication, CSV size limits, production rate limiting, and multi-process distributed coordination are out of scope.

## AI tool use

ChatGPT was used as a coding assistant during development for initial scaffolding, implementation suggestions, generating test-case, and documentation.
