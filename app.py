"""CatalogIQ: a small, durable product-enrichment service."""
from __future__ import annotations

import concurrent.futures
import contextlib
import csv
import hashlib
import io
import json
import os
import random
import re
import sqlite3
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

ROOT = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get("CATALOGIQ_DB", ROOT / "catalogiq.db"))
PROVIDER = os.environ.get("LLM_PROVIDER", "mock").lower()
LLM_CONCURRENCY = max(1, int(os.environ.get("LLM_CONCURRENCY", "5")))
MOCK_LATENCY_MS = max(0, int(os.environ.get("MOCK_LATENCY_MS", "200")))
MOCK_FAILURE_RATE = min(1.0, max(0.0, float(os.environ.get("MOCK_FAILURE_RATE", "0.1"))))
CATEGORIES = ["Groceries", "Beverages", "Personal Care", "Household", "Electronics", "Fashion", "Home & Kitchen", "Other"]
PROMPT = '''Turn this raw marketplace listing into one JSON object with keys clean_title (tidy human-readable title), category (exactly one of Groceries, Beverages, Personal Care, Household, Electronics, Fashion, Home & Kitchen, Other), brand (string or null), and tags (up to five lowercase strings). Do not invent details. Return JSON only.\nTitle: {title}\nDescription: {description}'''

_db_lock = threading.RLock()
_metrics_lock = threading.Lock()
_metrics = {"llm_calls_total": 0, "llm_errors_total": 0, "max_concurrent_llm_calls": 0}
_active_calls = 0
_llm_slots = threading.BoundedSemaphore(LLM_CONCURRENCY)
_flight_lock = threading.Lock()
_inflight: dict[str, concurrent.futures.Future] = {}
_pool = concurrent.futures.ThreadPoolExecutor(max_workers=max(LLM_CONCURRENCY * 4, 16), thread_name_prefix="catalogiq")


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@contextlib.contextmanager
def connect():
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    try:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA foreign_keys=ON")
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def init_db() -> None:
    with _db_lock, connect() as db:
        db.executescript("""
        CREATE TABLE IF NOT EXISTS products (
          sku TEXT PRIMARY KEY, raw_title TEXT NOT NULL, raw_description TEXT NOT NULL DEFAULT '',
          clean_title TEXT, category TEXT, brand TEXT, tags TEXT NOT NULL DEFAULT '[]',
          status TEXT NOT NULL DEFAULT 'failed', error TEXT, content_key TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS products_category_sku ON products(category, sku);
        CREATE INDEX IF NOT EXISTS products_content_key ON products(content_key);
        CREATE TABLE IF NOT EXISTS cache (
          content_key TEXT PRIMARY KEY, clean_title TEXT NOT NULL, category TEXT NOT NULL,
          brand TEXT, tags TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS jobs (
          id TEXT PRIMARY KEY, status TEXT NOT NULL, total INTEGER NOT NULL,
          done INTEGER NOT NULL DEFAULT 0, failed INTEGER NOT NULL DEFAULT 0,
          cache_hits INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL,
          started_at TEXT, finished_at TEXT
        );
        """)


def normalize_content(title: str, description: str) -> str:
    return re.sub(r"\s+", " ", f"{title} {description}".lower())


def content_key(title: str, description: str) -> str:
    return hashlib.sha256(normalize_content(title, description).encode("utf-8")).hexdigest()


def product_dict(row: sqlite3.Row) -> dict[str, Any]:
    result = dict(row)
    result["tags"] = json.loads(result["tags"])
    result.pop("content_key", None)
    result.pop("updated_at", None)
    return result


def job_dict(row: sqlite3.Row) -> dict[str, Any]:
    return dict(row)


def _keyword_enrichment(title: str, description: str) -> dict[str, Any]:
    text = f"{title} {description}".lower()
    rules = [
        ("Beverages", ("drink", "juice", "water", "cola", "tea", "coffee", "beverage", "soda")),
        ("Personal Care", ("shampoo", "soap", "toothpaste", "lotion", "deodorant", "skin", "hair")),
        ("Household", ("detergent", "cleaner", "tissue", "floor", "dishwash", "laundry")),
        ("Electronics", ("phone", "charger", "laptop", "headphone", "cable", "earbud", "electronic")),
        ("Fashion", ("shirt", "jeans", "dress", "shoe", "jacket", "fashion", "cotton tee")),
        ("Home & Kitchen", ("pan", "mug", "bottle", "kitchen", "cookware", "storage", "towel")),
        ("Groceries", ("butter", "milk", "rice", "bread", "snack", "flour", "cheese", "grocery", "dal", "oil")),
    ]
    category = next((cat for cat, words in rules if any(word in text for word in words)), "Other")
    clean = re.sub(r"\s+", " ", title).strip(" .-_\t").title()
    clean = re.sub(r"\b(\d+)\s*(g|gm|grams?)\b", r"\1 g", clean, flags=re.I)
    brand = re.match(r"([A-Za-z][A-Za-z0-9&'-]*)", clean)
    tokens = re.findall(r"[a-z0-9]+", text)
    tags = list(dict.fromkeys(t for t in tokens if len(t) > 2))[:5]
    return {"clean_title": clean or "Untitled product", "category": category,
            "brand": brand.group(1) if brand else None, "tags": tags}


def _provider_call(title: str, description: str) -> Any:
    if PROVIDER == "mock":
        time.sleep(MOCK_LATENCY_MS / 1000)
        if random.random() < MOCK_FAILURE_RATE:
            raise RuntimeError("Mock LLM transient failure")
        return _keyword_enrichment(title, description)
    prompt = PROMPT.format(title=title, description=description)
    if PROVIDER == "ollama":
        endpoint = os.environ.get("OLLAMA_URL", "http://localhost:11434/api/generate")
        payload = {"model": os.environ.get("OLLAMA_MODEL", "llama3.2:3b"), "prompt": prompt, "stream": False, "format": "json"}
        request = urllib.request.Request(endpoint, data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=120) as response:
            body = json.loads(response.read())
        return json.loads(body["response"])
    if PROVIDER == "groq":
        key = os.environ.get("GROQ_API_KEY")
        if not key:
            raise RuntimeError("GROQ_API_KEY is required when LLM_PROVIDER=groq")
        payload = {"model": os.environ.get("GROQ_MODEL", "llama-3.1-8b-instant"), "messages": [{"role": "user", "content": prompt}], "response_format": {"type": "json_object"}, "temperature": 0}
        request = urllib.request.Request("https://api.groq.com/openai/v1/chat/completions", data=json.dumps(payload).encode(), headers={"Content-Type": "application/json", "Authorization": f"Bearer {key}"})
        with urllib.request.urlopen(request, timeout=120) as response:
            body = json.loads(response.read())
        return json.loads(body["choices"][0]["message"]["content"])
    raise RuntimeError(f"Unsupported LLM_PROVIDER: {PROVIDER}")


def _call_and_validate(title: str, description: str) -> dict[str, Any]:
    global _active_calls
    with _llm_slots:
        with _metrics_lock:
            _metrics["llm_calls_total"] += 1
            _active_calls += 1
            _metrics["max_concurrent_llm_calls"] = max(_metrics["max_concurrent_llm_calls"], _active_calls)
        try:
            raw = _provider_call(title, description)
            if isinstance(raw, str):
                raw = json.loads(raw)
            if not isinstance(raw, dict):
                raise ValueError("LLM output must be a JSON object")
            category, clean_title, tags, brand = raw.get("category"), raw.get("clean_title"), raw.get("tags"), raw.get("brand")
            if category not in CATEGORIES:
                raise ValueError("Invalid category in LLM output")
            if not isinstance(clean_title, str) or not clean_title.strip():
                raise ValueError("LLM output has no clean_title")
            if not isinstance(tags, list) or len(tags) > 5 or not all(isinstance(t, str) and t == t.lower() for t in tags):
                raise ValueError("LLM tags must be up to 5 lowercase strings")
            if brand is not None and not isinstance(brand, str):
                raise ValueError("LLM brand must be a string or null")
            return {"clean_title": clean_title.strip(), "category": category, "brand": brand,
                    "tags": tags}
        except Exception:
            with _metrics_lock:
                _metrics["llm_errors_total"] += 1
            raise
        finally:
            with _metrics_lock:
                _active_calls -= 1


def enrich(title: str, description: str) -> dict[str, Any]:
    key = content_key(title, description)
    with connect() as db:
        cached = db.execute("SELECT clean_title,category,brand,tags FROM cache WHERE content_key=?", (key,)).fetchone()
    if cached:
        return {**dict(cached), "cache_hit": True}
    owner = False
    with _flight_lock:
        future = _inflight.get(key)
        if future is None:
            future = concurrent.futures.Future()
            _inflight[key] = future
            owner = True
    if not owner:
        result = future.result()
        return {**result, "cache_hit": True}
    try:
        error = None
        for attempt in range(4):
            try:
                result = _call_and_validate(title, description)
                with _db_lock, connect() as db:
                    db.execute("INSERT OR REPLACE INTO cache(content_key,clean_title,category,brand,tags) VALUES(?,?,?,?,?)",
                               (key, result["clean_title"], result["category"], result["brand"], json.dumps(result["tags"])))
                future.set_result(result)
                return {**result, "cache_hit": False}
            except Exception as exc:
                error = str(exc)
                if attempt < 3:
                    time.sleep(0.2 * (2 ** attempt))
        raise RuntimeError(error or "Enrichment failed after 4 attempts")
    except Exception as exc:
        future.set_exception(exc)
        # Avoid an unobserved Future exception when there were no waiters.
        try:
            future.exception()
        except Exception:
            pass
        raise
    finally:
        with _flight_lock:
            _inflight.pop(key, None)


def _process_product(job_id: str, product: dict[str, str]) -> None:
    sku, title, description = product["sku"], product["raw_title"], product.get("raw_description", "")
    key = content_key(title, description)
    failed = False
    hit = False
    error = None
    try:
        data = enrich(title, description)
        hit = data.pop("cache_hit")
        with _db_lock, connect() as db:
            db.execute("UPDATE products SET clean_title=?,category=?,brand=?,tags=?,status='enriched',error=NULL,updated_at=? WHERE sku=?",
                       (data["clean_title"], data["category"], data["brand"], json.dumps(data["tags"]), now(), sku))
    except Exception as exc:
        failed, error = True, str(exc)
        with _db_lock, connect() as db:
            db.execute("UPDATE products SET status='failed',error=?,updated_at=? WHERE sku=?", (error, now(), sku))
    finally:
        with _db_lock, connect() as db:
            db.execute("UPDATE jobs SET done=done+1,failed=failed+?,cache_hits=cache_hits+? WHERE id=?",
                       (int(failed), int(hit), job_id))
            job = db.execute("SELECT done,total FROM jobs WHERE id=?", (job_id,)).fetchone()
            if job and job["done"] >= job["total"]:
                db.execute("UPDATE jobs SET status='completed',finished_at=? WHERE id=?", (now(), job_id))


def submit_job(products: list[dict[str, str]]) -> dict[str, Any]:
    job_id = "j_" + uuid.uuid4().hex[:12]
    created = now()
    with _db_lock, connect() as db:
        db.execute("INSERT INTO jobs(id,status,total,created_at) VALUES(?,'queued',?,?)", (job_id, len(products), created))
        for p in products:
            key = content_key(p["raw_title"], p.get("raw_description", ""))
            db.execute("INSERT INTO products(sku,raw_title,raw_description,status,error,content_key,updated_at) VALUES(?,?,?,'failed',NULL,?,?) "
                       "ON CONFLICT(sku) DO UPDATE SET raw_title=excluded.raw_title,raw_description=excluded.raw_description,status='failed',error=NULL,content_key=excluded.content_key,updated_at=excluded.updated_at",
                       (p["sku"], p["raw_title"], p.get("raw_description", ""), key, created))
        db.execute("UPDATE jobs SET status='running',started_at=? WHERE id=?", (now(), job_id))
    for p in products:
        _pool.submit(_process_product, job_id, p)
    return {"id": job_id, "status": "running", "total": len(products), "done": 0, "failed": 0,
            "cache_hits": 0, "created_at": created, "started_at": now(), "finished_at": None}


def respond(handler: BaseHTTPRequestHandler, code: int, body: Any) -> None:
    raw = json.dumps(body, ensure_ascii=False).encode("utf-8")
    handler.send_response(code)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(raw)))
    handler.send_header("Access-Control-Allow-Origin", "*")
    handler.end_headers()
    handler.wfile.write(raw)


class Handler(BaseHTTPRequestHandler):
    server_version = "CatalogIQ/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[{self.log_date_time_string()}] {fmt % args}")

    def body(self) -> Any:
        length = int(self.headers.get("Content-Length", "0"))
        return json.loads(self.rfile.read(length) or b"{}")

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,PATCH,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self) -> None:
        path = urlparse(self.path)
        qs = parse_qs(path.query)
        if path.path == "/":
            raw = (ROOT / "index.html").read_bytes()
            self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(raw))); self.end_headers(); self.wfile.write(raw); return
        if path.path == "/api/health":
            respond(self, 200, {"status": "ok", "llm_provider": PROVIDER, "llm_concurrency": LLM_CONCURRENCY}); return
        if path.path == "/api/metrics":
            with _metrics_lock: metrics = dict(_metrics)
            respond(self, 200, metrics); return
        if path.path.startswith("/api/jobs/"):
            job_id = unquote(path.path.rsplit("/", 1)[1])
            with connect() as db: row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            respond(self, 200, job_dict(row)) if row else respond(self, 404, {"error": "Unknown job"}); return
        if path.path == "/api/products":
            try:
                page = int(qs.get("page", ["1"])[0]); size = int(qs.get("page_size", ["20"])[0])
                if page < 1 or size < 1 or size > 100: raise ValueError()
            except ValueError:
                respond(self, 400, {"error": "page and page_size must be positive integers; page_size maximum is 100"}); return
            clauses, args = [], []
            category = qs.get("category", [""])[0]
            query = qs.get("q", [""])[0].strip()
            if category: clauses.append("category=?"); args.append(category)
            if query:
                clauses.append("(clean_title LIKE ? COLLATE NOCASE OR raw_title LIKE ? COLLATE NOCASE)")
                args.extend([f"%{query}%", f"%{query}%"])
            where = " WHERE " + " AND ".join(clauses) if clauses else ""
            with connect() as db:
                total = db.execute("SELECT COUNT(*) FROM products" + where, args).fetchone()[0]
                rows = db.execute("SELECT * FROM products" + where + " ORDER BY sku LIMIT ? OFFSET ?", args + [size, (page-1)*size]).fetchall()
            respond(self, 200, {"items": [product_dict(r) for r in rows], "page": page, "page_size": size, "total": total}); return
        if path.path.startswith("/api/products/"):
            sku = unquote(path.path.rsplit("/", 1)[1])
            with connect() as db: row = db.execute("SELECT * FROM products WHERE sku=?", (sku,)).fetchone()
            respond(self, 200, product_dict(row)) if row else respond(self, 404, {"error": "Unknown SKU"}); return
        respond(self, 404, {"error": "Not found"})

    def do_POST(self) -> None:
        if self.path != "/api/jobs":
            respond(self, 404, {"error": "Not found"}); return
        try:
            payload = self.body(); products = payload.get("products")
            if not isinstance(products, list) or not products:
                respond(self, 400, {"error": "products must be a non-empty list"}); return
            cleaned = []
            for i, product in enumerate(products):
                if not isinstance(product, dict) or not str(product.get("sku", "")).strip() or not str(product.get("raw_title", "")).strip():
                    respond(self, 400, {"error": f"product at index {i} requires sku and raw_title"}); return
                cleaned.append({"sku": str(product["sku"]).strip(), "raw_title": str(product["raw_title"]), "raw_description": str(product.get("raw_description") or "")})
            respond(self, 202, submit_job(cleaned))
        except (ValueError, TypeError, AttributeError) as exc:
            respond(self, 400, {"error": str(exc) or "Invalid JSON request"})

    def do_PATCH(self) -> None:
        if not self.path.startswith("/api/products/"):
            respond(self, 404, {"error": "Not found"}); return
        sku = unquote(urlparse(self.path).path.rsplit("/", 1)[1])
        try: payload = self.body()
        except Exception as exc: respond(self, 400, {"error": f"Invalid JSON: {exc}"}); return
        allowed = {"clean_title", "category", "tags"}
        if not isinstance(payload, dict) or not payload or set(payload) - allowed:
            respond(self, 400, {"error": "Provide clean_title, category, or tags"}); return
        if "clean_title" in payload and (not isinstance(payload["clean_title"], str) or not payload["clean_title"].strip()):
            respond(self, 400, {"error": "clean_title cannot be empty"}); return
        if "category" in payload and payload["category"] not in CATEGORIES:
            respond(self, 400, {"error": "Invalid category"}); return
        if "tags" in payload and (not isinstance(payload["tags"], list) or len(payload["tags"]) > 5 or not all(isinstance(t, str) and t == t.lower() for t in payload["tags"])):
            respond(self, 400, {"error": "tags must be up to 5 lowercase strings"}); return
        assignments, values = [], []
        for key in ("clean_title", "category", "tags"):
            if key in payload:
                assignments.append(f"{key}=?"); values.append(json.dumps(payload[key]) if key == "tags" else payload[key].strip() if key == "clean_title" else payload[key])
        values.append(sku)
        with _db_lock, connect() as db:
            cur = db.execute(f"UPDATE products SET {','.join(assignments)},status='approved',error=NULL,updated_at=? WHERE sku=?", [*values[:-1], now(), sku])
            row = db.execute("SELECT * FROM products WHERE sku=?", (sku,)).fetchone()
        if not row: respond(self, 404, {"error": "Unknown SKU"})
        else: respond(self, 200, product_dict(row))


def main() -> None:
    init_db()
    host, port = os.environ.get("HOST", "127.0.0.1"), int(os.environ.get("PORT", "8000"))
    print(f"CatalogIQ listening on http://{host}:{port} (provider={PROVIDER}, concurrency={LLM_CONCURRENCY})")
    ThreadingHTTPServer((host, port), Handler).serve_forever()


if __name__ == "__main__":
    main()
