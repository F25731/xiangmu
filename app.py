from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup, Tag
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field


APP_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("APP_DB_PATH", APP_DIR / "data" / "resource_service.db"))
ADMIN_TOKEN = os.getenv("APP_ADMIN_TOKEN", "").strip()
PAN_HOSTS = ("pan.quark.cn", "pan.baidu.com")


DEFAULT_CONFIG: dict[str, Any] = {
    "source_url": "https://xiangmu.eu.cc/",
    "enabled": True,
    "auto_transfer": True,
    "first_run_limit": 20,
    "scan_limit": 80,
    "fetch_limit": 20,
    "fetch_interval_seconds": 1800,
    "request_delay_seconds": 2.0,
    "user_agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "delete_names": [
        "网创平台入口",
        "免责声明",
        "项目工具资料",
        "众包任务悬赏平台",
        "返佣任务平台1-入口",
        "返佣任务平台2-操作教程",
        "领取AI创作工具",
    ],
    "quark_cookie": "",
    "quark_save_fid": "0",
    "quark_script_path": "/app/scripts/quark_xinyue_test.py",
    "baidu_cookie": "",
    "baidu_new_share_code": "1111",
    "baidu_transfer_dir": "/测试",
    "openlist_url": "http://127.0.0.1:5244/",
    "openlist_token": "",
    "openlist_auth_prefix": "",
    "openlist_root_dir": "/百度网盘",
    "baidu_script_path": "/app/scripts/baidu_openlist_test.py",
    "runner_timeout_seconds": 600,
}


class ConfigIn(BaseModel):
    source_url: Optional[str] = None
    first_run_limit: Optional[int] = Field(default=None, ge=1, le=500)
    scan_limit: Optional[int] = Field(default=None, ge=1, le=500)
    fetch_interval_seconds: Optional[int] = Field(default=None, ge=60, le=86400)
    request_delay_seconds: Optional[float] = Field(default=None, ge=0.5, le=60)
    user_agent: Optional[str] = None
    delete_names: Optional[List[str]] = None
    quark_cookie: Optional[str] = None
    quark_save_fid: Optional[str] = None
    quark_script_path: Optional[str] = None
    baidu_cookie: Optional[str] = None
    baidu_new_share_code: Optional[str] = None
    baidu_transfer_dir: Optional[str] = None
    openlist_url: Optional[str] = None
    openlist_token: Optional[str] = None
    openlist_auth_prefix: Optional[str] = None
    openlist_root_dir: Optional[str] = None
    baidu_script_path: Optional[str] = None
    runner_timeout_seconds: Optional[int] = Field(default=None, ge=60, le=7200)


class RunIn(BaseModel):
    limit: Optional[int] = Field(default=None, ge=1, le=500)
    retry_failures: Optional[bool] = True


class TransferIn(BaseModel):
    providers: Optional[List[str]] = None


class AckIn(BaseModel):
    ids: List[int] = Field(default_factory=list)


runtime_status: dict[str, Any] = {
    "running": True,
    "busy": False,
    "last_started_at": "",
    "last_finished_at": "",
    "last_error": "",
    "last_inserted": 0,
    "last_seen": 0,
    "last_transferred": 0,
    "last_failed": 0,
    "next_run_at": "",
}
scheduler_task: Optional[asyncio.Task] = None
fetch_lock = asyncio.Lock()


app = FastAPI(title="Resource Transfer Service")


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


@contextmanager
def db() -> Any:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    columns = {row["name"] for row in conn.execute(f"pragma table_info({table})").fetchall()}
    if column not in columns:
        conn.execute(f"alter table {table} add column {column} {ddl}")


def migrate_transfer_failures(conn: sqlite3.Connection) -> None:
    indexes = conn.execute("pragma index_list(transfer_failures)").fetchall()
    has_source_only_unique = False
    for index in indexes:
        if not index["unique"]:
            continue
        columns = [
            row["name"]
            for row in conn.execute(f"pragma index_info({index['name']})").fetchall()
        ]
        if columns == ["source_key"]:
            has_source_only_unique = True
            break
    if not has_source_only_unique:
        return

    rows = conn.execute("select * from transfer_failures").fetchall()
    conn.execute("alter table transfer_failures rename to transfer_failures_old")
    conn.execute(
        """
        create table transfer_failures (
            id integer primary key autoincrement,
            source_key text not null,
            provider text not null default '',
            title text not null,
            item_json text not null,
            errors text not null default '[]',
            source_rank integer not null default 999999,
            retry_count integer not null default 0,
            last_error text not null default '',
            created_at text not null,
            updated_at text not null,
            unique(source_key, provider)
        )
        """
    )
    for row in rows:
        provider = row["provider"] if "provider" in row.keys() and row["provider"] else "unknown"
        conn.execute(
            """
            insert or ignore into transfer_failures(
                source_key, provider, title, item_json, errors, source_rank,
                retry_count, last_error, created_at, updated_at
            ) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["source_key"],
                provider,
                row["title"],
                row["item_json"],
                row["errors"],
                row["source_rank"],
                row["retry_count"],
                row["last_error"],
                row["created_at"],
                row["updated_at"],
            ),
        )
    conn.execute("drop table transfer_failures_old")


def init_db() -> None:
    with db() as conn:
        conn.execute(
            """
            create table if not exists config (
                key text primary key,
                value text not null
            )
            """
        )
        conn.execute(
            """
            create table if not exists resources (
                id integer primary key autoincrement,
                source_key text not null unique,
                source_url text not null,
                title text not null,
                intro text not null,
                image_url text not null,
                provider_links text not null,
                new_links text not null default '{}',
                raw text not null default '{}',
                source_rank integer not null default 999999,
                push_count integer not null default 0,
                transferred_at text not null default '',
                created_at text not null,
                updated_at text not null
            )
            """
        )
        conn.execute(
            """
            create table if not exists runs (
                id integer primary key autoincrement,
                started_at text not null,
                finished_at text not null default '',
                ok integer not null default 0,
                seen integer not null default 0,
                inserted integer not null default 0,
                transferred integer not null default 0,
                error text not null default ''
            )
            """
        )
        conn.execute(
            """
            create table if not exists events (
                id integer primary key autoincrement,
                created_at text not null,
                level text not null,
                message text not null,
                context text not null default '{}'
            )
            """
        )
        conn.execute(
            """
            create table if not exists transfer_failures (
                id integer primary key autoincrement,
                source_key text not null,
                provider text not null default '',
                title text not null,
                item_json text not null,
                errors text not null default '[]',
                source_rank integer not null default 999999,
                retry_count integer not null default 0,
                last_error text not null default '',
                created_at text not null,
                updated_at text not null,
                unique(source_key, provider)
            )
            """
        )
        ensure_column(conn, "transfer_failures", "provider", "text not null default ''")
        migrate_transfer_failures(conn)
        conn.execute(
            """
            create table if not exists delivery_marks (
                id integer primary key autoincrement,
                client_id text not null,
                resource_id integer not null,
                delivered_at text not null,
                unique(client_id, resource_id)
            )
            """
        )
        ensure_column(conn, "resources", "source_rank", "integer not null default 999999")
        ensure_column(conn, "resources", "push_count", "integer not null default 0")
        existing = {
            row["key"]
            for row in conn.execute("select key from config").fetchall()
        }
        for key, value in DEFAULT_CONFIG.items():
            if key not in existing:
                conn.execute(
                    "insert into config(key, value) values(?, ?)",
                    (key, json.dumps(value, ensure_ascii=False)),
                )


def load_config() -> dict[str, Any]:
    init_db()
    config = dict(DEFAULT_CONFIG)
    with db() as conn:
        rows = conn.execute("select key, value from config").fetchall()
    for row in rows:
        try:
            config[row["key"]] = json.loads(row["value"])
        except json.JSONDecodeError:
            config[row["key"]] = row["value"]
    return config


def save_config(patch: dict[str, Any]) -> dict[str, Any]:
    current = load_config()
    for key, value in patch.items():
        if value is not None and key in DEFAULT_CONFIG:
            current[key] = value
    current["enabled"] = True
    current["auto_transfer"] = True
    with db() as conn:
        for key, value in current.items():
            conn.execute(
                """
                insert into config(key, value) values(?, ?)
                on conflict(key) do update set value = excluded.value
                """,
                (key, json.dumps(value, ensure_ascii=False)),
            )
    runtime_status["running"] = True
    return current


def log_event(level: str, message: str, context: Optional[dict[str, Any]] = None) -> None:
    stamp = now_iso()
    with db() as conn:
        conn.execute(
            """
            insert into events(created_at, level, message, context)
            values(?, ?, ?, ?)
            """,
            (
                stamp,
                level,
                message,
                json.dumps(context or {}, ensure_ascii=False),
            ),
        )
    if level.lower() in {"error", "warning"}:
        runtime_status["last_error"] = message


async def require_admin(
    request: Request,
    x_admin_token: Optional[str] = Header(default=None),
) -> None:
    if not ADMIN_TOKEN:
        return
    token = x_admin_token or request.query_params.get("token", "")
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="invalid admin token")


def row_to_resource(row: sqlite3.Row) -> dict[str, Any]:
    item = dict(row)
    for key in ("provider_links", "new_links", "raw"):
        try:
            item[key] = json.loads(item[key])
        except (TypeError, json.JSONDecodeError):
            item[key] = {} if key != "provider_links" else []
    return item


def public_resource(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": item["id"],
        "title": item["title"],
        "intro": item["intro"],
        "image_url": item["image_url"],
        "links": item.get("new_links") or {},
        "new_links": item.get("new_links") or {},
        "transferred_at": item.get("transferred_at", ""),
        "created_at": item.get("created_at", ""),
        "updated_at": item.get("updated_at", ""),
    }


def mark_delivered(client_id: str, resource_ids: list[int]) -> None:
    if not client_id or not resource_ids:
        return
    stamp = now_iso()
    with db() as conn:
        for resource_id in resource_ids:
            conn.execute(
                """
                insert or ignore into delivery_marks(client_id, resource_id, delivered_at)
                values(?, ?, ?)
                """,
                (client_id, int(resource_id), stamp),
            )
        conn.executemany(
            "update resources set push_count = push_count + 1 where id = ?",
            [(int(resource_id),) for resource_id in resource_ids],
        )


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def provider_for_url(url: str) -> str:
    host = urlparse(url).netloc.lower()
    if "pan.quark.cn" in host:
        return "quark"
    if "pan.baidu.com" in host:
        return "baidu"
    return ""


def link_code(url: str, text: str) -> str:
    query = parse_qs(urlparse(url).query)
    for key in ("pwd", "passcode"):
        if query.get(key):
            return str(query[key][0]).strip()
    match = re.search(r"(?:提取码|访问码|密码)[:：\s]*([A-Za-z0-9]{4,8})", text)
    return match.group(1) if match else ""


def absolute_url(base_url: str, value: str) -> str:
    return urljoin(base_url, value.strip()) if value else ""


def best_container(anchor: Tag) -> Tag:
    current: Tag = anchor
    fallback = anchor
    for _ in range(7):
        parent = current.parent
        if not isinstance(parent, Tag):
            break
        text = clean_text(parent.get_text(" ", strip=True))
        pan_count = len(
            [
                a
                for a in parent.find_all("a", href=True)
                if provider_for_url(str(a.get("href") or ""))
            ]
        )
        if 1 <= pan_count <= 6:
            fallback = parent
            if 50 <= len(text) <= 6000:
                return parent
        current = parent
    return fallback


def extract_title(container: Tag) -> str:
    all_text = clean_text(container.get_text(" ", strip=True))
    quoted = re.search(r"《([^》]{6,180})》", all_text)
    if quoted:
        return f"《{quoted.group(1)}》"
    for selector in ("h1", "h2", "h3", "h4", ".title"):
        found = container.select_one(selector)
        if found:
            title = clean_text(found.get_text(" ", strip=True))
            if title:
                return title
    for line in container.get_text("\n", strip=True).splitlines():
        line = clean_text(line)
        if len(line) >= 6 and not provider_for_url(line) and "网盘" not in line:
            return line[:180]
    return "未命名资源"


def extract_intro(container: Tag, title: str) -> str:
    lines: list[str] = []
    after_intro = False
    for line in container.get_text("\n", strip=True).splitlines():
        text = clean_text(line)
        if not text or text == title:
            continue
        if "项目介绍" in text or "简介" in text:
            after_intro = True
            text = re.sub(r"^.*?(项目介绍|简介)[:：\s]*", "", text).strip()
        if any(word in text for word in ("夸克网盘", "百度网盘", "UC网盘", "迅雷网盘")):
            continue
        if "pan.quark.cn" in text or "pan.baidu.com" in text:
            continue
        if after_intro or len(text) > 18:
            lines.append(text)
    intro = " ".join(lines)
    return intro[:1000]


def parse_resources(html: str, base_url: str, limit: int) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    anchors = [
        a
        for a in soup.find_all("a", href=True)
        if provider_for_url(absolute_url(base_url, str(a.get("href") or "")))
    ]
    grouped: dict[int, dict[str, Any]] = {}
    for anchor in anchors:
        container = best_container(anchor)
        key = id(container)
        grouped.setdefault(key, {"container": container, "links": []})
        grouped[key]["links"].append(anchor)

    items: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for group in grouped.values():
        container: Tag = group["container"]
        title = extract_title(container)
        text = clean_text(container.get_text(" ", strip=True))
        image_tag = container.find("img")
        image_url = ""
        if isinstance(image_tag, Tag):
            image_url = absolute_url(
                base_url,
                str(image_tag.get("src") or image_tag.get("data-src") or ""),
            )
        links = []
        seen_urls: set[str] = set()
        for anchor in group["links"]:
            url = absolute_url(base_url, str(anchor.get("href") or ""))
            provider = provider_for_url(url)
            if not provider or url in seen_urls:
                continue
            seen_urls.add(url)
            links.append(
                {
                    "provider": provider,
                    "url": url,
                    "code": link_code(url, text),
                    "label": clean_text(anchor.get_text(" ", strip=True)),
                }
            )
        if not links:
            continue
        source_key = hashlib.sha256(
            json.dumps([title, sorted(seen_urls)], ensure_ascii=False).encode("utf-8")
        ).hexdigest()
        if source_key in seen_keys:
            continue
        seen_keys.add(source_key)
        items.append(
            {
                "source_key": source_key,
                "source_url": base_url,
                "source_rank": len(items) + 1,
                "title": title,
                "intro": extract_intro(container, title),
                "image_url": image_url,
                "provider_links": links,
                "raw": {"text": text[:3000]},
            }
        )
        if len(items) >= limit:
            break
    return items


async def fetch_source(config: dict[str, Any], limit: int) -> list[dict[str, Any]]:
    headers = {"User-Agent": str(config.get("user_agent") or DEFAULT_CONFIG["user_agent"])}
    async with httpx.AsyncClient(
        timeout=30,
        follow_redirects=True,
        headers=headers,
        trust_env=False,
    ) as client:
        response = await client.get(str(config["source_url"]))
        response.raise_for_status()
    html = response.content.decode("utf-8", errors="replace")
    return parse_resources(html, str(response.url), limit)


def upsert_resources(items: list[dict[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    inserted = 0
    changed: list[dict[str, Any]] = []
    stamp = now_iso()
    with db() as conn:
        for item in items:
            before = conn.execute(
                "select id from resources where source_key = ?",
                (item["source_key"],),
            ).fetchone()
            conn.execute(
                """
                insert into resources(
                    source_key, source_url, title, intro, image_url,
                    provider_links, new_links, raw, created_at, updated_at
                ) values(?, ?, ?, ?, ?, ?, '{}', ?, ?, ?)
                on conflict(source_key) do update set
                    title = excluded.title,
                    intro = excluded.intro,
                    image_url = excluded.image_url,
                    provider_links = excluded.provider_links,
                    raw = excluded.raw,
                    updated_at = excluded.updated_at
                """,
                (
                    item["source_key"],
                    item["source_url"],
                    item["title"],
                    item["intro"],
                    item["image_url"],
                    json.dumps(item["provider_links"], ensure_ascii=False),
                    json.dumps(item["raw"], ensure_ascii=False),
                    stamp,
                    stamp,
                ),
            )
            row = conn.execute(
                "select * from resources where source_key = ?",
                (item["source_key"],),
            ).fetchone()
            if before is None:
                inserted += 1
                changed.append(row_to_resource(row))
    return inserted, changed


def script_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = APP_DIR / value
    return path


def run_json_command(args: list[str], timeout: int) -> dict[str, Any]:
    proc = subprocess.run(
        args,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
    )
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout).strip()
        raise RuntimeError(detail or f"command failed with exit code {proc.returncode}")
    output = proc.stdout.strip()
    try:
        return json.loads(output)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"runner returned non-json: {output[:500]}") from exc


def transfer_link(link: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    provider = link.get("provider")
    timeout = int(config.get("runner_timeout_seconds") or 600)
    delete_names = [name for name in config.get("delete_names", []) if str(name).strip()]

    if provider == "quark":
        path = script_path(str(config.get("quark_script_path") or ""))
        if not path.exists():
            raise RuntimeError(f"Quark script not found: {path}")
        args = [
            sys.executable,
            str(path),
            "--share-url",
            str(link["url"]),
            "--quark-cookie",
            str(config.get("quark_cookie") or ""),
            "--save-fid",
            str(config.get("quark_save_fid") or "0"),
            "--create-share",
        ]
        if link.get("code"):
            args += ["--share-code", str(link["code"])]
        for name in delete_names:
            args += ["--delete-name", name]
        result = run_json_command(args, timeout)
        new_url = str(result.get("new_share_url") or "")
        if not new_url:
            raise RuntimeError("Quark runner did not return new_share_url")
        return {
            "provider": "quark",
            "url": new_url,
            "code": str(result.get("new_share_code") or ""),
            "raw": result,
        }

    if provider == "baidu":
        path = script_path(str(config.get("baidu_script_path") or ""))
        if not path.exists():
            raise RuntimeError(f"Baidu script not found: {path}")
        args = [
            sys.executable,
            str(path),
            "--share-url",
            str(link["url"]),
            "--baidu-cookie",
            str(config.get("baidu_cookie") or ""),
            "--openlist-url",
            str(config.get("openlist_url") or ""),
            "--openlist-token",
            str(config.get("openlist_token") or ""),
            "--openlist-auth-prefix",
            str(config.get("openlist_auth_prefix") or ""),
            "--transfer-dir",
            str(config.get("baidu_transfer_dir") or "/测试"),
            "--openlist-root-dir",
            str(config.get("openlist_root_dir") or "/百度网盘"),
            "--new-share-code",
            str(config.get("baidu_new_share_code") or "1111"),
            "--create-share",
        ]
        if link.get("code"):
            args += ["--share-code", str(link["code"])]
        for name in delete_names:
            args += ["--delete-name", name]
        result = run_json_command(args, timeout)
        new_url = str(result.get("new_share_url") or "")
        if not new_url:
            raise RuntimeError("Baidu runner did not return new_share_url")
        return {
            "provider": "baidu",
            "url": new_url,
            "code": str(result.get("new_share_code") or ""),
            "raw": result,
        }

    raise RuntimeError(f"Unsupported provider: {provider}")


def transfer_item_all(item: dict[str, Any], config: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    successes: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    priority = {"quark": 0, "baidu": 1}
    links = sorted(
        item.get("provider_links") or [],
        key=lambda link: priority.get(str(link.get("provider") or ""), 99),
    )
    for link in links:
        provider = str(link.get("provider") or "")
        if provider not in {"quark", "baidu"}:
            continue
        try:
            log_event("info", f"开始转存：{item['title']} [{provider}]", {"source_key": item["source_key"]})
            result = transfer_link(link, config)
            log_event("info", f"转存成功：{item['title']} [{provider}]", {"url": result["url"]})
            successes.append(result)
        except Exception as exc:
            message = str(exc)
            failures.append({"provider": provider, "error": message})
            log_event("error", f"转存失败：{item['title']} [{provider}] {message}", {"source_key": item["source_key"]})
    return successes, failures


def try_transfer_item(item: dict[str, Any], config: dict[str, Any]) -> Tuple[Optional[dict[str, Any]], list[dict[str, str]]]:
    successes, failures = transfer_item_all(item, config)
    return (successes[0] if successes else None), failures


def links_to_new_links(links: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        link["provider"]: {
            "url": link["url"],
            "code": link.get("code", ""),
        }
        for link in links
    }


def insert_transferred_resource(item: dict[str, Any], links: list[dict[str, Any]] | dict[str, Any]) -> dict[str, Any]:
    stamp = now_iso()
    if isinstance(links, dict):
        links = [links]
    new_links = links_to_new_links(links)
    with db() as conn:
        conn.execute(
            """
            insert into resources(
                source_key, source_url, title, intro, image_url,
                provider_links, new_links, raw, source_rank,
                transferred_at, created_at, updated_at
            ) values(?, ?, ?, ?, ?, '[]', ?, ?, ?, ?, ?, ?)
            on conflict(source_key) do update set
                title = excluded.title,
                intro = excluded.intro,
                image_url = excluded.image_url,
                provider_links = '[]',
                new_links = excluded.new_links,
                raw = excluded.raw,
                source_rank = excluded.source_rank,
                transferred_at = excluded.transferred_at,
                updated_at = excluded.updated_at
            """,
            (
                item["source_key"],
                item["source_url"],
                item["title"],
                item["intro"],
                item["image_url"],
                json.dumps(new_links, ensure_ascii=False),
                json.dumps(item.get("raw") or {}, ensure_ascii=False),
                int(item.get("source_rank") or 999999),
                stamp,
                stamp,
                stamp,
            ),
        )
        for provider in new_links:
            conn.execute(
                "delete from transfer_failures where source_key = ? and provider = ?",
                (item["source_key"], provider),
            )
        row = conn.execute("select * from resources where source_key = ?", (item["source_key"],)).fetchone()
    return row_to_resource(row)


def record_transfer_failures(item: dict[str, Any], errors: list[dict[str, str]]) -> None:
    stamp = now_iso()
    with db() as conn:
        for error in errors:
            provider = str(error.get("provider") or "")
            last_error = str(error.get("error") or "unknown error")
            conn.execute(
                """
                insert into transfer_failures(
                    source_key, provider, title, item_json, errors, source_rank,
                    retry_count, last_error, created_at, updated_at
                ) values(?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
                on conflict(source_key, provider) do update set
                    title = excluded.title,
                    item_json = excluded.item_json,
                    errors = excluded.errors,
                    source_rank = excluded.source_rank,
                    last_error = excluded.last_error,
                    updated_at = excluded.updated_at
                """,
                (
                    item["source_key"],
                    provider,
                    item["title"],
                    json.dumps(item, ensure_ascii=False),
                    json.dumps([error], ensure_ascii=False),
                    int(item.get("source_rank") or 999999),
                    last_error,
                    stamp,
                    stamp,
                ),
            )


def record_transfer_failure(item: dict[str, Any], errors: list[dict[str, str]]) -> None:
    record_transfer_failures(item, errors)


def resource_exists(source_key: str) -> bool:
    with db() as conn:
        row = conn.execute("select 1 from resources where source_key = ?", (source_key,)).fetchone()
    return row is not None


def merge_resource_link(item: dict[str, Any], link: dict[str, Any]) -> dict[str, Any]:
    stamp = now_iso()
    with db() as conn:
        row = conn.execute("select * from resources where source_key = ?", (item["source_key"],)).fetchone()
        if not row:
            return insert_transferred_resource(item, link)
        resource = row_to_resource(row)
        new_links = dict(resource.get("new_links") or {})
        new_links[link["provider"]] = {
            "url": link["url"],
            "code": link.get("code", ""),
        }
        conn.execute(
            """
            update resources
            set new_links = ?, updated_at = ?
            where source_key = ?
            """,
            (json.dumps(new_links, ensure_ascii=False), stamp, item["source_key"]),
        )
        conn.execute(
            "delete from transfer_failures where source_key = ? and provider = ?",
            (item["source_key"], link["provider"]),
        )
        row = conn.execute("select * from resources where source_key = ?", (item["source_key"],)).fetchone()
    return row_to_resource(row)


def update_resource_rank(source_key: str, rank: int) -> None:
    with db() as conn:
        conn.execute(
            "update resources set source_rank = ?, updated_at = ? where source_key = ?",
            (rank, now_iso(), source_key),
        )


def transfer_resource(resource: dict[str, Any], config: dict[str, Any], providers: Optional[List[str]] = None) -> dict[str, Any]:
    wanted = set(providers or ["quark", "baidu"])
    source_links = [
        link
        for link in resource.get("provider_links", [])
        if str(link.get("provider") or "") in wanted
    ]
    item = dict(resource)
    item["provider_links"] = source_links
    successes, errors = transfer_item_all(item, config)
    if not successes:
        raise RuntimeError(json.dumps(errors, ensure_ascii=False))
    new_links = links_to_new_links(successes)
    if errors:
        record_transfer_failures(item, errors)
    if resource.get("id"):
        with db() as conn:
            conn.execute(
                """
                update resources
                set new_links = ?, provider_links = '[]', transferred_at = ?, updated_at = ?
                where id = ?
                """,
                (
                    json.dumps(new_links, ensure_ascii=False),
                    now_iso(),
                    now_iso(),
                    resource["id"],
                ),
            )
    return new_links


async def retry_failures_once(config: dict[str, Any]) -> tuple[int, int]:
    success = 0
    failed = 0
    with db() as conn:
        rows = conn.execute(
            "select * from transfer_failures order by source_rank asc, id asc"
        ).fetchall()
    if not rows:
        return success, failed

    log_event("info", f"开始重试失败队列：{len(rows)} 条")
    for row in rows:
        item = json.loads(row["item_json"])
        provider = str(row["provider"] or "")
        item["provider_links"] = [
            link
            for link in item.get("provider_links", [])
            if str(link.get("provider") or "") == provider
        ]
        successes, errors = await asyncio.to_thread(transfer_item_all, item, config)
        if successes:
            await asyncio.to_thread(merge_resource_link, item, successes[0])
            success += 1
            log_event("info", f"失败队列补链成功：{item['title']} [{provider}]")
        else:
            failed += 1
            stamp = now_iso()
            with db() as conn:
                conn.execute(
                    """
                    update transfer_failures
                    set retry_count = retry_count + 1,
                        errors = ?,
                        last_error = ?,
                        updated_at = ?
                    where source_key = ? and provider = ?
                    """,
                    (
                        json.dumps(errors, ensure_ascii=False),
                        errors[-1]["error"] if errors else "no available provider",
                        stamp,
                        item["source_key"],
                        provider,
                    ),
                )
        await asyncio.sleep(float(config.get("request_delay_seconds") or 2))
    return success, failed


async def run_fetch_once(limit: Optional[int] = None, retry_failures: Optional[bool] = True) -> dict[str, Any]:
    async with fetch_lock:
        config = load_config()
        run_limit = int(limit or config.get("scan_limit") or config.get("fetch_limit") or 80)
        runtime_status.update(
            {
                "busy": True,
                "last_started_at": now_iso(),
                "last_error": "",
            }
        )
        run_id = None
        with db() as conn:
            cur = conn.execute("insert into runs(started_at) values(?)", (now_iso(),))
            run_id = cur.lastrowid
        try:
            log_event("info", f"开始扫描：最多 {run_limit} 条")
            items = await fetch_source(config, run_limit)
            seen = len(items)
            transferred = 0
            failed = 0
            skipped = 0
            for item in items:
                if resource_exists(item["source_key"]):
                    update_resource_rank(item["source_key"], int(item.get("source_rank") or 999999))
                    skipped += 1
                    continue
                successes, errors = await asyncio.to_thread(transfer_item_all, item, config)
                if successes:
                    await asyncio.to_thread(insert_transferred_resource, item, successes)
                    transferred += 1
                    if errors:
                        await asyncio.to_thread(record_transfer_failures, item, errors)
                        failed += len(errors)
                else:
                    await asyncio.to_thread(record_transfer_failures, item, errors)
                    failed += max(1, len(errors))
                await asyncio.sleep(float(config.get("request_delay_seconds") or 2))

            retry_success = 0
            retry_failed = 0
            if retry_failures:
                retry_success, retry_failed = await retry_failures_once(config)
                transferred += retry_success
                failed += retry_failed

            result = {
                "ok": True,
                "seen": seen,
                "skipped_existing": skipped,
                "inserted": transferred,
                "transferred": transferred,
                "failed": failed,
                "retry_success": retry_success,
                "retry_failed": retry_failed,
            }
            with db() as conn:
                conn.execute(
                    """
                    update runs
                    set finished_at = ?, ok = 1, seen = ?, inserted = ?, transferred = ?
                    where id = ?
                    """,
                    (now_iso(), seen, transferred, transferred, run_id),
                )
            runtime_status.update(
                {
                    "last_finished_at": now_iso(),
                    "last_seen": seen,
                    "last_inserted": transferred,
                    "last_transferred": transferred,
                    "last_failed": failed,
                }
            )
            log_event("info", f"扫描完成：看到 {seen} 条，新增转存 {transferred} 条，失败 {failed} 条，已有 {skipped} 条")
            return result
        except Exception as exc:
            runtime_status["last_error"] = str(exc)
            log_event("error", f"扫描异常：{exc}")
            with db() as conn:
                conn.execute(
                    "update runs set finished_at = ?, ok = 0, error = ? where id = ?",
                    (now_iso(), str(exc), run_id),
                )
            raise
        finally:
            runtime_status["busy"] = False


async def scheduler_loop() -> None:
    await asyncio.sleep(1)
    while True:
        config = load_config()
        runtime_status["running"] = True
        interval = int(config.get("fetch_interval_seconds") or 1800)
        if not runtime_status["busy"]:
            try:
                await run_fetch_once(int(config.get("scan_limit") or config.get("fetch_limit") or 80), True)
            except Exception as exc:
                log_event("error", f"定时扫描失败：{exc}")
        next_ts = time.time() + interval
        runtime_status["next_run_at"] = datetime.fromtimestamp(next_ts).astimezone().isoformat(timespec="seconds")
        await asyncio.sleep(interval)


@app.on_event("startup")
async def on_startup() -> None:
    global scheduler_task
    init_db()
    save_config({})
    runtime_status["running"] = True
    scheduler_task = asyncio.create_task(scheduler_loop())


@app.on_event("shutdown")
async def on_shutdown() -> None:
    if scheduler_task:
        scheduler_task.cancel()


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return (APP_DIR / "static" / "index.html").read_text(encoding="utf-8")


@app.get("/api/config", dependencies=[Depends(require_admin)])
async def api_get_config() -> Dict[str, Any]:
    return load_config()


@app.post("/api/config", dependencies=[Depends(require_admin)])
async def api_save_config(payload: ConfigIn) -> Dict[str, Any]:
    return save_config(payload.dict(exclude_unset=True))


@app.get("/api/status", dependencies=[Depends(require_admin)])
async def api_status() -> Dict[str, Any]:
    with db() as conn:
        transferred = conn.execute(
            "select count(*) as c from resources where transferred_at != ''"
        ).fetchone()["c"]
        failures = conn.execute("select count(*) as c from transfer_failures").fetchone()["c"]
        recent_runs = [
            dict(row)
            for row in conn.execute(
                "select * from runs order by id desc limit 10"
            ).fetchall()
        ]
    return {
        **runtime_status,
        "total_resources": transferred,
        "transferred_resources": transferred,
        "failure_resources": failures,
        "recent_runs": recent_runs,
    }


@app.post("/api/run", dependencies=[Depends(require_admin)])
async def api_run(payload: RunIn) -> Dict[str, Any]:
    return await run_fetch_once(payload.limit, payload.retry_failures)


@app.post("/api/start", dependencies=[Depends(require_admin)])
async def api_start() -> Dict[str, Any]:
    config = save_config({})
    return {"enabled": True, "interval": config["fetch_interval_seconds"]}


@app.post("/api/stop", dependencies=[Depends(require_admin)])
async def api_stop() -> Dict[str, Any]:
    return {"enabled": True, "message": "定时扫描为强制开启，不能关闭。"}


@app.get("/api/resources", dependencies=[Depends(require_admin)])
async def api_resources(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    with db() as conn:
        rows = conn.execute(
            """
            select * from resources
            where transferred_at != ''
            order by source_rank asc, id asc
            limit ? offset ?
            """,
            (limit, offset),
        ).fetchall()
        total = conn.execute(
            "select count(*) as c from resources where transferred_at != ''"
        ).fetchone()["c"]
    return {"total": total, "items": [row_to_resource(row) for row in rows]}


@app.get("/api/resources/latest")
async def api_latest(limit: int = Query(20, ge=1, le=200)) -> Dict[str, Any]:
    with db() as conn:
        rows = conn.execute(
            """
            select * from resources
            where transferred_at != ''
            order by source_rank asc, id asc
            limit ?
            """,
            (limit,),
        ).fetchall()
    return {"items": [public_resource(row_to_resource(row)) for row in rows]}


@app.get("/api/events", dependencies=[Depends(require_admin)])
async def api_events(limit: int = Query(100, ge=1, le=500)) -> Dict[str, Any]:
    with db() as conn:
        rows = conn.execute(
            "select * from events order by id desc limit ?",
            (limit,),
        ).fetchall()
    events = []
    for row in rows:
        item = dict(row)
        try:
            item["context"] = json.loads(item["context"])
        except json.JSONDecodeError:
            item["context"] = {}
        events.append(item)
    return {"items": events}


@app.get("/api/failures", dependencies=[Depends(require_admin)])
async def api_failures(limit: int = Query(100, ge=1, le=500)) -> Dict[str, Any]:
    with db() as conn:
        rows = conn.execute(
            """
            select * from transfer_failures
            order by source_rank asc, id asc
            limit ?
            """,
            (limit,),
        ).fetchall()
    items = []
    for row in rows:
        item = dict(row)
        for key in ("item_json", "errors"):
            try:
                item[key] = json.loads(item[key])
            except json.JSONDecodeError:
                item[key] = {} if key == "item_json" else []
        items.append(item)
    return {"items": items}


@app.get("/api/clients/{client_id}/pending")
async def api_client_pending(
    client_id: str,
    limit: int = Query(20, ge=1, le=200),
    first_limit: int = Query(20, ge=1, le=500),
    mark: bool = Query(True),
    ensure: bool = Query(True),
) -> Dict[str, Any]:
    with db() as conn:
        delivered_count = conn.execute(
            "select count(*) as c from delivery_marks where client_id = ?",
            (client_id,),
        ).fetchone()["c"]
    actual_limit = first_limit if delivered_count == 0 else limit

    if ensure and not runtime_status["busy"]:
        with db() as conn:
            pending_count = conn.execute(
                """
                select count(*) as c
                from resources r
                where r.transferred_at != ''
                and not exists (
                    select 1 from delivery_marks d
                    where d.client_id = ? and d.resource_id = r.id
                )
                """,
                (client_id,),
            ).fetchone()["c"]
        if pending_count < actual_limit:
            try:
                await run_fetch_once(max(actual_limit, int(load_config().get("scan_limit") or 80)), True)
            except Exception:
                pass

    with db() as conn:
        rows = conn.execute(
            """
            select r.*
            from resources r
            where r.transferred_at != ''
            and not exists (
                select 1 from delivery_marks d
                where d.client_id = ? and d.resource_id = r.id
            )
            order by r.source_rank asc, r.id asc
            limit ?
            """,
            (client_id, actual_limit),
        ).fetchall()
    items = [public_resource(row_to_resource(row)) for row in rows]
    ids = [int(item["id"]) for item in items]
    if mark:
        mark_delivered(client_id, ids)
    return {
        "client_id": client_id,
        "first_delivery": delivered_count == 0,
        "marked": mark,
        "items": items,
    }


@app.post("/api/clients/{client_id}/ack")
async def api_client_ack(client_id: str, payload: AckIn) -> Dict[str, Any]:
    mark_delivered(client_id, payload.ids)
    return {"client_id": client_id, "marked_ids": payload.ids}


@app.post("/api/resources/{resource_id}/transfer", dependencies=[Depends(require_admin)])
async def api_transfer(resource_id: int, payload: TransferIn) -> Dict[str, Any]:
    config = load_config()
    with db() as conn:
        row = conn.execute("select * from resources where id = ?", (resource_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="resource not found")
    resource = row_to_resource(row)
    links = await asyncio.to_thread(transfer_resource, resource, config, payload.providers)
    return {"new_links": links}
