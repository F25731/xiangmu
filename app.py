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
from typing import Any, Dict, List, Optional
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
    "enabled": False,
    "auto_transfer": False,
    "first_run_limit": 20,
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
    enabled: Optional[bool] = None
    auto_transfer: Optional[bool] = None
    first_run_limit: Optional[int] = Field(default=None, ge=1, le=500)
    fetch_limit: Optional[int] = Field(default=None, ge=1, le=500)
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
    transfer: Optional[bool] = None


class TransferIn(BaseModel):
    providers: Optional[List[str]] = None


runtime_status: dict[str, Any] = {
    "running": False,
    "busy": False,
    "last_started_at": "",
    "last_finished_at": "",
    "last_error": "",
    "last_inserted": 0,
    "last_seen": 0,
    "last_transferred": 0,
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
    with db() as conn:
        for key, value in current.items():
            conn.execute(
                """
                insert into config(key, value) values(?, ?)
                on conflict(key) do update set value = excluded.value
                """,
                (key, json.dumps(value, ensure_ascii=False)),
            )
    runtime_status["running"] = bool(current.get("enabled"))
    return current


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


def transfer_resource(resource: dict[str, Any], config: dict[str, Any], providers: Optional[List[str]] = None) -> dict[str, Any]:
    wanted = set(providers or ["quark", "baidu"])
    timeout = int(config.get("runner_timeout_seconds") or 600)
    new_links: dict[str, Any] = dict(resource.get("new_links") or {})
    delete_names = [name for name in config.get("delete_names", []) if str(name).strip()]

    for link in resource.get("provider_links", []):
        provider = link.get("provider")
        if provider not in wanted:
            continue
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
            new_links["quark"] = {
                "url": result.get("new_share_url") or "",
                "code": result.get("new_share_code") or "",
                "raw": result,
            }
        elif provider == "baidu":
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
            new_links["baidu"] = {
                "url": result.get("new_share_url") or "",
                "code": result.get("new_share_code") or "",
                "raw": result,
            }
    if new_links:
        with db() as conn:
            conn.execute(
                """
                update resources
                set new_links = ?, transferred_at = ?, updated_at = ?
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


async def run_fetch_once(limit: Optional[int] = None, transfer: Optional[bool] = None) -> dict[str, Any]:
    async with fetch_lock:
        config = load_config()
        run_limit = int(limit or config.get("fetch_limit") or 20)
        should_transfer = bool(config.get("auto_transfer") if transfer is None else transfer)
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
            items = await fetch_source(config, run_limit)
            inserted, changed = upsert_resources(items)
            transferred = 0
            if should_transfer:
                for resource in changed:
                    transfer_resource(resource, config)
                    transferred += 1
                    await asyncio.sleep(float(config.get("request_delay_seconds") or 2))
            result = {
                "ok": True,
                "seen": len(items),
                "inserted": inserted,
                "transferred": transferred,
            }
            with db() as conn:
                conn.execute(
                    """
                    update runs
                    set finished_at = ?, ok = 1, seen = ?, inserted = ?, transferred = ?
                    where id = ?
                    """,
                    (now_iso(), len(items), inserted, transferred, run_id),
                )
            runtime_status.update(
                {
                    "last_finished_at": now_iso(),
                    "last_seen": len(items),
                    "last_inserted": inserted,
                    "last_transferred": transferred,
                }
            )
            return result
        except Exception as exc:
            runtime_status["last_error"] = str(exc)
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
        runtime_status["running"] = bool(config.get("enabled"))
        interval = int(config.get("fetch_interval_seconds") or 1800)
        if runtime_status["running"] and not runtime_status["busy"]:
            try:
                await run_fetch_once(int(config.get("fetch_limit") or 20), None)
            except Exception:
                pass
        next_ts = time.time() + interval
        runtime_status["next_run_at"] = datetime.fromtimestamp(next_ts).astimezone().isoformat(timespec="seconds")
        await asyncio.sleep(interval)


@app.on_event("startup")
async def on_startup() -> None:
    global scheduler_task
    init_db()
    runtime_status["running"] = bool(load_config().get("enabled"))
    scheduler_task = asyncio.create_task(scheduler_loop())


@app.on_event("shutdown")
async def on_shutdown() -> None:
    if scheduler_task:
        scheduler_task.cancel()


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return INDEX_HTML


@app.get("/api/config", dependencies=[Depends(require_admin)])
async def api_get_config() -> Dict[str, Any]:
    return load_config()


@app.post("/api/config", dependencies=[Depends(require_admin)])
async def api_save_config(payload: ConfigIn) -> Dict[str, Any]:
    return save_config(payload.dict(exclude_unset=True))


@app.get("/api/status", dependencies=[Depends(require_admin)])
async def api_status() -> Dict[str, Any]:
    with db() as conn:
        total = conn.execute("select count(*) as c from resources").fetchone()["c"]
        transferred = conn.execute(
            "select count(*) as c from resources where transferred_at != ''"
        ).fetchone()["c"]
        recent_runs = [
            dict(row)
            for row in conn.execute(
                "select * from runs order by id desc limit 10"
            ).fetchall()
        ]
    return {
        **runtime_status,
        "total_resources": total,
        "transferred_resources": transferred,
        "recent_runs": recent_runs,
    }


@app.post("/api/run", dependencies=[Depends(require_admin)])
async def api_run(payload: RunIn) -> Dict[str, Any]:
    return await run_fetch_once(payload.limit, payload.transfer)


@app.post("/api/start", dependencies=[Depends(require_admin)])
async def api_start() -> Dict[str, Any]:
    config = save_config({"enabled": True})
    return {"enabled": config["enabled"]}


@app.post("/api/stop", dependencies=[Depends(require_admin)])
async def api_stop() -> Dict[str, Any]:
    config = save_config({"enabled": False})
    return {"enabled": config["enabled"]}


@app.get("/api/resources", dependencies=[Depends(require_admin)])
async def api_resources(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    with db() as conn:
        rows = conn.execute(
            "select * from resources order by id desc limit ? offset ?",
            (limit, offset),
        ).fetchall()
        total = conn.execute("select count(*) as c from resources").fetchone()["c"]
    return {"total": total, "items": [row_to_resource(row) for row in rows]}


@app.get("/api/resources/latest")
async def api_latest(limit: int = Query(20, ge=1, le=200)) -> Dict[str, Any]:
    with db() as conn:
        rows = conn.execute(
            "select * from resources order by id desc limit ?",
            (limit,),
        ).fetchall()
    return {"items": [row_to_resource(row) for row in rows]}


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


INDEX_HTML = r"""
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>资源抓取控制台</title>
  <style>
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: #f7f7f5;
      color: #202124;
      font-family: "Microsoft YaHei", Arial, sans-serif;
      font-size: 14px;
      line-height: 1.55;
    }
    button, input, textarea { font: inherit; }
    button {
      border: 1px solid #202124;
      background: #202124;
      color: #fff;
      padding: 8px 12px;
      cursor: pointer;
    }
    button.secondary { background: transparent; color: #202124; }
    button:disabled { opacity: .5; cursor: not-allowed; }
    main {
      width: min(1180px, calc(100% - 32px));
      margin: 34px auto 60px;
      display: grid;
      grid-template-columns: 360px 1fr;
      gap: 24px;
      align-items: start;
    }
    h1 { margin: 0 0 20px; font-size: 24px; }
    h2 { margin: 0 0 14px; font-size: 16px; }
    section {
      background: #fff;
      border: 1px solid #ddd;
      padding: 18px;
      margin-bottom: 16px;
    }
    label { display: block; margin: 12px 0 5px; color: #555; }
    input, textarea {
      width: 100%;
      border: 1px solid #ccc;
      background: #fff;
      padding: 8px 10px;
      color: #202124;
    }
    textarea { min-height: 86px; resize: vertical; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
    .actions { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 14px; }
    .status {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 10px;
    }
    .metric { border: 1px solid #ddd; padding: 12px; background: #fbfbfa; }
    .metric strong { display: block; font-size: 20px; }
    .item {
      border-top: 1px solid #e3e3e0;
      padding: 14px 0;
      display: grid;
      grid-template-columns: 92px 1fr;
      gap: 14px;
    }
    .thumb {
      width: 92px;
      aspect-ratio: 4 / 3;
      object-fit: cover;
      background: #eee;
      border: 1px solid #ddd;
    }
    .item h3 { margin: 0 0 6px; font-size: 15px; }
    .item p { margin: 0 0 8px; color: #555; }
    .links { display: flex; flex-wrap: wrap; gap: 8px; }
    .links a { color: #0b57d0; text-decoration: none; }
    .message { min-height: 20px; color: #555; margin-top: 10px; }
    @media (max-width: 880px) {
      main { grid-template-columns: 1fr; }
      .status { grid-template-columns: repeat(2, 1fr); }
    }
  </style>
</head>
<body>
  <main>
    <div>
      <h1>资源抓取控制台</h1>
      <section>
        <h2>运行</h2>
        <div class="status">
          <div class="metric"><span>资源</span><strong id="total">0</strong></div>
          <div class="metric"><span>已转存</span><strong id="transferred">0</strong></div>
          <div class="metric"><span>上次新增</span><strong id="inserted">0</strong></div>
          <div class="metric"><span>状态</span><strong id="running">-</strong></div>
        </div>
        <div class="actions">
          <button id="runBtn">立即抓取</button>
          <button id="firstBtn" class="secondary">首次抓取 N 条</button>
          <button id="startBtn" class="secondary">启动定时</button>
          <button id="stopBtn" class="secondary">停止定时</button>
        </div>
        <div class="message" id="message"></div>
      </section>

      <section>
        <h2>基础配置</h2>
        <label>来源地址</label>
        <input id="source_url">
        <div class="row">
          <div>
            <label>首次抓取条数</label>
            <input id="first_run_limit" type="number" min="1">
          </div>
          <div>
            <label>每次抓取条数</label>
            <input id="fetch_limit" type="number" min="1">
          </div>
        </div>
        <div class="row">
          <div>
            <label>抓取间隔秒</label>
            <input id="fetch_interval_seconds" type="number" min="60">
          </div>
          <div>
            <label>请求间隔秒</label>
            <input id="request_delay_seconds" type="number" min="0.5" step="0.5">
          </div>
        </div>
        <label><input id="enabled" type="checkbox" style="width:auto"> 启用定时抓取</label>
        <label><input id="auto_transfer" type="checkbox" style="width:auto"> 新资源自动转存</label>
      </section>

      <section>
        <h2>网盘配置</h2>
        <label>夸克 Cookie</label>
        <textarea id="quark_cookie"></textarea>
        <label>夸克保存目录 fid</label>
        <input id="quark_save_fid">
        <label>百度 Cookie</label>
        <textarea id="baidu_cookie"></textarea>
        <label>百度新分享提取码</label>
        <input id="baidu_new_share_code">
        <label>百度转存目录</label>
        <input id="baidu_transfer_dir">
        <label>OpenList 地址</label>
        <input id="openlist_url">
        <label>OpenList Token</label>
        <textarea id="openlist_token"></textarea>
        <label>OpenList 百度挂载根目录</label>
        <input id="openlist_root_dir">
        <label>广告文件名，一行一个</label>
        <textarea id="delete_names"></textarea>
        <div class="actions">
          <button id="saveBtn">保存配置</button>
        </div>
      </section>
    </div>

    <section>
      <h2>最近资源</h2>
      <div id="items"></div>
    </section>
  </main>

  <script>
    const $ = (id) => document.getElementById(id);
    const adminToken = new URLSearchParams(location.search).get("token") || localStorage.getItem("adminToken") || "";
    if (adminToken) localStorage.setItem("adminToken", adminToken);
    const fields = [
      "source_url", "first_run_limit", "fetch_limit", "fetch_interval_seconds",
      "request_delay_seconds", "quark_cookie", "quark_save_fid", "baidu_cookie",
      "baidu_new_share_code", "baidu_transfer_dir", "openlist_url",
      "openlist_token", "openlist_root_dir"
    ];
    function msg(text) { $("message").textContent = text || ""; }
    async function api(path, options = {}) {
      const headers = { "Content-Type": "application/json" };
      if (adminToken) headers["X-Admin-Token"] = adminToken;
      const res = await fetch(path, {
        headers,
        ...options
      });
      if (!res.ok) throw new Error(await res.text());
      return await res.json();
    }
    async function loadConfig() {
      const config = await api("/api/config");
      for (const key of fields) $(key).value = config[key] ?? "";
      $("enabled").checked = !!config.enabled;
      $("auto_transfer").checked = !!config.auto_transfer;
      $("delete_names").value = (config.delete_names || []).join("\n");
    }
    async function saveConfig() {
      const payload = {};
      for (const key of fields) payload[key] = $(key).value;
      for (const key of ["first_run_limit", "fetch_limit", "fetch_interval_seconds"]) payload[key] = Number(payload[key]);
      payload.request_delay_seconds = Number(payload.request_delay_seconds);
      payload.enabled = $("enabled").checked;
      payload.auto_transfer = $("auto_transfer").checked;
      payload.delete_names = $("delete_names").value.split("\n").map(x => x.trim()).filter(Boolean);
      await api("/api/config", { method: "POST", body: JSON.stringify(payload) });
      msg("配置已保存");
      await refresh();
    }
    function renderItems(items) {
      $("items").innerHTML = items.map(item => {
        const original = (item.provider_links || []).map(link => `<a href="${link.url}" target="_blank">${link.provider}${link.code ? " " + link.code : ""}</a>`).join("");
        const newer = Object.entries(item.new_links || {}).map(([name, link]) => `<a href="${link.url}" target="_blank">新${name}${link.code ? " " + link.code : ""}</a>`).join("");
        return `<div class="item">
          ${item.image_url ? `<img class="thumb" src="${item.image_url}">` : `<div class="thumb"></div>`}
          <div>
            <h3>${item.title}</h3>
            <p>${item.intro || ""}</p>
            <div class="links">${original}${newer}</div>
          </div>
        </div>`;
      }).join("");
    }
    async function refresh() {
      const status = await api("/api/status");
      $("total").textContent = status.total_resources;
      $("transferred").textContent = status.transferred_resources;
      $("inserted").textContent = status.last_inserted;
      $("running").textContent = status.busy ? "忙" : (status.running ? "开" : "停");
      if (status.last_error) msg(status.last_error);
      const resources = await api("/api/resources?limit=30");
      renderItems(resources.items || []);
    }
    $("saveBtn").onclick = () => saveConfig().catch(e => msg(e.message));
    $("runBtn").onclick = async () => {
      msg("抓取中...");
      await api("/api/run", { method: "POST", body: JSON.stringify({}) }).catch(e => msg(e.message));
      await refresh();
    };
    $("firstBtn").onclick = async () => {
      msg("首次抓取中...");
      await api("/api/run", { method: "POST", body: JSON.stringify({ limit: Number($("first_run_limit").value), transfer: $("auto_transfer").checked }) }).catch(e => msg(e.message));
      await refresh();
    };
    $("startBtn").onclick = async () => { await api("/api/start", { method: "POST" }); await loadConfig(); await refresh(); };
    $("stopBtn").onclick = async () => { await api("/api/stop", { method: "POST" }); await loadConfig(); await refresh(); };
    loadConfig().then(refresh).catch(e => msg(e.message));
    setInterval(refresh, 10000);
  </script>
</body>
</html>
"""
