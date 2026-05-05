#!/usr/bin/env python3
"""Local test script that ports xinyue-search's Quark transfer and cleanup flow."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import PurePosixPath
from typing import Any, Iterable


DEFAULT_DELETE_TARGETS = [
    "网创平台入口",
    "免责声明",
    "项目工具资料",
    "众包任务悬赏平台",
    "返佣任务平台1-入口",
    "返佣任务平台2-操作教程",
    "领取AI创作工具",
]

QUARK_BASE_URL = "https://drive-pc.quark.cn/1/clouddrive"
QUARK_QUERY = {"pr": "ucpro", "fr": "pc", "uc_param_str": ""}


class ScriptError(Exception):
    """Raised when a remote call or local validation fails."""


def normalize_query(extra: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = dict(QUARK_QUERY)
    if extra:
        payload.update(extra)
    return payload


def normalize_targets(targets: Iterable[str]) -> set[str]:
    normalized: set[str] = set()
    for item in targets:
        text = str(item).strip()
        if text:
            normalized.add(text)
    return normalized


def stem_for_match(name: str) -> str:
    if "." not in name:
        return name
    return name.rsplit(".", 1)[0]


def extract_pwd_id(share_url: str) -> str:
    text = share_url.strip()
    marker = "/s/"
    if marker not in text:
        raise ScriptError("Quark share URL format is invalid.")
    pwd_id = text.split(marker, 1)[1]
    return pwd_id.split("?", 1)[0].split("#", 1)[0].strip("/")


def extract_share_code(share_url: str, share_code: str) -> str:
    code = share_code.strip()
    if code:
        return code
    parsed = urllib.parse.urlparse(share_url.strip())
    query = urllib.parse.parse_qs(parsed.query)
    for key in ("pwd", "passcode"):
        value = query.get(key)
        if value and str(value[0]).strip():
            return str(value[0]).strip()
    return ""


def request_json(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    data: dict[str, Any] | None = None,
    query: dict[str, Any] | None = None,
    timeout: int = 60,
) -> dict[str, Any]:
    final_url = url
    if query:
        final_url = f"{url}?{urllib.parse.urlencode(query)}"

    body: bytes | None = None
    request_headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
    }
    if headers:
        request_headers.update(headers)
    if data is not None:
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")

    req = urllib.request.Request(
        url=final_url,
        data=body,
        headers=request_headers,
        method=method.upper(),
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise ScriptError(f"HTTP {exc.code} {final_url}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise ScriptError(f"Request failed for {final_url}: {exc.reason}") from exc

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ScriptError(f"Non-JSON response from {final_url}: {raw[:500]}") from exc
    if not isinstance(payload, dict):
        raise ScriptError(f"Unexpected response from {final_url}: {payload!r}")
    return payload


class QuarkClient:
    def __init__(self, cookie: str, save_fid: str) -> None:
        self.cookie = cookie.strip()
        self.save_fid = save_fid.strip() or "0"
        if not self.cookie:
            raise ScriptError("Missing Quark cookie.")

    @property
    def headers(self) -> dict[str, str]:
        return {
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Referer": "https://pan.quark.cn/",
            "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "cookie": self.cookie,
        }

    def api(
        self,
        path: str,
        *,
        method: str = "GET",
        data: dict[str, Any] | None = None,
        query: dict[str, Any] | None = None,
        timeout: int = 60,
    ) -> dict[str, Any]:
        payload = request_json(
            f"{QUARK_BASE_URL}{path}",
            method=method,
            headers=self.headers,
            data=data,
            query=normalize_query(query),
            timeout=timeout,
        )
        status = payload.get("status")
        if status != 200:
            raise ScriptError(str(payload.get("message") or "Quark API request failed."))
        return payload

    def list_dir(self, pdir_fid: str) -> list[dict[str, Any]]:
        payload = self.api(
            "/file/sort",
            method="GET",
            query={
                "pdir_fid": pdir_fid,
                "_page": 1,
                "_size": 200,
                "_fetch_total": 1,
                "_fetch_sub_dirs": 0,
                "_sort": "file_type:asc,updated_at:desc",
            },
        )
        items = ((payload.get("data") or {}).get("list")) or []
        return items if isinstance(items, list) else []

    def get_stoken(self, pwd_id: str, share_code: str) -> str:
        payload = self.api(
            "/share/sharepage/token",
            method="POST",
            data={"passcode": share_code, "pwd_id": pwd_id},
        )
        stoken = str((payload.get("data") or {}).get("stoken") or "")
        if not stoken:
            raise ScriptError("Quark returned no stoken.")
        return stoken.replace(" ", "+")

    def get_share_detail(self, pwd_id: str, stoken: str) -> dict[str, Any]:
        payload = self.api(
            "/share/sharepage/detail",
            method="GET",
            query={
                "pwd_id": pwd_id,
                "stoken": stoken,
                "pdir_fid": "0",
                "force": "0",
                "_page": "1",
                "_size": "100",
                "_fetch_banner": "1",
                "_fetch_share": "1",
                "_fetch_total": "1",
                "_sort": "file_type:asc,updated_at:desc",
            },
        )
        data = payload.get("data")
        if not isinstance(data, dict):
            raise ScriptError("Quark share detail payload is invalid.")
        return data

    def share_save(
        self,
        pwd_id: str,
        stoken: str,
        fid_list: list[str],
        fid_token_list: list[str],
    ) -> str:
        payload = self.api(
            "/share/sharepage/save",
            method="POST",
            data={
                "fid_list": fid_list,
                "fid_token_list": fid_token_list,
                "to_pdir_fid": self.save_fid,
                "pwd_id": pwd_id,
                "stoken": stoken,
                "pdir_fid": "0",
                "scene": "link",
            },
            query={"entry": "update_share"},
            timeout=120,
        )
        task_id = str((payload.get("data") or {}).get("task_id") or "")
        if not task_id:
            raise ScriptError("Quark returned no save task_id.")
        return task_id

    def get_task(self, task_id: str, retry_index: int) -> dict[str, Any]:
        payload = self.api(
            "/task",
            method="GET",
            query={"task_id": task_id, "retry_index": retry_index},
            timeout=120,
        )
        data = payload.get("data")
        if not isinstance(data, dict):
            raise ScriptError("Quark task payload is invalid.")
        return data

    def wait_for_task(self, task_id: str, max_retries: int = 50) -> dict[str, Any]:
        last_data: dict[str, Any] | None = None
        for retry_index in range(max_retries):
            data = self.get_task(task_id, retry_index)
            last_data = data
            if data.get("status") == 2:
                return data
            time.sleep(1.5)
        raise ScriptError(f"Quark save task did not complete in time: {last_data!r}")

    def create_share_task(
        self,
        fid_list: list[str],
        title: str,
        expired_type: int = 1,
    ) -> str:
        clean_fids = [str(fid).strip() for fid in fid_list if str(fid).strip()]
        if not clean_fids:
            raise ScriptError("Quark create share needs at least one fid.")
        payload = self.api(
            "/share",
            method="POST",
            data={
                "fid_list": clean_fids,
                "expired_type": int(expired_type),
                "title": title.strip() or "云盘资源分享",
                "url_type": 1,
            },
            timeout=120,
        )
        task_id = str((payload.get("data") or {}).get("task_id") or "")
        if not task_id:
            raise ScriptError("Quark returned no share task_id.")
        return task_id

    def get_share_password(self, share_id: str) -> dict[str, Any]:
        payload = self.api(
            "/share/password",
            method="POST",
            data={"share_id": share_id},
            timeout=120,
        )
        data = payload.get("data")
        if not isinstance(data, dict):
            raise ScriptError("Quark share password payload is invalid.")
        return data

    def delete_fids(self, fids: list[str], dry_run: bool = False) -> None:
        if not fids or dry_run:
            return
        self.api(
            "/file/delete",
            method="POST",
            data={"action_type": 2, "exclude_fids": [], "filelist": fids},
            timeout=120,
        )


def collect_matched_items(items: list[dict[str, Any]], delete_targets: Iterable[str]) -> list[dict[str, Any]]:
    target_set = normalize_targets(delete_targets)
    matched: list[dict[str, Any]] = []
    for item in items:
        name = str(item.get("file_name") or "")
        if not name:
            continue
        if name in target_set or stem_for_match(name) in target_set:
            matched.append(item)
    return matched


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Local test that ports xinyue-search's Quark transfer and deletion logic."
    )
    parser.add_argument("--share-url", help="Quark share URL.")
    parser.add_argument("--share-code", default="", help="Quark share code if needed.")
    parser.add_argument(
        "--quark-cookie",
        default=os.getenv("QUARK_COOKIE", ""),
        help="Quark cookie. Can also use QUARK_COOKIE.",
    )
    parser.add_argument(
        "--save-fid",
        default=os.getenv("QUARK_SAVE_FID", "0"),
        help="Target Quark folder fid. Can also use QUARK_SAVE_FID. Default is root fid 0.",
    )
    parser.add_argument(
        "--delete-name",
        action="append",
        default=[],
        help="Add one deletion target. Can be repeated.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print matched deletions, do not actually delete.",
    )
    parser.add_argument(
        "--create-share",
        action="store_true",
        help="Create a new Quark share link after transfer and cleanup.",
    )
    parser.add_argument(
        "--share-title",
        default="",
        help="Title for the newly created Quark share. Defaults to source share title.",
    )
    parser.add_argument(
        "--share-expired-type",
        type=int,
        choices=(1, 2),
        default=1,
        help="Quark share expiry type: 1 permanent, 2 temporary.",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if not args.share_url:
        raise ScriptError("Missing --share-url.")
    if not args.quark_cookie:
        raise ScriptError("Missing --quark-cookie or QUARK_COOKIE.")


def main() -> int:
    args = parse_args()
    try:
        validate_args(args)
        delete_targets = args.delete_name or list(DEFAULT_DELETE_TARGETS)
        share_code = extract_share_code(args.share_url, args.share_code)
        pwd_id = extract_pwd_id(args.share_url)

        client = QuarkClient(cookie=args.quark_cookie, save_fid=args.save_fid)

        before_items = client.list_dir(client.save_fid)
        before_fids = {str(item.get("fid") or "") for item in before_items}

        stoken = client.get_stoken(pwd_id, share_code)
        detail = client.get_share_detail(pwd_id, stoken)
        share_items = detail.get("list") or []
        if not isinstance(share_items, list) or not share_items:
            raise ScriptError("Quark share detail returned no items.")

        fid_list = [str(item.get("fid") or "") for item in share_items if str(item.get("fid") or "").strip()]
        fid_token_list = [
            str(item.get("share_fid_token") or "")
            for item in share_items
            if str(item.get("share_fid_token") or "").strip()
        ]
        if not fid_list or len(fid_list) != len(fid_token_list):
            raise ScriptError("Quark share detail is missing fid or share_fid_token.")

        task_id = client.share_save(pwd_id, stoken, fid_list, fid_token_list)
        task_data = client.wait_for_task(task_id)

        top_fids = [str(item) for item in ((task_data.get("save_as") or {}).get("save_as_top_fids") or []) if str(item).strip()]
        after_items = client.list_dir(client.save_fid)
        new_top_items = [
            item
            for item in after_items
            if str(item.get("fid") or "").strip() and str(item.get("fid") or "") not in before_fids
        ]

        cleanup_root: dict[str, Any] | None = None
        if len(new_top_items) == 1:
            cleanup_root = new_top_items[0]
        elif len(top_fids) == 1:
            cleanup_root = next((item for item in after_items if str(item.get("fid") or "") == top_fids[0]), None)

        cleanup_items: list[dict[str, Any]] = []
        cleanup_root_fid = ""
        cleanup_root_name = ""
        if cleanup_root is not None:
            cleanup_root_fid = str(cleanup_root.get("fid") or "")
            cleanup_root_name = str(cleanup_root.get("file_name") or "")
            if cleanup_root_fid:
                cleanup_items = client.list_dir(cleanup_root_fid)

        matched_items = collect_matched_items(cleanup_items, delete_targets)
        client.delete_fids(
            [str(item.get("fid") or "") for item in matched_items if str(item.get("fid") or "").strip()],
            dry_run=args.dry_run,
        )

        shared_fids: list[str] = []
        new_share: dict[str, Any] = {}
        new_share_url = ""
        new_share_code = ""
        if args.create_share and not args.dry_run:
            shared_fids = top_fids or [
                str(item.get("fid") or "")
                for item in new_top_items
                if str(item.get("fid") or "").strip()
            ]
            if not shared_fids:
                raise ScriptError("Quark share creation failed: no transferred fid found.")
            share_title = (
                args.share_title.strip()
                or str((detail.get("share") or {}).get("title") or "")
                or cleanup_root_name
                or "云盘资源分享"
            )
            share_task_id = client.create_share_task(
                shared_fids,
                share_title,
                expired_type=args.share_expired_type,
            )
            share_task_data = client.wait_for_task(share_task_id)
            share_id = str(share_task_data.get("share_id") or "")
            if not share_id:
                raise ScriptError(f"Quark share task returned no share_id: {share_task_data!r}")
            new_share = client.get_share_password(share_id)
            new_share_url = str(new_share.get("share_url") or "")
            if not new_share_url:
                pwd_id = str(new_share.get("pwd_id") or "")
                if pwd_id:
                    new_share_url = f"https://pan.quark.cn/s/{pwd_id}"
            new_share_code = str(
                new_share.get("share_pwd")
                or new_share.get("passcode")
                or new_share.get("pwd")
                or ""
            )

        result = {
            "save_fid": client.save_fid,
            "share_title": str((detail.get("share") or {}).get("title") or ""),
            "dry_run": args.dry_run,
            "new_top_level_names": [str(item.get("file_name") or "") for item in new_top_items],
            "cleanup_root_name": cleanup_root_name,
            "cleanup_root_fid": cleanup_root_fid,
            "matched_deleted_names": [str(item.get("file_name") or "") for item in matched_items],
            "top_fids_from_task": top_fids,
            "create_share": args.create_share,
            "shared_fids": shared_fids,
            "new_share_url": new_share_url,
            "new_share_code": new_share_code,
            "new_share": new_share,
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except ScriptError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
