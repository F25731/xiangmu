#!/usr/bin/env python3
"""Pure local Baidu transfer + OpenList cleanup test script."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
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

BAIDU_ERROR_CODES = {
    -1: "链接错误，链接失效或缺少提取码或访问频繁风控",
    -4: "无效登录。请退出账号在其他地方的登录",
    -6: "请用浏览器无痕模式获取 Cookie 后再试",
    -7: "转存失败，转存文件夹名有非法字符，不能包含 < > | * ? \\ :",
    -8: "转存失败，目录中已有同名文件或文件夹存在",
    -9: "链接不存在或提取码错误",
    -10: "转存失败，容量不足",
    -12: "链接错误，提取码错误",
    -62: "链接访问次数过多，请手动转存或稍后再试",
    0: "成功",
    2: "转存失败，目标目录不存在",
    4: "转存失败，目录中存在同名文件",
    12: "转存失败，转存文件数超过限制",
    20: "转存失败，容量不足",
    105: "链接错误，所访问的页面不存在",
    115: "该文件禁止分享",
}


class ScriptError(Exception):
    """Raised when a remote call or local validation fails."""


def normalize_base_url(url: str) -> str:
    return url.rstrip("/")


def normalize_posix_path(path: str) -> str:
    text = (path or "/").strip()
    if not text:
        return "/"
    pure = PurePosixPath(text)
    normalized = pure.as_posix()
    if not normalized.startswith("/"):
        normalized = "/" + normalized
    return normalized


def split_parent_and_name(path: str) -> tuple[str, str]:
    pure = PurePosixPath(normalize_posix_path(path))
    if pure == PurePosixPath("/"):
        return "/", ""
    return str(pure.parent), pure.name


def map_baidu_path_to_openlist(baidu_path: str, openlist_root_dir: str) -> str:
    normalized_baidu = normalize_posix_path(baidu_path)
    normalized_root = normalize_posix_path(openlist_root_dir)
    if normalized_root == "/":
        return normalized_baidu
    if normalized_baidu == "/":
        return normalized_root
    return normalize_posix_path(f"{normalized_root}/{normalized_baidu.lstrip('/')}")


def stem_for_match(name: str) -> str:
    if "." not in name:
        return name
    return name.rsplit(".", 1)[0]


def build_delete_match_set(targets: Iterable[str]) -> set[str]:
    normalized = set()
    for item in targets:
        text = item.strip()
        if text:
            normalized.add(text)
    return normalized


def request_raw(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    data: dict[str, Any] | None = None,
    timeout: int = 60,
) -> str:
    body: bytes | None = None
    request_headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
    }
    if headers:
        request_headers.update(headers)
    if data is not None:
        request_headers.setdefault(
            "Content-Type", "application/x-www-form-urlencoded; charset=utf-8"
        )
        body = urllib.parse.urlencode(data).encode("utf-8")

    req = urllib.request.Request(
        url=url,
        data=body,
        headers=request_headers,
        method=method.upper(),
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise ScriptError(f"HTTP {exc.code} {url}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise ScriptError(f"Request failed for {url}: {exc.reason}") from exc


def request_json(
    url: str,
    *,
    method: str = "POST",
    headers: dict[str, str] | None = None,
    data: dict[str, Any] | None = None,
    timeout: int = 60,
    form_encoded: bool = False,
) -> dict[str, Any]:
    body: bytes | None = None
    request_headers = {"Accept": "application/json, text/plain, */*"}
    if headers:
        request_headers.update(headers)

    if data is not None:
        if form_encoded:
            request_headers.setdefault(
                "Content-Type", "application/x-www-form-urlencoded; charset=utf-8"
            )
            body = urllib.parse.urlencode(data).encode("utf-8")
        else:
            request_headers.setdefault("Content-Type", "application/json; charset=utf-8")
            body = json.dumps(data, ensure_ascii=False).encode("utf-8")

    req = urllib.request.Request(
        url=url,
        data=body,
        headers=request_headers,
        method=method.upper(),
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise ScriptError(f"HTTP {exc.code} {url}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise ScriptError(f"Request failed for {url}: {exc.reason}") from exc

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ScriptError(f"Non-JSON response from {url}: {raw[:500]}") from exc

    if not isinstance(parsed, dict):
        raise ScriptError(f"Unexpected JSON shape from {url}: {parsed!r}")
    return parsed


def response_ok(payload: dict[str, Any]) -> bool:
    code = payload.get("code")
    return code in (0, 200) or payload.get("message") == "success"


def response_message(payload: dict[str, Any]) -> str:
    for key in ("message", "msg", "error"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return json.dumps(payload, ensure_ascii=False)


def baidu_error_message(code: int) -> str:
    return BAIDU_ERROR_CODES.get(code, f"未知错误（错误码：{code}）")


@dataclass
class Entry:
    name: str
    is_dir: bool


class OpenListClient:
    def __init__(self, base_url: str, token: str, auth_prefix: str = "") -> None:
        self.base_url = normalize_base_url(base_url)
        self.token = token.strip()
        self.auth_prefix = auth_prefix
        if not self.token:
            raise ScriptError("OpenList token is required.")

    @property
    def headers(self) -> dict[str, str]:
        return {"Authorization": f"{self.auth_prefix}{self.token}"}

    def post(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        return request_json(
            f"{self.base_url}{endpoint}",
            method="POST",
            headers=self.headers,
            data=payload,
        )

    def list_dir(self, path: str, refresh: bool = False) -> list[Entry]:
        resp = self.post(
            "/api/fs/list",
            {
                "path": normalize_posix_path(path),
                "password": "",
                "page": 1,
                "per_page": 0,
                "refresh": refresh,
            },
        )
        if not response_ok(resp):
            raise ScriptError(f"OpenList list failed: {response_message(resp)}")
        content = (resp.get("data") or {}).get("content") or []
        entries: list[Entry] = []
        for item in content:
            if isinstance(item, dict) and item.get("name"):
                entries.append(Entry(name=str(item["name"]), is_dir=bool(item.get("is_dir"))))
        return entries

    def refresh_parent(self, path: str) -> None:
        parent, _ = split_parent_and_name(path)
        try:
            self.list_dir(parent, refresh=True)
        except ScriptError:
            return

    def path_exists(self, path: str) -> bool:
        normalized = normalize_posix_path(path)
        try:
            resp = self.post("/api/fs/get", {"path": normalized, "password": ""})
        except ScriptError as exc:
            message = str(exc)
            if "storage not found" in message.lower() or "object not found" in message.lower():
                return False
            raise
        return response_ok(resp) and bool(resp.get("data"))

    def wait_for_path(self, path: str, retry_seconds: float = 1.5, max_wait_seconds: float = 20.0) -> None:
        normalized = normalize_posix_path(path)
        deadline = time.time() + max_wait_seconds
        last_error: Exception | None = None
        while time.time() <= deadline:
            try:
                if self.path_exists(normalized):
                    return
                self.list_dir("/", refresh=True)
                self.refresh_parent(normalized)
            except ScriptError as exc:
                last_error = exc
            time.sleep(retry_seconds)
        if last_error is not None:
            raise last_error
        raise ScriptError(f"OpenList 在限定时间内未发现路径：{normalized}")

    def mkdir(self, path: str) -> None:
        resp = self.post("/api/fs/mkdir", {"path": normalize_posix_path(path)})
        if not response_ok(resp):
            message = response_message(resp)
            if "exist" not in message.lower():
                raise ScriptError(f"OpenList mkdir failed: {message}")

    def ensure_dir(self, path: str) -> None:
        normalized = normalize_posix_path(path)
        if normalized == "/":
            return
        current = PurePosixPath("/")
        for part in PurePosixPath(normalized).parts[1:]:
            current = current / part
            self.mkdir(str(current))

    def remove(self, dir_path: str, names: list[str], dry_run: bool = False) -> None:
        if not names or dry_run:
            return
        resp = self.post(
            "/api/fs/remove",
            {"dir": normalize_posix_path(dir_path), "names": names},
        )
        if not response_ok(resp):
            raise ScriptError(f"OpenList remove failed: {response_message(resp)}")

    def is_dir(self, path: str) -> bool:
        parent, name = split_parent_and_name(path)
        if not name:
            return True
        for entry in self.list_dir(parent, refresh=True):
            if entry.name == name:
                return entry.is_dir
        raise ScriptError(f"Path not found in OpenList: {path}")


class BaiduPanClient:
    def __init__(self, cookie: str) -> None:
        self.base_url = "https://pan.baidu.com"
        self.cookie = cookie.strip()
        self.bdstoken = ""
        if not self.cookie:
            raise ScriptError("Baidu cookie is required.")

    @property
    def headers(self) -> dict[str, str]:
        return {
            "Referer": "https://pan.baidu.com",
            "Cookie": self.cookie,
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Site": "same-site",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Dest": "document",
        }

    def request_json(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        timeout: int = 60,
    ) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        if params:
            url = f"{url}?{urllib.parse.urlencode(params)}"
        raw = request_raw(url, method=method, headers=self.headers, data=data, timeout=timeout)
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ScriptError(f"Baidu returned non-JSON from {url}: {raw[:500]}") from exc
        if not isinstance(parsed, dict):
            raise ScriptError(f"Unexpected Baidu response from {url}: {parsed!r}")
        return parsed

    def request_text(
        self,
        method: str,
        url: str,
        data: dict[str, Any] | None = None,
        timeout: int = 60,
    ) -> str:
        return request_raw(url, method=method, headers=self.headers, data=data, timeout=timeout)

    def update_cookie_value(self, key: str, value: str) -> None:
        pairs: dict[str, str] = {}
        for part in self.cookie.split(";"):
            piece = part.strip()
            if not piece or "=" not in piece:
                continue
            name, current = piece.split("=", 1)
            pairs[name.strip()] = current.strip()
        pairs[key] = value
        self.cookie = "; ".join(f"{name}={current}" for name, current in pairs.items())

    def get_bdstoken(self) -> str:
        resp = self.request_json(
            "GET",
            "/api/gettemplatevariable",
            params={
                "clienttype": "0",
                "app_id": "38824127",
                "web": "1",
                "fields": '["bdstoken","token","uk","isdocuser","servertime"]',
            },
        )
        errno = int(resp.get("errno", -9999))
        if errno != 0:
            raise ScriptError(f"获取 bdstoken 失败: {baidu_error_message(errno)}")
        token = str((resp.get("result") or {}).get("bdstoken") or "")
        if not token:
            raise ScriptError("获取 bdstoken 失败：响应里没有 bdstoken")
        self.bdstoken = token
        return token

    def get_dir_list(self, folder_path: str) -> list[dict[str, Any]]:
        resp = self.request_json(
            "GET",
            "/api/list",
            params={
                "order": "time",
                "desc": "1",
                "showempty": "0",
                "web": "1",
                "page": "1",
                "num": "1000",
                "dir": normalize_posix_path(folder_path),
                "bdstoken": self.bdstoken,
            },
        )
        errno = int(resp.get("errno", -9999))
        if errno != 0:
            raise ScriptError(f"读取百度目录失败: {baidu_error_message(errno)}")
        items = resp.get("list")
        if not isinstance(items, list):
            raise ScriptError("百度目录列表格式异常")
        return items

    def create_dir(self, folder_path: str) -> None:
        resp = self.request_json(
            "POST",
            "/api/create",
            params={"a": "commit", "bdstoken": self.bdstoken},
            data={
                "path": normalize_posix_path(folder_path),
                "isdir": "1",
                "block_list": "[]",
            },
        )
        errno = int(resp.get("errno", -9999))
        if errno not in (0, -8):
            raise ScriptError(f"创建百度目录失败: {baidu_error_message(errno)}")

    def ensure_dir(self, folder_path: str) -> None:
        normalized = normalize_posix_path(folder_path)
        if normalized == "/":
            return
        try:
            self.get_dir_list(normalized)
            return
        except ScriptError:
            pass
        current = PurePosixPath("/")
        for part in PurePosixPath(normalized).parts[1:]:
            current = current / part
            try:
                self.get_dir_list(str(current))
            except ScriptError:
                self.create_dir(str(current))

    def extract_surl(self, share_url: str) -> str:
        parsed = urllib.parse.urlparse(share_url)
        query = urllib.parse.parse_qs(parsed.query)
        if "surl" in query and query["surl"]:
            value = str(query["surl"][0]).strip()
            return value[1:] if value.startswith("1") else value
        if parsed.path.startswith("/s/"):
            value = urllib.parse.unquote(parsed.path.split("/s/", 1)[1].split("/", 1)[0]).strip()
            if value:
                return value[1:] if value.startswith("1") else value
        raise ScriptError("无法从百度分享链接中提取 surl")

    def verify_pass_code(self, share_url: str, pass_code: str) -> None:
        if not pass_code:
            return
        resp = self.request_json(
            "POST",
            "/share/verify",
            params={
                "surl": self.extract_surl(share_url),
                "bdstoken": self.bdstoken,
                "t": round(time.time() * 1000),
                "channel": "chunlei",
                "web": "1",
                "clienttype": "0",
            },
            data={"pwd": pass_code, "vcode": "", "vcode_str": ""},
        )
        errno = int(resp.get("errno", -9999))
        if errno != 0:
            raise ScriptError(f"提取码校验失败: {baidu_error_message(errno)}")
        randsk = str(resp.get("randsk") or "")
        if not randsk:
            raise ScriptError("提取码校验失败：响应里没有 randsk")
        self.update_cookie_value("BDCLND", randsk)

    def get_transfer_params(self, share_url: str) -> dict[str, Any]:
        response = self.request_text("GET", share_url, timeout=60)
        patterns = {
            "shareid": r'"shareid":(\d+?),"',
            "user_id": r'"share_uk":"(\d+?)",',
            "fs_id": r'"fs_id":(\d+?),"',
            "server_filename": r'"server_filename":"(.+?)",',
            "isdir": r'"isdir":(\d+?),',
        }
        results: dict[str, list[str]] = {}
        for key, pattern in patterns.items():
            results[key] = re.findall(pattern, response)
        if not all(results.get(key) for key in patterns):
            raise ScriptError("无法从百度分享页解析转存参数，可能链接失效、缺提取码或触发风控")
        names = []
        seen = set()
        for raw_name in results["server_filename"]:
            name = bytes(raw_name, "utf-8").decode("unicode_escape", errors="ignore")
            if name not in seen:
                seen.add(name)
                names.append(name)
        return {
            "share_id": results["shareid"][0],
            "user_id": results["user_id"][0],
            "fs_ids": results["fs_id"],
            "file_names": names,
            "is_dirs": results["isdir"],
        }

    def transfer_file(self, share_id: str, user_id: str, fs_ids: list[str], target_dir: str) -> None:
        normalized = normalize_posix_path(target_dir)
        resp = self.request_json(
            "POST",
            "/share/transfer",
            params={
                "shareid": share_id,
                "from": user_id,
                "bdstoken": self.bdstoken,
                "channel": "chunlei",
                "web": "1",
                "clienttype": "0",
                "ondup": "newcopy",
            },
            data={"fsidlist": json.dumps([int(item) for item in fs_ids]), "path": normalized},
            timeout=120,
        )
        errno = int(resp.get("errno", -9999))
        if errno != 0:
            raise ScriptError(f"百度转存失败: {baidu_error_message(errno)}")

    def create_share(self, fs_ids: list[str], period: int = 0, password: str = "1111") -> dict[str, Any]:
        normalized_ids = [int(str(fs_id)) for fs_id in fs_ids if str(fs_id).strip()]
        if not normalized_ids:
            raise ScriptError("Baidu create share needs at least one fs_id.")
        resp = self.request_json(
            "POST",
            "/share/set",
            params={
                "channel": "chunlei",
                "bdstoken": self.bdstoken,
                "clienttype": "0",
                "app_id": "250528",
                "web": "1",
            },
            data={
                "period": str(period),
                "pwd": password,
                "eflag_disable": "true",
                "channel_list": "[]",
                "schannel": "4",
                "fid_list": json.dumps(normalized_ids, ensure_ascii=False),
            },
            timeout=120,
        )
        errno = int(resp.get("errno", -9999))
        if errno != 0:
            raise ScriptError(f"百度创建分享失败: {baidu_error_message(errno)}")
        link = str(resp.get("link") or "")
        if password and link and "pwd=" not in link:
            separator = "&" if "?" in link else "?"
            link = f"{link}{separator}pwd={urllib.parse.quote(password)}"
        resp["link_with_pwd"] = link
        resp["pwd"] = password
        return resp

    def batch_delete_files(self, file_paths: list[str]) -> None:
        valid_paths = [normalize_posix_path(path) for path in file_paths if normalize_posix_path(path) != "/"]
        if not valid_paths:
            return
        resp = self.request_json(
            "POST",
            "/api/filemanager",
            params={
                "async": "2",
                "onnest": "fail",
                "opera": "delete",
                "bdstoken": self.bdstoken,
                "newVerify": "1",
                "clienttype": "0",
                "app_id": "250528",
                "web": "1",
            },
            data={"filelist": json.dumps(valid_paths, ensure_ascii=False)},
            timeout=120,
        )
        errno = int(resp.get("errno", -9999))
        if errno != 0:
            raise ScriptError(f"百度删除失败: {baidu_error_message(errno)}")


def collect_deletions(entries: list[Entry], delete_targets: Iterable[str]) -> list[str]:
    target_set = build_delete_match_set(delete_targets)
    matched = []
    for entry in entries:
        if entry.name in target_set or stem_for_match(entry.name) in target_set:
            matched.append(entry.name)
    return matched


def infer_cleanup_path(
    openlist: OpenListClient,
    transfer_dir: str,
    file_names: list[str],
    discovered_names: list[str],
    retry_seconds: float,
    max_wait_seconds: float,
) -> str:
    openlist.wait_for_path(transfer_dir, retry_seconds=retry_seconds, max_wait_seconds=max_wait_seconds)

    preferred_names = [name for name in discovered_names if str(name).strip()]
    fallback_names = [name for name in file_names if str(name).strip()]
    if len(preferred_names) == 1:
        direct_candidate = normalize_posix_path(f"{transfer_dir}/{preferred_names[0]}")
        try:
            openlist.wait_for_path(
                direct_candidate,
                retry_seconds=retry_seconds,
                max_wait_seconds=max_wait_seconds,
            )
            if openlist.is_dir(direct_candidate):
                return direct_candidate
        except ScriptError:
            pass

    entries = openlist.list_dir(transfer_dir, refresh=True)
    candidate_pool = preferred_names or fallback_names

    candidates = [entry for entry in entries if entry.name in set(candidate_pool)]
    if len(candidates) == 1 and candidates[0].is_dir:
        return normalize_posix_path(f"{transfer_dir}/{candidates[0].name}")
    return normalize_posix_path(transfer_dir)


def wait_for_entries(
    openlist: OpenListClient,
    path: str,
    retry_seconds: float,
    max_wait_seconds: float,
) -> list[Entry]:
    deadline = time.time() + max_wait_seconds
    last_error: Exception | None = None
    while time.time() <= deadline:
        try:
            openlist.list_dir("/", refresh=True)
            openlist.refresh_parent(path)
            return openlist.list_dir(path, refresh=True)
        except ScriptError as exc:
            last_error = exc
            time.sleep(retry_seconds)
    if last_error is not None:
        raise last_error
    return openlist.list_dir(path, refresh=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pure local Baidu transfer, then delete junk items via OpenList."
    )
    parser.add_argument("--share-url", help="Baidu share URL.")
    parser.add_argument("--share-code", default="", help="Baidu extraction code.")
    parser.add_argument(
        "--baidu-cookie",
        default=os.getenv("BAIDU_COOKIE", ""),
        help="Baidu cookie. Can also use BAIDU_COOKIE.",
    )
    parser.add_argument(
        "--openlist-url",
        default=os.getenv("OPENLIST_URL", "http://100.64.0.24:5244/"),
        help="OpenList base URL. Can also use OPENLIST_URL.",
    )
    parser.add_argument(
        "--openlist-token",
        default=os.getenv("OPENLIST_TOKEN", ""),
        help="OpenList token. Can also use OPENLIST_TOKEN.",
    )
    parser.add_argument(
        "--openlist-auth-prefix",
        default=os.getenv("OPENLIST_AUTH_PREFIX", ""),
        help='Optional prefix before token, for example "Bearer ".',
    )
    parser.add_argument(
        "--transfer-dir",
        default=os.getenv("TRANSFER_DIR", "/测试"),
        help="Target transfer directory inside Baidu netdisk.",
    )
    parser.add_argument(
        "--openlist-root-dir",
        default=os.getenv("OPENLIST_ROOT_DIR", "/百度网盘"),
        help="OpenList mount root that corresponds to Baidu netdisk, for example /百度网盘.",
    )
    parser.add_argument(
        "--cleanup-path",
        default=os.getenv("CLEANUP_PATH", ""),
        help="Force cleanup inside this OpenList path instead of auto-detecting.",
    )
    parser.add_argument(
        "--delete-name",
        action="append",
        default=[],
        help="Add one deletion target. Can be repeated.",
    )
    parser.add_argument(
        "--retry-seconds",
        type=float,
        default=2.0,
        help="Polling interval when waiting for OpenList listing.",
    )
    parser.add_argument(
        "--max-wait-seconds",
        type=float,
        default=20.0,
        help="Max wait time for OpenList listing.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print matched deletions, do not actually delete.",
    )
    parser.add_argument(
        "--create-share",
        action="store_true",
        help="Create a new Baidu share link after transfer and cleanup.",
    )
    parser.add_argument(
        "--new-share-code",
        default=os.getenv("BAIDU_NEW_SHARE_CODE", "1111"),
        help="Extraction code for the newly created Baidu share.",
    )
    parser.add_argument(
        "--share-period",
        type=int,
        default=int(os.getenv("BAIDU_SHARE_PERIOD", "0")),
        help="Baidu share period. 0 means permanent.",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if not args.share_url:
        raise ScriptError("Missing --share-url.")
    if not args.baidu_cookie:
        raise ScriptError("Missing --baidu-cookie or BAIDU_COOKIE.")
    if not args.openlist_token:
        raise ScriptError("Missing --openlist-token or OPENLIST_TOKEN.")


def main() -> int:
    args = parse_args()
    try:
        validate_args(args)
        delete_targets = args.delete_name or list(DEFAULT_DELETE_TARGETS)
        baidu_transfer_dir = normalize_posix_path(args.transfer_dir)
        openlist_transfer_dir = map_baidu_path_to_openlist(
            baidu_transfer_dir,
            args.openlist_root_dir,
        )

        openlist = OpenListClient(
            base_url=args.openlist_url,
            token=args.openlist_token,
            auth_prefix=args.openlist_auth_prefix,
        )
        baidu = BaiduPanClient(cookie=args.baidu_cookie)

        baidu.get_bdstoken()
        try:
            transfer_params = baidu.get_transfer_params(args.share_url)
        except ScriptError:
            baidu.verify_pass_code(args.share_url, args.share_code)
            transfer_params = baidu.get_transfer_params(args.share_url)
        baidu.ensure_dir(baidu_transfer_dir)

        before_entries = baidu.get_dir_list(baidu_transfer_dir)
        before_names = {str(item.get("server_filename") or "") for item in before_entries}

        baidu.transfer_file(
            share_id=str(transfer_params["share_id"]),
            user_id=str(transfer_params["user_id"]),
            fs_ids=[str(item) for item in transfer_params["fs_ids"]],
            target_dir=baidu_transfer_dir,
        )
        openlist.wait_for_path(
            openlist_transfer_dir,
            retry_seconds=args.retry_seconds,
            max_wait_seconds=args.max_wait_seconds,
        )

        after_entries = baidu.get_dir_list(baidu_transfer_dir)
        after_names = {str(item.get("server_filename") or "") for item in after_entries}
        new_names = sorted(name for name in after_names if name and name not in before_names)

        cleanup_path = (
            normalize_posix_path(args.cleanup_path)
            if args.cleanup_path
            else infer_cleanup_path(
                openlist,
                openlist_transfer_dir,
                list(transfer_params["file_names"]),
                new_names,
                retry_seconds=args.retry_seconds,
                max_wait_seconds=args.max_wait_seconds,
            )
        )
        entries = wait_for_entries(
            openlist,
            cleanup_path,
            retry_seconds=args.retry_seconds,
            max_wait_seconds=args.max_wait_seconds,
        )
        matched = collect_deletions(entries, delete_targets)
        openlist.remove(cleanup_path, matched, dry_run=args.dry_run)

        shared_fs_ids: list[str] = []
        new_share: dict[str, Any] = {}
        new_share_url = ""
        new_share_code = ""
        if args.create_share and not args.dry_run:
            deleted_top_names = set(matched) if cleanup_path == openlist_transfer_dir else set()
            transferred_entries = [
                item
                for item in after_entries
                if str(item.get("server_filename") or "") in set(new_names)
                and str(item.get("server_filename") or "") not in deleted_top_names
            ]
            shared_fs_ids = [
                str(item.get("fs_id") or "")
                for item in transferred_entries
                if str(item.get("fs_id") or "").strip()
            ]
            if not shared_fs_ids:
                raise ScriptError("Baidu share creation failed: no transferred fs_id found.")
            new_share = baidu.create_share(
                shared_fs_ids,
                period=args.share_period,
                password=args.new_share_code,
            )
            new_share_url = str(new_share.get("link_with_pwd") or new_share.get("link") or "")
            new_share_code = str(new_share.get("pwd") or "")

        result = {
            "baidu_transfer_dir": baidu_transfer_dir,
            "openlist_transfer_dir": openlist_transfer_dir,
            "cleanup_path": cleanup_path,
            "dry_run": args.dry_run,
            "transferred_top_level_names": new_names,
            "parsed_share_file_names": transfer_params["file_names"],
            "delete_targets": delete_targets,
            "matched_deleted_names": matched,
            "create_share": args.create_share,
            "shared_fs_ids": shared_fs_ids,
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
