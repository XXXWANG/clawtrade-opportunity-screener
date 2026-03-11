import argparse
import copy
import errno
import html
import json
import os
import re
import shutil
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import quote, urlencode, urljoin
from urllib.error import URLError
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup
from reporting_runtime import (
    archive_team_report,
    build_screener_team_report,
    detect_event_alerts,
    iso_utc_now,
    list_retry_queue,
    load_json_file,
    remove_retry_queue_entry,
    save_json_file,
    upsert_retry_queue_entry,
    pick_report_profile,
    query_report_history,
    read_report_content,
    update_archived_report_delivery,
)


TCC_CAPTURE_MODE = False
NEUTRAL_SCORE = 50.0
MISSING_METRIC_SCORE = 40.0
FULL_SAMPLE_SIZE = 5
HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    )
}
HKEX_STOCK_LOOKUP_CACHE = {}
HKEX_BOARD_MEETING_CACHE = {}
NEWS_POSITIVE_KEYWORDS = (
    "beat",
    "beats",
    "growth",
    "launch",
    "partner",
    "partnership",
    "approval",
    "record",
    "buyback",
    "dividend",
    "upgrade",
    "expand",
    "strong",
    "rebound",
    "surge",
    "jump",
    "rise",
    "bullish",
    "investment",
)
NEWS_NEGATIVE_KEYWORDS = (
    "warning",
    "lawsuit",
    "probe",
    "investigation",
    "decline",
    "drop",
    "fall",
    "cut",
    "downgrade",
    "delay",
    "suspension",
    "winding up",
    "liquidation",
    "fraud",
    "regulatory",
    "antitrust",
    "miss",
    "sell-off",
    "loss",
    "weak",
)
ANNOUNCEMENT_POSITIVE_KEYWORDS = (
    "final results",
    "interim results",
    "dividend",
    "special dividend",
    "share buyback",
    "business update",
    "voluntary announcement",
    "contract",
    "partnership",
    "strategic",
)
ANNOUNCEMENT_NEGATIVE_KEYWORDS = (
    "profit warning",
    "inside information",
    "delay in publication",
    "suspension",
    "winding up",
    "liquidation",
    "resignation",
    "non-compliance",
    "audit issues",
    "modified report by auditors",
)
ANNOUNCEMENT_NOISE_KEYWORDS = (
    "monthly returns",
    "next day disclosure returns",
    "disclosure of interests",
    "list of directors",
)


class SkillExit(Exception):
    def __init__(self, payload, exit_code):
        super().__init__(payload.get("error") if isinstance(payload, dict) else str(payload))
        self.payload = payload
        self.exit_code = exit_code


@contextmanager
def advisory_file_lock(lock_path):
    lock_path = Path(lock_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(lock_path, "w", encoding="utf-8")
    try:
        if not sys.platform.startswith("win"):
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        if not sys.platform.startswith("win"):
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def json_out(payload, exit_code=0):
    print(json.dumps(payload, ensure_ascii=False))
    if TCC_CAPTURE_MODE:
        raise SkillExit(payload, exit_code)
    sys.exit(exit_code)


def get_execution_owner(task_id=None):
    if os.environ.get("SCREENER_RUN_CONTEXT") == "scheduled":
        return "scheduled"
    if task_id:
        return "tcc"
    return "cli"


def resolve_execution_lock_path():
    target = get_report_root() / "index" / "runtime_execution.lock"
    target.parent.mkdir(parents=True, exist_ok=True)
    return target


@contextmanager
def runtime_execution_lock(command=None, task_id=None):
    lock_commands = {"screen", "auto", "report-retry"}
    if command not in lock_commands:
        yield
        return
    owner = get_execution_owner(task_id)
    lock_path = resolve_execution_lock_path()
    lock_handle = lock_path.open("a+", encoding="utf-8")
    lock_acquired = False
    try:
        if not sys.platform.startswith("win"):
            import fcntl

            flags = fcntl.LOCK_EX
            if owner == "scheduled":
                flags |= fcntl.LOCK_NB
            try:
                fcntl.flock(lock_handle.fileno(), flags)
                lock_acquired = True
            except OSError as exc:
                if owner == "scheduled" and exc.errno in (errno.EACCES, errno.EAGAIN):
                    json_out(
                        {
                            "ok": True,
                            "data": {
                                "skipped": True,
                                "reason": "execution_locked",
                                "command": command,
                                "owner": owner,
                                "lock_path": str(lock_path),
                            },
                        }
                    )
                raise
        yield
    finally:
        if lock_acquired and not sys.platform.startswith("win"):
            import fcntl

            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        lock_handle.close()


def get_python_version(python_cmd):
    try:
        output = subprocess.check_output(
            [python_cmd, "-c", "import sys; print(f'{sys.version_info[0]}.{sys.version_info[1]}')"],
            text=True,
        ).strip()
    except Exception:
        return None
    parts = output.split(".")
    if len(parts) < 2:
        return None
    try:
        return int(parts[0]), int(parts[1])
    except ValueError:
        return None


def is_compatible_python(version_tuple):
    if not version_tuple:
        return False
    major, minor = version_tuple
    return major == 3 and minor in {10, 11, 12}


def select_python():
    current = (sys.version_info[0], sys.version_info[1])
    if is_compatible_python(current):
        return sys.executable
    candidates = ["python3.12", "python3.11", "python3.10", "python3", "python"]
    for candidate in candidates:
        version = get_python_version(candidate)
        if is_compatible_python(version):
            return candidate
    json_out(
        {
            "ok": False,
            "error": "未找到兼容 Python 版本",
            "next_steps": [
                "请安装 Python 3.10/3.11/3.12",
                "安装后重新运行本技能",
            ],
        },
        1,
    )


def ensure_venv():
    if os.environ.get("SCREEN_SKILL_VENV") == "1":
        return
    base_dir = Path(__file__).resolve().parent
    venv_dir = base_dir / ".venv"
    python_path = venv_dir / "bin" / "python"
    pip_path = venv_dir / "bin" / "pip"
    if sys.platform.startswith("win"):
        python_path = venv_dir / "Scripts" / "python.exe"
        pip_path = venv_dir / "Scripts" / "pip.exe"
    python_cmd = select_python()
    lock_path = base_dir / ".venv.lock"
    with advisory_file_lock(lock_path):
        if python_path.exists():
            venv_version = get_python_version(str(python_path))
            if not is_compatible_python(venv_version):
                shutil.rmtree(venv_dir, ignore_errors=True)
        if not python_path.exists():
            subprocess.check_call([python_cmd, "-m", "venv", str(venv_dir)])
        if not pip_path.exists():
            raise RuntimeError("虚拟环境创建失败")
        requirements = base_dir / "requirements.txt"
        if requirements.exists():
            stamp_path = venv_dir / ".requirements_installed"
            stamp_value = str(requirements.stat().st_mtime_ns)
            installed_value = stamp_path.read_text(encoding="utf-8").strip() if stamp_path.exists() else ""
            if installed_value != stamp_value:
                subprocess.check_call([str(pip_path), "install", "-r", str(requirements)])
                stamp_path.write_text(stamp_value, encoding="utf-8")
    env = os.environ.copy()
    env["SCREEN_SKILL_VENV"] = "1"
    subprocess.check_call([str(python_path), str(Path(__file__).resolve()), *sys.argv[1:]], env=env)
    sys.exit(0)


def resolve_futu_skill_path():
    env_path = os.environ.get("FUTU_PAPER_TRADE_DIR") or os.environ.get("FUTU_PAPER_TRADE_PATH")
    script_names = ("clawtrade_futu_skill.py", "futu_skill.py", "xtrade_futu_skill.py")
    base_dir = Path(__file__).resolve().parent
    candidate_dirs = []
    if env_path:
        candidate_dirs.append(Path(env_path).expanduser().resolve())
    candidate_dirs.extend(
        [
            base_dir.parent / "clawtrade-futu-paper-trade",
            base_dir.parent / "Clawtrade-futu-paper-trade_Project",
            base_dir / "clawtrade-futu-paper-trade",
            base_dir / "Clawtrade-futu-paper-trade_Project",
        ]
    )
    for candidate in candidate_dirs:
        if candidate.is_file() and candidate.name in script_names:
            return candidate
        for script_name in script_names:
            script = candidate / script_name
            if script.exists():
                return script
    json_out(
        {
            "ok": False,
            "error": "未找到 clawtrade-futu-paper-trade 技能入口",
            "next_steps": [
                "确认 clawtrade-futu-paper-trade 技能已安装",
                "设置 FUTU_PAPER_TRADE_DIR 指向 clawtrade-futu-paper-trade 目录或脚本路径",
            ],
        },
        1,
    )


def read_json_output(output):
    lines = output.splitlines()
    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        try:
            return json.loads(line)
        except Exception:
            continue
    return None


def call_futu_skill(args):
    script = resolve_futu_skill_path()
    python_cmd = select_python()
    env = os.environ.copy()
    env.setdefault("FUTU_TRD_MARKET", "HK")
    try:
        output = subprocess.check_output(
            [python_cmd, str(script), *args],
            text=True,
            stderr=subprocess.STDOUT,
            env=env,
        )
    except subprocess.CalledProcessError as exc:
        payload = read_json_output(exc.output or "")
        if payload:
            json_out(payload, 1)
        json_out({"ok": False, "error": exc.output.strip() or "clawtrade-futu-paper-trade 调用失败"}, 1)
    payload = read_json_output(output)
    if not payload:
        json_out({"ok": False, "error": "clawtrade-futu-paper-trade 返回内容无法解析"}, 1)
    if not payload.get("ok"):
        json_out(payload, 1)
    return payload


def call_futu_skill_safe(args):
    script = resolve_futu_skill_path()
    python_cmd = select_python()
    env = os.environ.copy()
    env.setdefault("FUTU_TRD_MARKET", "HK")
    try:
        output = subprocess.check_output(
            [python_cmd, str(script), *args],
            text=True,
            stderr=subprocess.STDOUT,
            env=env,
        )
    except subprocess.CalledProcessError as exc:
        payload = read_json_output(exc.output or "")
        if payload:
            return payload
        return {"ok": False, "error": exc.output.strip() or "clawtrade-futu-paper-trade 调用失败"}
    payload = read_json_output(output)
    if not payload:
        return {"ok": False, "error": "clawtrade-futu-paper-trade 返回内容无法解析"}
    return payload


def get_openclaw_workspace_root():
    env_keys = ["OPENCLAW_WORKSPACE", "OPENCLAW_WORKSPACE_DIR", "WORKSPACE_DIR"]
    for key in env_keys:
        value = os.environ.get(key)
        if value:
            return Path(value).expanduser().resolve()
    return Path.home() / ".openclaw" / "workspace"


def resolve_tcc_tasks_file():
    override = os.environ.get("SCREENER_TCC_TASKS_FILE")
    if override:
        return Path(override).expanduser().resolve()
    return get_openclaw_workspace_root() / "skills" / "TCC" / "global_tasks.json"


def resolve_tcc_lock_file(tasks_file):
    override = os.environ.get("SCREENER_TCC_LOCK_FILE")
    if override:
        return Path(override).expanduser().resolve()
    return tasks_file.parent / ".tasks.lock"


@contextmanager
def locked_tcc_registry():
    try:
        import fcntl
    except ImportError as exc:
        raise RuntimeError("当前平台不支持 TCC 文件锁") from exc
    tasks_file = resolve_tcc_tasks_file()
    lock_file = resolve_tcc_lock_file(tasks_file)
    if not tasks_file.exists():
        raise RuntimeError(f"TCC 注册表不存在: {tasks_file}")
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_file, "w", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        try:
            with open(tasks_file, "r", encoding="utf-8") as tasks_handle:
                data = json.load(tasks_handle)
            yield data
            with open(tasks_file, "w", encoding="utf-8") as tasks_handle:
                json.dump(data, tasks_handle, ensure_ascii=False, indent=2)
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)


def get_tcc_task_payload(task_id):
    with locked_tcc_registry() as data:
        for task in data.get("tasks", []):
            if task.get("id") == task_id:
                return dict(task.get("payload") or {})
    raise RuntimeError(f"未找到 TCC 任务: {task_id}")


def update_tcc_task_status(task_id, status, result=None):
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    with locked_tcc_registry() as data:
        for task in data.get("tasks", []):
            if task.get("id") == task_id:
                task["status"] = status
                task["updated_at"] = now
                if result is not None:
                    task["last_result"] = result
                    task["result"] = result
                return
    raise RuntimeError(f"未找到 TCC 任务: {task_id}")


def record_tcc_task_failure(task_id, error_message, result=None):
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    with locked_tcc_registry() as data:
        for task in data.get("tasks", []):
            if task.get("id") == task_id:
                retry_count = int(task.get("retry_count", 0)) + 1
                task["retry_count"] = retry_count
                task.setdefault("attempted_strategies", []).append(
                    {
                        "strategy": "execute",
                        "error": error_message,
                        "timestamp": now,
                    }
                )
                task["updated_at"] = now
                task["status"] = "failed" if retry_count >= int(task.get("max_retries", 5)) else "pending"
                task["last_result"] = result or {"ok": False, "error": error_message}
                return
    raise RuntimeError(f"未找到 TCC 任务: {task_id}")


def try_normalize_hk_symbol(symbol):
    if symbol is None:
        return None
    text = str(symbol).strip().upper()
    if not text:
        return None
    if text.startswith("HK."):
        code = text.split(".", 1)[1].strip()
    elif text.endswith(".HK"):
        code = text.split(".", 1)[0].strip()
    elif "." in text:
        return None
    else:
        code = text
    if not code.isdigit():
        return None
    return "HK." + code.zfill(5)


def normalize_hk_symbol(symbol):
    normalized = try_normalize_hk_symbol(symbol)
    if normalized:
        return normalized
    upper = str(symbol).upper()
    if "." in upper:
        json_out({"ok": False, "error": "仅支持港股代码，格式应为 HK.00700"}, 1)
    json_out({"ok": False, "error": "股票代码必须为港股数字代码，格式应为 HK.00700"}, 1)


def parse_float(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip().replace("%", "")
        if stripped == "" or stripped.upper() == "N/A":
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def clamp(value, low, high):
    return max(low, min(high, value))


def parse_date(value):
    if value is None:
        return None
    text = str(value)
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(text[:19], fmt)
        except Exception:
            continue
    return None


def pick_metric(row, keys):
    for key in keys:
        if key in row:
            return parse_float(row.get(key))
    return None


def extract_latest_row(rows):
    if not rows:
        return None
    date_keys = ["report_date", "report_period", "end_date", "date", "报告期", "报告日期", "截止日期"]
    best = None
    best_dt = None
    for row in rows:
        for key in date_keys:
            if key in row:
                dt = parse_date(row.get(key))
                if dt and (best_dt is None or dt > best_dt):
                    best_dt = dt
                    best = row
    return best or rows[-1]


def extract_prices(rows):
    if not rows:
        return []
    time_keys = ["time_key", "time", "date", "datetime"]
    close_keys = ["close", "close_price", "closePrice", "Close"]
    enriched = []
    for row in rows:
        time_val = None
        for key in time_keys:
            if key in row:
                time_val = parse_date(row.get(key)) or row.get(key)
                break
        close_val = None
        for key in close_keys:
            if key in row:
                close_val = parse_float(row.get(key))
                break
        if close_val is not None:
            enriched.append((time_val, close_val))
    enriched.sort(key=lambda x: x[0] or "")
    return [item[1] for item in enriched]


def fetch_kline(symbol, start, end, ktype, autype, max_count):
    collected = []
    page_key = None
    for _ in range(10):
        args = [
            "historical-kline",
            "--code",
            symbol,
            "--start",
            start,
            "--end",
            end,
            "--ktype",
            ktype,
            "--autype",
            autype,
            "--max-count",
            str(max_count),
        ]
        if page_key:
            args += ["--page-req-key", page_key]
        payload = call_futu_skill(args)
        data = payload.get("data") or []
        collected.extend(data)
        page_key = payload.get("page_req_key")
        if not page_key:
            break
    return collected


def fetch_financial_indicators(symbol, period, start, end):
    args = ["financial-indicators", "--code", symbol, "--period", period]
    if start:
        args += ["--start", start]
    if end:
        args += ["--end", end]
    payload = call_futu_skill(args)
    data = payload.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return []


def fetch_text(url, params=None, timeout=20):
    target = url
    if params:
        target = f"{url}?{urlencode(params)}"
    request = Request(target, headers=HTTP_HEADERS)
    with urlopen(request, timeout=timeout) as response:
        content = response.read()
        charset = response.headers.get_content_charset() or "utf-8"
    return content.decode(charset, errors="ignore")


def parse_jsonp_payload(text):
    start = text.find("(")
    end = text.rfind(")")
    if start == -1 or end == -1 or end <= start:
        return {}
    try:
        return json.loads(text[start + 1 : end])
    except Exception:
        return {}


def normalize_symbol_digits(symbol):
    return normalize_hk_symbol(symbol).split(".", 1)[1]


def safe_parse_datetime(value, formats):
    if not value:
        return None
    text = str(value).strip()
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def utc_now():
    return datetime.now(timezone.utc)


def derive_company_aliases(company_name, fallback_code):
    aliases = []
    for raw in (company_name, fallback_code, str(int(fallback_code)) if fallback_code else None):
        text = str(raw).strip() if raw is not None else ""
        if not text:
            continue
        aliases.append(text)
    upper_name = (str(company_name or "")).upper()
    removable = (" HOLDINGS", " GROUP", " LTD", " LIMITED", " INC", " CORP", "-W", "-SW", "-B")
    for token in removable:
        if upper_name.endswith(token):
            aliases.append(upper_name[: -len(token)].strip())
    cleaned = []
    seen = set()
    for item in aliases:
        key = item.upper()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(item)
    return cleaned


def text_mentions_company(text, aliases, symbol_digits):
    haystack = (text or "").upper()
    if not haystack:
        return False
    if symbol_digits and symbol_digits in haystack:
        return True
    if symbol_digits:
        short_code = str(int(symbol_digits))
        if f"SEHK:{short_code}" in haystack or f"HK:{short_code}" in haystack:
            return True
    for alias in aliases:
        alias_upper = alias.upper().strip()
        if not alias_upper:
            continue
        if alias_upper in haystack:
            return True
    return False


def classify_text_signal(text, positive_keywords, negative_keywords):
    lowered = (text or "").lower()
    if not lowered:
        return "neutral"
    negative_hits = sum(1 for item in negative_keywords if item in lowered)
    positive_hits = sum(1 for item in positive_keywords if item in lowered)
    if negative_hits > positive_hits and negative_hits > 0:
        return "negative"
    if positive_hits > negative_hits and positive_hits > 0:
        return "positive"
    return "neutral"


def fetch_hkex_stock_lookup(symbol, company_name=None):
    symbol_digits = normalize_symbol_digits(symbol)
    if symbol_digits in HKEX_STOCK_LOOKUP_CACHE:
        return HKEX_STOCK_LOOKUP_CACHE[symbol_digits]
    queries = [
        str(int(symbol_digits)),
        company_name,
        f"{int(symbol_digits)} {company_name}" if company_name else None,
    ]
    for query in queries:
        if not query:
            continue
        try:
            text = fetch_text(
                "https://www1.hkexnews.hk/search/prefix.do",
                params={
                    "callback": "callback",
                    "lang": "EN",
                    "type": "A",
                    "name": query,
                    "market": "SEHK",
                },
            )
        except Exception:
            continue
        payload = parse_jsonp_payload(text)
        for item in payload.get("stockInfo") or []:
            if str(item.get("code", "")).zfill(5) == symbol_digits:
                lookup = {
                    "stock_id": item.get("stockId"),
                    "code": str(item.get("code", "")).zfill(5),
                    "name": item.get("name"),
                }
                HKEX_STOCK_LOOKUP_CACHE[symbol_digits] = lookup
                return lookup
    lookup = {"stock_id": None, "code": symbol_digits, "name": company_name}
    HKEX_STOCK_LOOKUP_CACHE[symbol_digits] = lookup
    return lookup


def score_news_flow(items):
    if not items:
        return None
    positive_count = len([item for item in items if item.get("signal") == "positive"])
    negative_count = len([item for item in items if item.get("signal") == "negative"])
    score = 50.0 + min(positive_count, 3) * 8.0 - min(negative_count, 3) * 10.0
    if positive_count > negative_count and len(items) >= 3:
        score += 4.0
    return clamp(score, 0.0, 100.0)


def score_announcement_flow(items):
    if not items:
        return None
    positive_count = len([item for item in items if item.get("signal") == "positive"])
    negative_count = len([item for item in items if item.get("signal") == "negative"])
    score = 50.0 + min(positive_count, 2) * 10.0 - min(negative_count, 3) * 12.0
    return clamp(score, 0.0, 100.0)


def score_event_window(next_in_days):
    if next_in_days is None:
        return 50.0
    if next_in_days <= 3:
        return 20.0
    if next_in_days <= 7:
        return 30.0
    if next_in_days <= 14:
        return 40.0
    if next_in_days <= 30:
        return 48.0
    if next_in_days <= 60:
        return 55.0
    return 60.0


def fetch_google_news(symbol, company_name, lookback_days, limit):
    symbol_digits = normalize_symbol_digits(symbol)
    aliases = derive_company_aliases(company_name, symbol_digits)
    query = f"\"{aliases[0]}\" OR \"{int(symbol_digits)} HK\" when:{int(lookback_days)}d"
    url = (
        "https://news.google.com/rss/search?q="
        f"{quote(query)}&hl=en-US&gl=US&ceid=US:en"
    )
    try:
        text = fetch_text(url)
        root = ET.fromstring(text)
    except Exception:
        return {"available": False, "items": [], "score": None}
    cutoff = utc_now().replace(tzinfo=None) - timedelta(days=int(lookback_days))
    items = []
    for node in root.findall("./channel/item"):
        title = html.unescape(node.findtext("title") or "")
        description = html.unescape(node.findtext("description") or "")
        combined = f"{title} {description}".strip()
        if not text_mentions_company(combined, aliases, symbol_digits):
            continue
        published_at = None
        try:
            published_at = parsedate_to_datetime(node.findtext("pubDate") or "")
        except Exception:
            published_at = None
        if published_at and published_at.replace(tzinfo=None) < cutoff:
            continue
        items.append(
            {
                "title": title,
                "link": node.findtext("link"),
                "published_at": published_at.isoformat() if published_at else None,
                "signal": classify_text_signal(combined, NEWS_POSITIVE_KEYWORDS, NEWS_NEGATIVE_KEYWORDS),
            }
        )
    items = items[: int(limit)]
    return {
        "available": True,
        "items": items,
        "score": score_news_flow(items),
        "positive_count": len([item for item in items if item.get("signal") == "positive"]),
        "negative_count": len([item for item in items if item.get("signal") == "negative"]),
    }


def parse_hkex_announcement_row(row, symbol_digits):
    release_node = row.select_one("td.release-time")
    code_node = row.select_one("td.stock-short-code")
    name_node = row.select_one("td.stock-short-name")
    headline_node = row.select_one("div.headline")
    link_node = row.select_one("div.doc-link a")
    if not release_node or not code_node or not headline_node or not link_node:
        return None
    codes = [item.strip() for item in code_node.stripped_strings if item.strip() and "Stock Code" not in item]
    if symbol_digits not in [str(item).zfill(5) for item in codes]:
        return None
    release_text = " ".join(item.strip() for item in release_node.stripped_strings if "Release Time" not in item)
    release_dt = safe_parse_datetime(release_text, ("%d/%m/%Y %H:%M",))
    category = " ".join(headline_node.stripped_strings)
    title = " ".join(link_node.stripped_strings)
    combined = f"{category} {title}"
    signal = classify_text_signal(combined, ANNOUNCEMENT_POSITIVE_KEYWORDS, ANNOUNCEMENT_NEGATIVE_KEYWORDS)
    significance = 1
    if any(item in combined.lower() for item in ANNOUNCEMENT_NOISE_KEYWORDS):
        significance = 0
    if signal != "neutral":
        significance = 2
    return {
        "release_time": release_dt.isoformat() if release_dt else None,
        "codes": [str(item).zfill(5) for item in codes],
        "names": [item.strip() for item in name_node.stripped_strings if item.strip() and "Stock Short Name" not in item],
        "category": category,
        "title": title,
        "link": urljoin("https://www1.hkexnews.hk", link_node.get("href", "")),
        "signal": signal,
        "significance": significance,
    }


def fetch_hkex_announcements(symbol, company_name, lookback_days, limit):
    lookup = fetch_hkex_stock_lookup(symbol, company_name)
    stock_id = lookup.get("stock_id")
    if not stock_id:
        return {"available": False, "items": [], "score": None, "lookup": lookup}
    try:
        text = fetch_text(
            "https://www1.hkexnews.hk/search/titlesearch.xhtml",
            params={"lang": "EN", "category": "0", "market": "SEHK", "stockId": stock_id},
        )
    except Exception:
        return {"available": False, "items": [], "score": None, "lookup": lookup}
    symbol_digits = normalize_symbol_digits(symbol)
    cutoff = utc_now().replace(tzinfo=None) - timedelta(days=int(lookback_days))
    soup = BeautifulSoup(text, "html.parser")
    records = []
    for row in soup.select("main tr"):
        item = parse_hkex_announcement_row(row, symbol_digits)
        if not item:
            continue
        release_dt = parse_date(item.get("release_time")) or safe_parse_datetime(item.get("release_time"), ("%Y-%m-%dT%H:%M:%S",))
        if release_dt and isinstance(release_dt, datetime) and release_dt < cutoff:
            continue
        records.append(item)
    records.sort(key=lambda item: item.get("release_time") or "", reverse=True)
    filtered = [item for item in records if item.get("significance", 0) > 0] or records
    filtered = filtered[: int(limit)]
    return {
        "available": True,
        "lookup": lookup,
        "items": filtered,
        "score": score_announcement_flow(filtered),
        "positive_count": len([item for item in filtered if item.get("signal") == "positive"]),
        "negative_count": len([item for item in filtered if item.get("signal") == "negative"]),
    }


def get_hkex_board_meeting_records(board="main"):
    board_key = "main" if board != "gem" else "gem"
    today_key = utc_now().strftime("%Y-%m-%d")
    cache_key = f"{board_key}:{today_key}"
    if cache_key in HKEX_BOARD_MEETING_CACHE:
        return HKEX_BOARD_MEETING_CACHE[cache_key]
    url = "https://www3.hkexnews.hk/reports/bmn/ebmn.htm"
    if board_key == "gem":
        url = "https://www3.hkexnews.hk/reports/bmn/ebmngem.htm"
    text = fetch_text(url)
    soup = BeautifulSoup(text, "html.parser")
    records = []
    for row in soup.select("table.textfont tr")[2:]:
        parts = [item.strip() for item in row.stripped_strings if item.strip()]
        if len(parts) < 5:
            continue
        bm_dt = safe_parse_datetime(parts[0], ("%d/%m/%Y",))
        if not bm_dt:
            continue
        records.append(
            {
                "bm_date": bm_dt.strftime("%Y-%m-%d"),
                "stock_short_name": parts[1],
                "code": str(parts[2]).zfill(5),
                "purpose": parts[3],
                "period": parts[4],
                "board": board_key.upper(),
            }
        )
    HKEX_BOARD_MEETING_CACHE[cache_key] = records
    return records


def fetch_hkex_earnings_calendar(symbol, lookahead_days, limit):
    symbol_digits = normalize_symbol_digits(symbol)
    now = utc_now().date()
    cutoff = now + timedelta(days=int(lookahead_days))
    matched = []
    try:
        for board in ("main", "gem"):
            for item in get_hkex_board_meeting_records(board):
                if item.get("code") != symbol_digits:
                    continue
                bm_date = safe_parse_datetime(item.get("bm_date"), ("%Y-%m-%d",))
                if not bm_date:
                    continue
                if bm_date.date() < now or bm_date.date() > cutoff:
                    continue
                matched.append(
                    {
                        **item,
                        "days_until": (bm_date.date() - now).days,
                    }
                )
    except Exception:
        return {"available": False, "items": [], "next_in_days": None, "score": None}
    matched.sort(key=lambda item: item.get("bm_date"))
    next_in_days = matched[0]["days_until"] if matched else None
    return {
        "available": True,
        "items": matched[: int(limit)],
        "next_in_days": next_in_days,
        "score": score_event_window(next_in_days),
    }


def build_information_snapshot(symbol, company_name, args):
    news = fetch_google_news(
        symbol,
        company_name,
        getattr(args, "news_lookback_days", 14),
        getattr(args, "news_limit", 5),
    )
    announcements = fetch_hkex_announcements(
        symbol,
        company_name,
        getattr(args, "announcement_lookback_days", 30),
        getattr(args, "announcement_limit", 6),
    )
    earnings_calendar = fetch_hkex_earnings_calendar(
        symbol,
        getattr(args, "earnings_lookahead_days", 120),
        3,
    )
    sub_scores = [item for item in (news.get("score"), announcements.get("score"), earnings_calendar.get("score")) if item is not None]
    overall_score = round(
        (news.get("score", 50.0) or 50.0) * 0.4
        + (announcements.get("score", 50.0) or 50.0) * 0.35
        + (earnings_calendar.get("score", 50.0) or 50.0) * 0.25,
        2,
    ) if sub_scores else None
    risk_flags = []
    next_in_days = earnings_calendar.get("next_in_days")
    if next_in_days is not None and next_in_days <= 7:
        risk_flags.append("earnings_window_7d")
    if (announcements.get("negative_count") or 0) > 0:
        risk_flags.append("negative_announcements")
    if (news.get("negative_count") or 0) > (news.get("positive_count") or 0):
        risk_flags.append("negative_news_flow")
    return {
        "available": bool(sub_scores),
        "score": overall_score,
        "risk_flags": risk_flags,
        "news": news,
        "announcements": announcements,
        "earnings_calendar": earnings_calendar,
    }


def compute_returns(prices, days):
    if prices is None or len(prices) <= days:
        return None
    start = prices[-days - 1]
    end = prices[-1]
    if start == 0:
        return None
    return (end - start) / start


def compute_volatility(prices):
    if prices is None or len(prices) < 2:
        return None
    returns = []
    for i in range(1, len(prices)):
        prev = prices[i - 1]
        if prev == 0:
            continue
        returns.append((prices[i] - prev) / prev)
    if len(returns) < 2:
        return None
    mean = sum(returns) / len(returns)
    var = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
    return (var ** 0.5) * (252 ** 0.5)


def compute_max_drawdown(prices):
    if prices is None or len(prices) < 2:
        return None
    peak = prices[0]
    max_dd = 0.0
    for price in prices:
        if price > peak:
            peak = price
        if peak == 0:
            continue
        drawdown = (peak - price) / peak
        if drawdown > max_dd:
            max_dd = drawdown
    return max_dd


def compute_monthly_volatility(annualized_vol):
    if annualized_vol is None:
        return None
    return annualized_vol / (12 ** 0.5)


def shrink_score_for_sample(raw_score, sample_size):
    if raw_score is None:
        return None
    if sample_size <= 1:
        return NEUTRAL_SCORE
    confidence = clamp((sample_size - 1) / max(FULL_SAMPLE_SIZE - 1, 1), 0.0, 1.0)
    return NEUTRAL_SCORE + (raw_score - NEUTRAL_SCORE) * confidence


def percentile_scores(values, higher_better=True):
    valid = [(idx, v) for idx, v in enumerate(values) if v is not None]
    scores = [None] * len(values)
    if not valid:
        return scores
    sorted_vals = sorted(valid, key=lambda x: x[1])
    n = len(sorted_vals)
    for rank, (idx, _) in enumerate(sorted_vals):
        score = rank / (n - 1) if n > 1 else 1.0
        if not higher_better:
            score = 1.0 - score
        scores[idx] = shrink_score_for_sample(score * 100, n)
    return scores


def score_factor(rows, metric_keys, higher_better_map):
    metrics = {key: [] for key in metric_keys}
    for row in rows:
        for key in metric_keys:
            metrics[key].append(row.get(key))
    scored = {key: percentile_scores(metrics[key], higher_better_map.get(key, True)) for key in metric_keys}
    factor_scores = []
    factor_meta = []
    for idx in range(len(rows)):
        per_metric = []
        valid_metric_count = 0
        for key in metric_keys:
            value = scored[key][idx]
            if value is not None:
                valid_metric_count += 1
                per_metric.append(value)
            else:
                per_metric.append(MISSING_METRIC_SCORE)
        if per_metric:
            factor_scores.append(sum(per_metric) / len(per_metric))
        else:
            factor_scores.append(None)
        factor_meta.append(
            {
                "valid_metrics": valid_metric_count,
                "total_metrics": len(metric_keys),
                "coverage": valid_metric_count / len(metric_keys) if metric_keys else 0.0,
            }
        )
    return factor_scores, scored, factor_meta


def parse_weights(weights_str):
    if not weights_str:
        return None
    try:
        data = json.loads(weights_str)
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    return {k: float(v) for k, v in data.items()}


def parse_thresholds(thresholds_str):
    if not thresholds_str:
        return None
    try:
        data = json.loads(thresholds_str)
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    return {k: float(v) for k, v in data.items()}


def parse_json_map(map_str):
    if not map_str:
        return None
    try:
        data = json.loads(map_str)
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    return data


def parse_stage_list(stage_str):
    if not stage_str:
        return []
    return [item.strip() for item in stage_str.split(",") if item.strip()]


def parse_symbol_list(text):
    if not text:
        return []
    tokens = []
    for raw in text.replace(",", " ").split():
        value = raw.strip()
        if value:
            tokens.append(value)
    return tokens


def parse_hhmm(value, default_value):
    if not value:
        value = default_value
    text = str(value).strip()
    if ":" not in text:
        json_out({"ok": False, "error": f"时间格式错误: {value}, 需要 HH:MM"}, 1)
    hour_str, minute_str = text.split(":", 1)
    try:
        hour = int(hour_str)
        minute = int(minute_str)
    except ValueError:
        json_out({"ok": False, "error": f"时间格式错误: {value}, 需要 HH:MM"}, 1)
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        json_out({"ok": False, "error": f"时间范围错误: {value}"}, 1)
    return f"{hour:02d}:{minute:02d}", hour * 60 + minute


def parse_interval_minutes(value, default_value):
    raw = value if value is not None else default_value
    try:
        minutes = int(raw)
    except ValueError:
        json_out({"ok": False, "error": f"间隔格式错误: {raw}，需要整数分钟"}, 1)
    if minutes <= 0:
        json_out({"ok": False, "error": f"间隔必须大于 0: {raw}"}, 1)
    return minutes


def parse_schedule_days(value):
    if not value:
        return ["mon", "tue", "wed", "thu", "fri"]
    tokens = [item.strip().lower() for item in str(value).replace(",", " ").split() if item.strip()]
    return tokens or ["mon", "tue", "wed", "thu", "fri"]


def resolve_schedule_preferences_path():
    target = get_report_root() / "config" / "report_schedule_preferences.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    return target


def build_default_schedule_preferences():
    return {
        "version": "v1",
        "profile": "default",
        "scheduler_mode": "openclaw_primary",
        "enabled_reports": [
            "pre_open",
            "pre_trade",
            "midday",
            "pre_close_risk",
            "post_close",
            "night_review",
        ],
        "report_times": {},
        "retry_maintenance": {
            "enabled": True,
            "start": "08:35",
            "end": "21:05",
            "interval_minutes": 5,
            "limit": 10,
        },
    }


def load_schedule_preferences():
    payload = load_json_file(resolve_schedule_preferences_path(), build_default_schedule_preferences())
    defaults = build_default_schedule_preferences()
    merged = dict(defaults)
    merged.update(payload or {})
    merged["scheduler_mode"] = payload.get("scheduler_mode") if isinstance(payload, dict) and payload.get("scheduler_mode") else defaults["scheduler_mode"]
    merged["enabled_reports"] = payload.get("enabled_reports") if isinstance(payload, dict) and payload.get("enabled_reports") else defaults["enabled_reports"]
    merged["report_times"] = payload.get("report_times") if isinstance(payload, dict) and isinstance(payload.get("report_times"), dict) else {}
    retry_payload = payload.get("retry_maintenance") if isinstance(payload, dict) and isinstance(payload.get("retry_maintenance"), dict) else {}
    retry_defaults = defaults["retry_maintenance"]
    merged["retry_maintenance"] = {
        "enabled": bool(retry_payload.get("enabled", retry_defaults["enabled"])),
        "start": retry_payload.get("start") or retry_defaults["start"],
        "end": retry_payload.get("end") or retry_defaults["end"],
        "interval_minutes": int(retry_payload.get("interval_minutes") or retry_defaults["interval_minutes"]),
        "limit": int(retry_payload.get("limit") or retry_defaults["limit"]),
    }
    return merged


def save_schedule_preferences(preferences):
    payload = build_default_schedule_preferences()
    payload.update(preferences or {})
    save_json_file(resolve_schedule_preferences_path(), payload)
    return payload


def build_schedule_profile(profile_name):
    defaults = build_default_schedule_preferences()
    profile = (profile_name or "default").lower()
    if profile == "compact":
        defaults["enabled_reports"] = ["pre_open", "midday", "post_close", "night_review"]
    elif profile == "minimal":
        defaults["enabled_reports"] = ["post_close", "night_review"]
    defaults["profile"] = profile
    return defaults


def parse_report_time_map(value):
    raw = parse_json_map(value)
    if not raw:
        return {}
    parsed = {}
    for key, item in raw.items():
        text, _minutes = parse_hhmm(item, item)
        parsed[str(key)] = text
    return parsed


def build_fixed_report_catalog():
    return [
        {
            "time": "08:30",
            "report_type": "pre_open",
            "title": "盘前总览",
            "command": "python3 {baseDir}/screener_skill.py run-scheduled --job pre_open",
        },
        {
            "time": "09:15",
            "report_type": "pre_trade",
            "title": "开盘前策略结论",
            "command": "python3 {baseDir}/screener_skill.py run-scheduled --job pre_trade",
        },
        {
            "time": "11:50",
            "report_type": "midday",
            "title": "午间进度报告",
            "command": "python3 {baseDir}/screener_skill.py run-scheduled --job midday",
        },
        {
            "time": "15:30",
            "report_type": "pre_close_risk",
            "title": "收盘前风险检查",
            "command": "python3 {baseDir}/screener_skill.py run-scheduled --job pre_close_risk",
        },
        {
            "time": "16:30",
            "report_type": "post_close",
            "title": "收盘后执行总结",
            "command": "python3 {baseDir}/screener_skill.py run-scheduled --job post_close",
        },
        {
            "time": "20:30",
            "report_type": "night_review",
            "title": "夜间复盘",
            "command": "python3 {baseDir}/screener_skill.py run-scheduled --job night_review",
        },
    ]


def build_report_alias_map():
    return {
        "pre_open": ["pre_open", "盘前总览", "盘前", "盘前报告", "盘前汇报"],
        "pre_trade": ["pre_trade", "开盘前策略结论", "开盘前", "开盘前报告", "开盘前汇报", "策略结论"],
        "midday": ["midday", "午间进度报告", "午间", "午间报告", "午盘", "中午报告"],
        "pre_close_risk": ["pre_close_risk", "收盘前风险检查", "收盘前", "收盘前风险", "尾盘风险", "风险检查"],
        "post_close": ["post_close", "收盘后执行总结", "收盘后", "收盘总结", "收盘报告", "执行总结"],
        "night_review": ["night_review", "夜间复盘", "夜间", "晚间复盘", "复盘", "夜盘复盘"],
    }


def alias_to_pattern(alias):
    if alias == "盘前":
        return r"(?<!收)盘前"
    return re.escape(alias)


def detect_report_types_in_text(text):
    matched = []
    source = text or ""
    lowered = source.lower()
    for report_type, aliases in build_report_alias_map().items():
        for alias in aliases:
            alias_lower = alias.lower()
            pattern = alias_to_pattern(alias) if any(ord(ch) > 127 for ch in alias) else re.escape(alias_lower)
            if alias_lower and re.search(pattern, source if any(ord(ch) > 127 for ch in alias) else lowered, re.IGNORECASE):
                matched.append(report_type)
                break
    return matched


def infer_schedule_profile_from_message(text):
    lowered = (text or "").lower()
    if any(token in lowered for token in ["恢复默认", "重置默认", "默认设置", "默认汇报", "reset default"]):
        return "default"
    if any(token in lowered for token in ["精简版", "简版", "compact"]):
        return "compact"
    if any(token in lowered for token in ["最小版", "极简版", "minimal"]):
        return "minimal"
    return None


def infer_scheduler_mode_from_message(text):
    lowered = (text or "").lower()
    if "cron" in lowered and any(token in lowered for token in ["主", "优先", "primary"]):
        return "cron_primary"
    if any(token in lowered for token in ["openclaw", "automation"]) and any(token in lowered for token in ["主", "优先", "primary"]):
        return "openclaw_primary"
    return None


def infer_time_updates_from_message(text):
    updates = {}
    if not text:
        return updates
    for report_type, aliases in build_report_alias_map().items():
        alias_pattern = "|".join(alias_to_pattern(alias) for alias in aliases)
        pattern = re.compile(rf"(?:{alias_pattern})(?:报告|汇报)?(?:时间)?(?:改到|调整到|改成|调整为|推迟到|提前到|改为)\s*(\d{{1,2}}:\d{{2}})", re.IGNORECASE)
        match = pattern.search(text)
        if match:
            updates[report_type] = parse_hhmm(match.group(1), match.group(1))[0]
    return updates


def build_schedule_preference_snapshot(preferences):
    schedule_spec = build_schedule_spec(preferences_override=preferences)
    default_catalog = {item["report_type"]: item for item in build_fixed_report_catalog()}
    active_reports = []
    scheduler_mode = (preferences.get("scheduler_mode") or "openclaw_primary").lower()
    for item in schedule_spec.get("fixed_reports") or []:
        active_reports.append(
            {
                "report_type": item["report_type"],
                "title": item["title"],
                "time": item["time"],
                "user_customized": bool(item.get("user_customized")),
                "delivery_surface": "OpenClaw automation 卡片" if scheduler_mode == "openclaw_primary" else "系统 cron 任务",
            }
        )
    active_report_types = {item["report_type"] for item in schedule_spec.get("fixed_reports") or []}
    disabled_reports = []
    for report_type, item in default_catalog.items():
        if report_type in active_report_types:
            continue
        disabled_reports.append(
            {
                "report_type": report_type,
                "title": item["title"],
                "default_time": item["time"],
            }
        )
    maintenance_jobs = []
    for item in schedule_spec.get("maintenance_jobs") or []:
        maintenance_jobs.append(
            {
                "job_id": item["task"],
                "title": item["title"],
                "window": item["window"],
                "interval_minutes": item["interval_minutes"],
                "delivery_surface": "系统 cron fallback",
            }
        )
    automation_plan, unsupported = build_openclaw_automation_payload(schedule_spec)
    return schedule_spec, {
        "preferences_path": str(resolve_schedule_preferences_path()),
        "schedule_path": str(resolve_schedule_path()),
        "profile": preferences.get("profile") or "default",
        "scheduler_mode": scheduler_mode,
        "user_touchpoints": {
            "primary": "用户先在对话里提出汇报时间或频率要求，再由主 agent 更新偏好并生成 OpenClaw automation 更新建议",
            "steps": [
                "用户直接对主 agent 说“调整汇报时间”或“关闭某个时段报告”",
                "主 agent 更新 report_schedule_preferences.json",
                "主 agent 生成或更新 OpenClaw automation 卡片，供用户确认",
                "固定报告按 OpenClaw automation 生效；高频 delivery_retry 继续走 cron fallback",
            ],
            "has_dedicated_settings_page": False,
        },
        "active_reports": active_reports,
        "disabled_reports": disabled_reports,
        "maintenance_jobs": maintenance_jobs,
        "automation_plan": automation_plan,
        "unsupported_jobs": unsupported,
    }


def persist_schedule_spec(spec=None):
    path = resolve_schedule_path()
    payload = spec or build_schedule_spec()
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return payload, path


def apply_natural_language_schedule_request(message, dry_run=False):
    if not message or not str(message).strip():
        json_out({"ok": False, "error": "缺少 --message，无法解析用户的汇报设置要求"}, 1)
    message = str(message).strip()
    preferences = load_schedule_preferences()
    original_preferences = copy.deepcopy(preferences)
    changes = []
    notes = []
    profile = infer_schedule_profile_from_message(message)
    if profile:
        preferences = build_schedule_profile(profile)
        preferences["scheduler_mode"] = original_preferences.get("scheduler_mode", preferences.get("scheduler_mode"))
        changes.append({"type": "profile", "value": profile})
    scheduler_mode = infer_scheduler_mode_from_message(message)
    if scheduler_mode:
        preferences["scheduler_mode"] = scheduler_mode
        changes.append({"type": "scheduler_mode", "value": scheduler_mode})
    lowered = message.lower()
    if "event_alert" in lowered or "事件快报" in message:
        notes.append("event_alert 为事件驱动快报，不属于固定时点报告，本次仅调整固定报告与 retry 维护任务。")
    if any(token in lowered for token in ["恢复默认", "重置默认", "默认设置", "reset default"]):
        preferences = build_default_schedule_preferences()
        changes.append({"type": "reset", "value": "default"})
    selected_reports = detect_report_types_in_text(message)
    if any(token in message for token in ["只保留", "只要"]) and selected_reports:
        preferences["enabled_reports"] = sorted(set(selected_reports))
        changes.append({"type": "set_enabled_reports", "value": preferences["enabled_reports"]})
    else:
        enabled_reports = set(preferences.get("enabled_reports") or [])
        if any(token in message for token in ["关闭", "不要", "取消", "停掉", "去掉"]) and selected_reports:
            enabled_reports = {item for item in enabled_reports if item not in set(selected_reports)}
            changes.append({"type": "disable_reports", "value": sorted(set(selected_reports))})
        if any(token in message for token in ["开启", "打开", "恢复"]) and selected_reports:
            enabled_reports.update(selected_reports)
            changes.append({"type": "enable_reports", "value": sorted(set(selected_reports))})
        if enabled_reports:
            preferences["enabled_reports"] = sorted(enabled_reports)
    time_updates = infer_time_updates_from_message(message)
    if time_updates:
        report_times = dict(preferences.get("report_times") or {})
        report_times.update(time_updates)
        preferences["report_times"] = report_times
        changes.append({"type": "time_updates", "value": time_updates})
    if any(token in message for token in ["关闭补发", "关闭重试", "停掉补发", "停掉重试"]):
        retry_preferences = dict(preferences.get("retry_maintenance") or {})
        retry_preferences["enabled"] = False
        preferences["retry_maintenance"] = retry_preferences
        changes.append({"type": "retry_maintenance", "value": "disabled"})
    if any(token in message for token in ["开启补发", "开启重试", "打开补发", "打开重试"]):
        retry_preferences = dict(preferences.get("retry_maintenance") or {})
        retry_preferences["enabled"] = True
        preferences["retry_maintenance"] = retry_preferences
        changes.append({"type": "retry_maintenance", "value": "enabled"})
    if not changes:
        json_out(
            {
                "ok": False,
                "error": "未识别到可执行的汇报设置变更，请明确说出要关闭哪些报告、保留哪些报告或把哪个时段改到几点。",
                "examples": [
                    "关闭午间报告和收盘前风险检查",
                    "只保留盘前和收盘后两份",
                    "把夜间复盘改到21:30",
                    "改成精简版",
                ],
            },
            1,
        )
    if not dry_run:
        preferences = save_schedule_preferences(preferences)
        schedule_spec, schedule_path = persist_schedule_spec(build_schedule_spec(preferences_override=preferences))
    else:
        schedule_spec = build_schedule_spec(preferences_override=preferences)
        schedule_path = resolve_schedule_path()
    _schedule_spec, summary = build_schedule_preference_snapshot(preferences)
    summary["request_message"] = message
    summary["changes"] = changes
    summary["notes"] = notes
    summary["dry_run"] = bool(dry_run)
    summary["schedule_path"] = str(schedule_path)
    summary["preferences_before"] = original_preferences
    summary["preferences_after"] = preferences
    return summary


def build_schedule_times(start_minutes, end_minutes, interval_minutes):
    if end_minutes < start_minutes:
        json_out({"ok": False, "error": "结束时间不能早于开始时间"}, 1)
    times = []
    current = start_minutes
    while current <= end_minutes:
        hour = current // 60
        minute = current % 60
        times.append(f"{hour:02d}:{minute:02d}")
        current += interval_minutes
    return times


def build_schedule_spec(args=None, preferences_override=None):
    preferences = preferences_override or load_schedule_preferences()
    start_value = getattr(args, "schedule_start", None) if args else None
    end_value = getattr(args, "schedule_end", None) if args else None
    interval_value = getattr(args, "schedule_interval", None) if args else None
    retry_prefs = preferences.get("retry_maintenance") or {}
    retry_start_value = getattr(args, "retry_schedule_start", None) if args else None
    retry_end_value = getattr(args, "retry_schedule_end", None) if args else None
    retry_interval_value = getattr(args, "retry_schedule_interval", None) if args else None
    retry_limit = int(getattr(args, "retry_limit", retry_prefs.get("limit", 10)) if args else retry_prefs.get("limit", 10))
    timezone = getattr(args, "schedule_timezone", None) if args else None
    days = parse_schedule_days(getattr(args, "schedule_days", None) if args else None)
    start_text, start_minutes = parse_hhmm(start_value, "08:30")
    end_text, end_minutes = parse_hhmm(end_value, "17:30")
    interval = parse_interval_minutes(interval_value, 30)
    retry_start_text, retry_start_minutes = parse_hhmm(retry_start_value, retry_prefs.get("start", "08:35"))
    retry_end_text, retry_end_minutes = parse_hhmm(retry_end_value, retry_prefs.get("end", "21:05"))
    retry_interval = parse_interval_minutes(retry_interval_value, retry_prefs.get("interval_minutes", 5))
    timezone = timezone or "Asia/Hong_Kong"
    times = build_schedule_times(start_minutes, end_minutes, interval)
    retry_times = build_schedule_times(retry_start_minutes, retry_end_minutes, retry_interval)
    command = getattr(args, "schedule_command", None) if args else None
    if not command:
        command = "python3 {baseDir}/screener_skill.py auto"
    fixed_reports = build_fixed_report_catalog()
    enabled_reports = set(preferences.get("enabled_reports") or [])
    report_time_overrides = preferences.get("report_times") or {}
    filtered_reports = []
    for item in fixed_reports:
        report_type = item["report_type"]
        if enabled_reports and report_type not in enabled_reports:
            continue
        overridden_time = report_time_overrides.get(report_type)
        if overridden_time:
            item = {**item, "time": overridden_time}
        item = {
            **item,
            "user_customized": bool(overridden_time or report_type not in build_default_schedule_preferences()["enabled_reports"]),
        }
        filtered_reports.append(item)
    fixed_reports = filtered_reports
    event_alerts = [
        "新开仓、清仓、止损、止盈",
        "单票仓位显著变化",
        "重大公告、重大新闻、监管风险",
        "财报窗口进入近7天",
        "组合回撤超阈值",
        "执行失败、数据异常、系统故障",
    ]
    maintenance_jobs = []
    retry_enabled = retry_prefs.get("enabled", True)
    if getattr(args, "disable_retry_maintenance", False) if args else False:
        retry_enabled = False
    if retry_enabled:
        maintenance_jobs.append(
            {
                "task": "delivery_retry",
                "title": "报告补发重试",
                "days": days,
                "window": {"start": retry_start_text, "end": retry_end_text},
                "interval_minutes": retry_interval,
                "times": retry_times,
                "command": "python3 {baseDir}/screener_skill.py run-scheduled --job delivery_retry",
                "limit": retry_limit,
                "purpose": "独立处理发送失败且已到期的历史报告，不依赖主筛选流程触发",
            }
        )
    jobs = []
    for item in fixed_reports:
        jobs.append(
            {
                "job_id": item["report_type"],
                "job_type": "fixed_report",
                "title": item["title"],
                "command": item["command"],
                "time": item["time"],
                "days": days,
            }
        )
    for item in maintenance_jobs:
        jobs.append(
            {
                "job_id": item["task"],
                "job_type": "maintenance",
                "title": item["title"],
                "command": item["command"],
                "days": item["days"],
                "window": item["window"],
                "interval_minutes": item["interval_minutes"],
                "times": item["times"],
            }
        )
    return {
        "timezone": timezone,
        "window": {"start": start_text, "end": end_text},
        "interval_minutes": interval,
        "days": days,
        "times": times,
        "command": command,
        "preferences": preferences,
        "jobs": jobs,
        "fixed_reports": fixed_reports,
        "maintenance_jobs": maintenance_jobs,
        "event_alert_triggers": event_alerts,
        "trade_day_mode": "weekday",
        "calendar_file": getattr(args, "calendar_file", None) if args else None,
    }


def resolve_schedule_path():
    target = get_report_root() / "schedule.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    return target


def resolve_scheduled_run_log_path():
    target = get_report_root() / "index" / "scheduled_run_log.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    return target


def load_scheduled_run_log():
    return load_json_file(
        resolve_scheduled_run_log_path(),
        {
            "version": "v1",
            "updated_at": iso_utc_now(),
            "entries": [],
        },
    )


def append_scheduled_run_log(entry, max_entries=500):
    log_payload = load_scheduled_run_log()
    entries = log_payload.setdefault("entries", [])
    entries.insert(0, entry)
    log_payload["entries"] = entries[:max_entries]
    log_payload["updated_at"] = iso_utc_now()
    save_json_file(resolve_scheduled_run_log_path(), log_payload)
    return entry


def query_scheduled_run_log(job=None, status=None, limit=20):
    log_payload = load_scheduled_run_log()
    entries = []
    for entry in log_payload.get("entries") or []:
        if job and entry.get("job_id") != job:
            continue
        if status and entry.get("status") != status:
            continue
        entries.append(entry)
        if limit and len(entries) >= limit:
            break
    return {
        "path": str(resolve_scheduled_run_log_path()),
        "total": len(entries),
        "entries": entries,
    }


def resolve_cron_export_path():
    target = get_report_root() / "cron" / "clawtrade_schedule.cron"
    target.parent.mkdir(parents=True, exist_ok=True)
    return target


def build_cron_day_field(days):
    day_map = {
        "sun": "0",
        "mon": "1",
        "tue": "2",
        "wed": "3",
        "thu": "4",
        "fri": "5",
        "sat": "6",
    }
    mapped = [day_map[item] for item in days if item in day_map]
    return ",".join(mapped) if mapped else "1,2,3,4,5"


def build_cron_lines(spec, python_path=None, base_dir=None):
    python_cmd = python_path or sys.executable
    base_dir = Path(base_dir or Path(__file__).resolve().parent).resolve()
    script_path = base_dir / "screener_skill.py"
    logs_dir = get_report_root() / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    scheduler_mode = ((spec.get("preferences") or {}).get("scheduler_mode") or "openclaw_primary").lower()
    lines = [
        f"# Generated by screener_skill.py on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"SCREENER_RUN_CONTEXT=scheduled",
        f"# scheduler_mode={scheduler_mode}",
    ]
    for job in spec.get("jobs") or []:
        if scheduler_mode == "openclaw_primary" and job.get("job_type") != "maintenance":
            continue
        day_field = build_cron_day_field(job.get("days") or spec.get("days") or [])
        job_times = [job.get("time")] if job.get("time") else list(job.get("times") or [])
        for hhmm in job_times:
            if not hhmm:
                continue
            hour, minute = hhmm.split(":", 1)
            log_path = logs_dir / f"{job['job_id']}.log"
            lines.append(
                f"{int(minute)} {int(hour)} * * {day_field} cd {base_dir} && "
                f"{python_cmd} {script_path} run-scheduled --job {job['job_id']} >> {log_path} 2>&1"
            )
    return lines


def export_cron(args):
    spec, _path = load_schedule_spec()
    output_path = Path(getattr(args, "output", None) or resolve_cron_export_path()).expanduser().resolve()
    lines = build_cron_lines(spec, python_path=getattr(args, "python_path", None), base_dir=getattr(args, "base_dir", None))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    json_out(
        {
            "ok": True,
            "data": {
                "path": str(output_path),
                "jobs_total": len(spec.get("jobs") or []),
                "line_count": len(lines),
                "preview": lines[: min(10, len(lines))],
            },
        }
    )


def install_cron(args):
    output_path = Path(getattr(args, "output", None) or resolve_cron_export_path()).expanduser().resolve()
    spec, _path = load_schedule_spec()
    lines = build_cron_lines(spec, python_path=getattr(args, "python_path", None), base_dir=getattr(args, "base_dir", None))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    begin_marker = "# BEGIN CLAWTRADE-SCHEDULE"
    end_marker = "# END CLAWTRADE-SCHEDULE"
    try:
        existing = subprocess.run(["crontab", "-l"], capture_output=True, text=True, check=False)
    except FileNotFoundError:
        json_out({"ok": False, "error": "未找到 crontab 命令"}, 1)
    current_text = existing.stdout if existing.returncode == 0 else ""
    preserved = []
    inside_block = False
    for raw_line in current_text.splitlines():
        line = raw_line.rstrip("\n")
        if line.strip() == begin_marker:
            inside_block = True
            continue
        if line.strip() == end_marker:
            inside_block = False
            continue
        if not inside_block:
            preserved.append(line)
    managed_block = [begin_marker, *lines, end_marker]
    merged_lines = [line for line in preserved if line.strip()]
    if merged_lines:
        merged_lines.append("")
    merged_lines.extend(managed_block)
    payload = "\n".join(merged_lines) + "\n"
    install = subprocess.run(["crontab", "-"], input=payload, text=True, capture_output=True, check=False)
    if install.returncode != 0:
        json_out(
            {
                "ok": False,
                "error": install.stderr.strip() or install.stdout.strip() or "crontab 安装失败",
                "path": str(output_path),
            },
            1,
        )
    json_out(
        {
            "ok": True,
            "data": {
                "installed": True,
                "path": str(output_path),
                "jobs_total": len(spec.get("jobs") or []),
                "managed_markers": {"begin": begin_marker, "end": end_marker},
            },
        }
    )


def output_schedule(args):
    spec = build_schedule_spec(args)
    path = resolve_schedule_path()
    path.write_text(json.dumps(spec, ensure_ascii=False), encoding="utf-8")
    json_out({"ok": True, "data": {"schedule": spec, "path": str(path)}})


def manage_schedule_preferences(args):
    if getattr(args, "reset", False):
        preferences = save_schedule_preferences(build_default_schedule_preferences())
    else:
        preferences = load_schedule_preferences()
        if getattr(args, "profile", None):
            preferences = build_schedule_profile(args.profile)
        if getattr(args, "scheduler_mode", None):
            preferences["scheduler_mode"] = args.scheduler_mode
        enabled_reports = set(preferences.get("enabled_reports") or [])
        if getattr(args, "enable_report_types", None):
            enabled_reports.update(args.enable_report_types)
        if getattr(args, "disable_report_types", None):
            enabled_reports = {item for item in enabled_reports if item not in set(args.disable_report_types)}
        if enabled_reports:
            preferences["enabled_reports"] = sorted(enabled_reports)
        report_times = dict(preferences.get("report_times") or {})
        report_times.update(parse_report_time_map(getattr(args, "report_times", None)))
        preferences["report_times"] = report_times
        retry_preferences = dict(preferences.get("retry_maintenance") or {})
        if getattr(args, "enable_retry_maintenance", False):
            retry_preferences["enabled"] = True
        if getattr(args, "disable_retry_maintenance", False):
            retry_preferences["enabled"] = False
        if getattr(args, "retry_schedule_start", None):
            retry_preferences["start"] = parse_hhmm(args.retry_schedule_start, args.retry_schedule_start)[0]
        if getattr(args, "retry_schedule_end", None):
            retry_preferences["end"] = parse_hhmm(args.retry_schedule_end, args.retry_schedule_end)[0]
        if getattr(args, "retry_schedule_interval", None):
            retry_preferences["interval_minutes"] = parse_interval_minutes(args.retry_schedule_interval, args.retry_schedule_interval)
        if getattr(args, "retry_limit", None):
            retry_preferences["limit"] = int(args.retry_limit)
        preferences["retry_maintenance"] = retry_preferences
        preferences = save_schedule_preferences(preferences)
    schedule_spec, schedule_path = persist_schedule_spec()
    json_out(
        {
            "ok": True,
            "data": {
                "path": str(resolve_schedule_preferences_path()),
                "preferences": preferences,
                "schedule_preview": schedule_spec,
                "schedule_path": str(schedule_path),
            },
        }
    )


def build_automation_rrule(days, hhmm):
    day_map = {
        "mon": "MO",
        "tue": "TU",
        "wed": "WE",
        "thu": "TH",
        "fri": "FR",
        "sat": "SA",
        "sun": "SU",
    }
    hour, minute = hhmm.split(":", 1)
    byday = ",".join(day_map[item] for item in days if item in day_map)
    return f"FREQ=WEEKLY;BYDAY={byday};BYHOUR={int(hour)};BYMINUTE={int(minute)}"


def build_openclaw_automation_payload(spec):
    workspace = str(Path(__file__).resolve().parent)
    automations = []
    unsupported = []
    for job in spec.get("jobs") or []:
        if job.get("job_type") != "fixed_report":
            unsupported.append(
                {
                    "job_id": job.get("job_id"),
                    "reason": "unsupported_high_frequency_or_maintenance_job",
                }
            )
            continue
        job_id = job["job_id"]
        automations.append(
            {
                "job_id": job_id,
                "name": f"ClawTrade {job_id}",
                "rrule": build_automation_rrule(job.get("days") or spec.get("days") or [], job["time"]),
                "cwds": [workspace],
                "status": "ACTIVE",
                "prompt": (
                    f"Run `python3 screener_skill.py run-scheduled --job {job_id}`. "
                    "Return the JSON result. If the run is skipped because another execution is active, say so clearly. "
                    "If a report is generated, include report_id, report_type, delivery_status, and any failure or retry signals."
                ),
            }
        )
    return automations, unsupported


def output_openclaw_automation_spec(_args):
    spec, path = load_schedule_spec()
    automations, unsupported = build_openclaw_automation_payload(spec)
    json_out(
        {
            "ok": True,
            "data": {
                "path": str(path),
                "preferences_path": str(resolve_schedule_preferences_path()),
                "automations": automations,
                "unsupported_jobs": unsupported,
            },
        }
    )


def build_report_settings_markdown(summary):
    lines = [
        "# 汇报设置摘要",
        "",
        f"- 调度模式：`{summary['scheduler_mode']}`",
        f"- 当前配置档位：`{summary['profile']}`",
        f"- 用户入口：{summary['user_touchpoints']['primary']}",
    ]
    if summary.get("request_message"):
        lines.extend(
            [
                f"- 用户请求：{summary['request_message']}",
                f"- 应用模式：`{'dry_run' if summary.get('dry_run') else 'applied'}`",
            ]
        )
    changes = summary.get("changes") or []
    if changes:
        lines.extend(["", "## 本次变更"])
        for item in changes:
            lines.append(f"- `{item['type']}` | {json.dumps(item.get('value'), ensure_ascii=False)}")
    lines.extend(["", "## 当前启用的固定报告"])
    for item in summary.get("active_reports") or []:
        lines.append(
            f"- `{item['report_type']}` | {item['title']} | {item['time']} | 触达形式：{item['delivery_surface']}"
        )
    disabled = summary.get("disabled_reports") or []
    if disabled:
        lines.extend(["", "## 当前关闭的固定报告"])
        for item in disabled:
            lines.append(
                f"- `{item['report_type']}` | {item['title']} | 默认时间：{item['default_time']}"
            )
    maintenance = summary.get("maintenance_jobs") or []
    if maintenance:
        lines.extend(["", "## 维护任务"])
        for item in maintenance:
            lines.append(
                f"- `{item['job_id']}` | {item['title']} | {item['window']['start']}-{item['window']['end']} 每 {item['interval_minutes']} 分钟 | 触达形式：{item['delivery_surface']}"
            )
    notes = summary.get("notes") or []
    if notes:
        lines.extend(["", "## 说明"])
        for item in notes:
            lines.append(f"- {item}")
    lines.extend(["", "## 用户怎么接触这个能力"])
    for item in summary.get("user_touchpoints", {}).get("steps") or []:
        lines.append(f"- {item}")
    return "\n".join(lines)


def output_report_settings(args):
    preferences = load_schedule_preferences()
    _spec, summary = build_schedule_preference_snapshot(preferences)
    markdown = build_report_settings_markdown(summary)
    if getattr(args, "format", "json") == "markdown":
        print(markdown)
        return
    json_out({"ok": True, "data": {**summary, "markdown": markdown}})


def output_report_settings_request(args):
    summary = apply_natural_language_schedule_request(
        getattr(args, "message", None),
        dry_run=getattr(args, "dry_run", False),
    )
    markdown = build_report_settings_markdown(summary)
    if getattr(args, "format", "json") == "markdown":
        print(markdown)
        return
    json_out({"ok": True, "data": {**summary, "markdown": markdown}})


def load_schedule_spec():
    path = resolve_schedule_path()
    spec = build_schedule_spec()
    path.write_text(json.dumps(spec, ensure_ascii=False), encoding="utf-8")
    return spec, path


def build_scheduled_job_index(spec):
    jobs = {}
    for item in spec.get("fixed_reports") or []:
        jobs[item.get("report_type")] = {"job_type": "fixed_report", **item}
    for item in spec.get("maintenance_jobs") or []:
        jobs[item.get("task")] = {"job_type": "maintenance", **item}
    return jobs


def run_scheduled_job(args):
    spec, path = load_schedule_spec()
    jobs = build_scheduled_job_index(spec)
    if getattr(args, "list", False):
        json_out(
            {
                "ok": True,
                "data": {
                    "path": str(path),
                    "jobs": list(jobs.values()),
                },
            }
        )
    job_name = getattr(args, "job", None)
    if not job_name:
        json_out({"ok": False, "error": "缺少 --job"}, 1)
    job = jobs.get(job_name)
    if not job:
        json_out({"ok": False, "error": f"未知计划任务: {job_name}", "available_jobs": sorted(jobs.keys())}, 1)
    if getattr(args, "dry_run", False):
        json_out({"ok": True, "data": {"path": str(path), "job": job}})
    delegated = None
    if job.get("job_type") == "maintenance" and job_name == "delivery_retry":
        delegated = ["report-retry", "--all-due", "--limit", str(job.get("limit") or 10)]
    elif job.get("job_type") == "fixed_report":
        delegated = ["auto", "--report-type", job_name]
    if delegated:
        started_at = iso_utc_now()
        env = os.environ.copy()
        env["SCREENER_RUN_CONTEXT"] = "scheduled"
        proc = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), *delegated],
            cwd=str(Path(__file__).resolve().parent),
            capture_output=True,
            text=True,
            env=env,
        )
        stdout = (proc.stdout or "").strip()
        stderr = (proc.stderr or "").strip()
        payload = None
        skipped = False
        if stdout:
            try:
                payload = json.loads(stdout)
            except json.JSONDecodeError:
                payload = None
        if isinstance(payload, dict):
            skipped = bool(((payload.get("data") or {}).get("skipped")))
        entry = append_scheduled_run_log(
            {
                "run_id": f"{job_name}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}",
                "job_id": job_name,
                "job_type": job.get("job_type"),
                "started_at": started_at,
                "finished_at": iso_utc_now(),
                "status": "skipped" if skipped else ("completed" if proc.returncode == 0 else "failed"),
                "exit_code": proc.returncode,
                "delegated_command": [sys.executable, str(Path(__file__).resolve()), *delegated],
                "schedule_path": str(path),
                "stderr": stderr or None,
                "result_ok": payload.get("ok") if isinstance(payload, dict) else None,
                "report_id": ((payload or {}).get("data") or {}).get("report", {}).get("report_id"),
                "report_type": ((payload or {}).get("data") or {}).get("report", {}).get("report_type"),
                "retried": ((payload or {}).get("data") or {}).get("retried"),
                "skipped": skipped,
            }
        )
        wrapper = {
            "ok": proc.returncode == 0,
            "data": {
                "path": str(path),
                "job": job,
                "delegated_command": delegated,
                "run_log_entry": entry,
                "result": payload if payload is not None else {"stdout": stdout, "stderr": stderr},
            },
        }
        if proc.returncode != 0 and payload is None:
            wrapper["error"] = stderr or stdout or f"scheduled job failed: {job_name}"
            json_out(wrapper, 1)
        json_out(wrapper, proc.returncode)
        return
    json_out({"ok": False, "error": f"暂不支持的计划任务: {job_name}"}, 1)


def output_scheduled_runs(args):
    json_out(
        {
            "ok": True,
            "data": query_scheduled_run_log(
                job=getattr(args, "job", None),
                status=getattr(args, "status", None),
                limit=int(getattr(args, "limit", 20)),
            ),
        }
    )


def load_trading_calendar(calendar_file):
    if not calendar_file:
        return None
    path = Path(calendar_file).expanduser().resolve()
    if not path.exists():
        json_out({"ok": False, "error": f"未找到交易日历文件: {path}"}, 1)
    content = path.read_text(encoding="utf-8").strip()
    if not content:
        return set()
    if path.suffix.lower() == ".json":
        data = json.loads(content)
        if isinstance(data, dict):
            items = data.get("dates") or []
        elif isinstance(data, list):
            items = data
        else:
            items = []
        return {str(item) for item in items}
    dates = []
    for line in content.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        dates.append(stripped)
    return set(dates)


def is_trading_day(date_value, calendar):
    if calendar is None:
        return date_value.weekday() < 5
    return date_value.strftime("%Y-%m-%d") in calendar


def normalize_hsi_symbol(value):
    return try_normalize_hk_symbol(value)


def resolve_universe_cache_dir():
    target = get_report_root() / "cache" / "universe"
    target.mkdir(parents=True, exist_ok=True)
    return target


def resolve_universe_json_path():
    """获取标的池JSON文件路径"""
    base_dir = Path(__file__).resolve().parent
    return base_dir / "universe.json"


def get_default_hsi_stocks():
    """获取默认恒生指数成分股列表"""
    return [
        # 互联网/科技
        {"code": "HK.00700", "name": "腾讯控股", "sector": "科技"},
        {"code": "HK.09988", "name": "百度集团-SW", "sector": "科技"},
        {"code": "HK.01810", "name": "小米集团-W", "sector": "科技"},
        {"code": "HK.03690", "name": "美团-W", "sector": "科技"},
        {"code": "HK.06618", "name": "京东集团-SW", "sector": "科技"},
        {"code": "HK.09618", "name": "携程集团-S", "sector": "科技"},
        # 金融
        {"code": "HK.02318", "name": "中国平安", "sector": "金融"},
        {"code": "HK.00939", "name": "建设银行", "sector": "金融"},
        {"code": "HK.02628", "name": "工商银行", "sector": "金融"},
        {"code": "HK.00981", "name": "中银香港", "sector": "金融"},
        {"code": "HK.02388", "name": "友邦保险", "sector": "金融"},
        {"code": "HK.01398", "name": "港交所", "sector": "金融"},
        {"code": "HK.00005", "name": "汇丰控股", "sector": "金融"},
        # 新能源汽车
        {"code": "HK.02015", "name": "理想汽车-W", "sector": "新能源汽车"},
        {"code": "HK.09868", "name": "小鹏汽车-W", "sector": "新能源汽车"},
        {"code": "HK.02518", "name": "蔚来-SW", "sector": "新能源汽车"},
        # 消费
        {"code": "HK.02331", "name": "泡泡玛特", "sector": "消费"},
        {"code": "HK.06186", "name": "京东物流", "sector": "消费"},
        {"code": "HK.01928", "name": "周大福", "sector": "消费"},
        {"code": "HK.02269", "name": "海底捞", "sector": "消费"},
        {"code": "HK.03616", "name": "农夫山泉", "sector": "消费"},
        # 医疗健康
        {"code": "HK.02218", "name": "阿里健康", "sector": "医疗健康"},
        {"code": "HK.06612", "name": "平安好医生", "sector": "医疗健康"},
        {"code": "HK.01885", "name": "康希诺生物", "sector": "医疗健康"},
        # 地产/博彩
        {"code": "HK.01171", "name": "碧桂园", "sector": "地产"},
        {"code": "HK.01628", "name": "银河娱乐", "sector": "博彩"},
        {"code": "HK.00027", "name": "九龙仓置业", "sector": "地产"},
        {"code": "HK.06808", "name": "华润置地", "sector": "地产"},
        # 通信
        {"code": "HK.00728", "name": "中国电信", "sector": "通信"},
        {"code": "HK.02618", "name": "中国联通", "sector": "通信"},
    ]


def load_universe_json():
    """加载标的池JSON文件"""
    path = resolve_universe_json_path()
    if path.exists():
        try:
            content = path.read_text(encoding="utf-8")
            return json.loads(content)
        except Exception:
            pass
    # 文件不存在或解析失败，返回默认结构
    return initialize_universe_json()


def save_universe_json(data):
    """保存标的池JSON文件"""
    path = resolve_universe_json_path()
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def initialize_universe_json():
    """初始化标的池JSON文件"""
    data = {
        "version": "1.0",
        "description": "港股标的池 - 恒生指数成分股",
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "hsi_stocks": get_default_hsi_stocks(),
        "user_stocks": [],
        "hsi_refresh_date": datetime.now().strftime("%Y-%m-%d"),
        "stats": {
            "hsi_count": len(get_default_hsi_stocks()),
            "user_count": 0,
            "total_count": len(get_default_hsi_stocks())
        }
    }
    save_universe_json(data)
    return data


def add_user_stock(symbol, name=None, sector=None):
    """添加用户关注的股票到标的池
    
    Args:
        symbol: 股票代码 (如 HK.02015)
        name: 股票名称 (可选，自动获取)
        sector: 行业分类 (可选，自动获取)
    """
    data = load_universe_json()
    
    # 归一化股票代码
    norm_symbol = normalize_hk_symbol(symbol)
    if not norm_symbol:
        return None, f"无效的股票代码: {symbol}"
    
    # 检查是否已存在
    all_stocks = data.get("hsi_stocks", []) + data.get("user_stocks", [])
    for stock in all_stocks:
        if normalize_hk_symbol(stock.get("code", "")) == norm_symbol:
            return data, f"股票 {norm_symbol} 已存在于标的池"
    
    # 自动获取名称和行业
    auto_name = name
    auto_sector = sector
    
    if not auto_name or not auto_sector:
        for stock in data.get("hsi_stocks", []):
            if try_normalize_hk_symbol(stock.get("code")) == norm_symbol:
                auto_name = auto_name or stock.get("name")
                auto_sector = auto_sector or stock.get("sector")
                break
    if not auto_name:
        try:
            payload = call_futu_skill(["quote", "--symbols", norm_symbol])
            rows = payload.get("data") or []
            if rows:
                row = rows[0]
                auto_name = row.get("name") or row.get("stock_name") or row.get("stock_name_cn")
        except Exception:
            pass

    # 添加到用户股票列表
    new_stock = {
        "code": norm_symbol,
        "name": auto_name or norm_symbol,
        "sector": auto_sector or "用户自定义",
        "added_date": datetime.now().strftime("%Y-%m-%d"),
        "source": "user"
    }
    data["user_stocks"].append(new_stock)
    data["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data["stats"]["user_count"] = len(data["user_stocks"])
    data["stats"]["total_count"] = len(data["hsi_stocks"]) + len(data["user_stocks"])
    
    save_universe_json(data)
    return data, f"已添加 {norm_symbol} ({auto_name or '未知名称'}) 到标的池，行业: {auto_sector or '用户自定义'}"


def get_all_universe_symbols():
    """获取所有标的池中的股票代码列表"""
    data = load_universe_json()
    symbols = []
    for stock in data.get("hsi_stocks", []) + data.get("user_stocks", []):
        code = try_normalize_hk_symbol(stock.get("code"))
        if code:
            symbols.append(code)
    return symbols


def get_invalid_universe_stocks():
    data = load_universe_json()
    invalid = []
    for section in ("hsi_stocks", "user_stocks"):
        for stock in data.get(section, []):
            code = stock.get("code")
            if code and not try_normalize_hk_symbol(code):
                invalid.append(
                    {
                        "section": section,
                        "code": code,
                        "name": stock.get("name"),
                    }
                )
    return invalid


def get_universe_by_sector():
    """按行业分类获取标的池"""
    data = load_universe_json()
    sectors = {}
    for stock in data.get("hsi_stocks", []) + data.get("user_stocks", []):
        sector = stock.get("sector", "未知")
        if sector not in sectors:
            sectors[sector] = []
        sectors[sector].append(stock)
    return sectors


def refresh_hsi_from_api():
    """从API刷新恒生指数成分股"""
    symbols, error = fetch_hsi_constituents()
    if not symbols:
        return None, error or "刷新失败"
    
    data = load_universe_json()
    
    # 保留用户自定义股票
    user_stocks = data.get("user_stocks", [])
    
    # 更新HSI股票列表
    hsi_stocks = []
    for sym in symbols:
        hsi_stocks.append({
            "code": sym,
            "name": sym,
            "sector": "科技",  # 默认分类，后续可优化
            "refresh_date": datetime.now().strftime("%Y-%m-%d")
        })
    
    data["hsi_stocks"] = hsi_stocks
    data["hsi_refresh_date"] = datetime.now().strftime("%Y-%m-%d")
    data["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data["stats"]["hsi_count"] = len(hsi_stocks)
    data["stats"]["total_count"] = len(hsi_stocks) + len(user_stocks)
    
    save_universe_json(data)
    return data, f"已刷新 {len(hsi_stocks)} 只恒生成分股"


def extract_constituent_symbols(records):
    symbol_keys = [
        "code",
        "symbol",
        "stock_code",
        "security",
        "ticker",
        "成分券代码",
        "证券代码",
        "股票代码",
    ]
    symbols = []
    for row in records:
        value = None
        for key in symbol_keys:
            if key in row and row.get(key):
                value = row.get(key)
                break
        norm = normalize_hsi_symbol(value) if value is not None else None
        if norm:
            symbols.append(norm)
    return symbols


def fetch_hsi_constituents():
    try:
        import akshare as ak
    except Exception as exc:
        return None, f"akshare_unavailable:{exc}"
    attempts = [
        ("index_stock_cons_sina", [{"symbol": "HSI"}, {"symbol": "恒生指数"}]),
        ("index_stock_cons", [{"symbol": "HSI"}, {"symbol": "恒生指数"}]),
    ]
    for name, params_list in attempts:
        if not hasattr(ak, name):
            continue
        fn = getattr(ak, name)
        for params in params_list:
            try:
                df = fn(**params)
            except Exception:
                continue
            records = df.to_dict("records") if hasattr(df, "to_dict") else df
            symbols = extract_constituent_symbols(records or [])
            if symbols:
                return symbols, None
    return None, "hsi_constituents_fetch_failed"


def load_default_hsi_universe():
    cache_dir = resolve_universe_cache_dir()
    date_tag = datetime.now().strftime("%Y%m%d")
    today_path = cache_dir / f"hsi_{date_tag}.json"
    if today_path.exists():
        payload = json.loads(today_path.read_text(encoding="utf-8") or "{}")
        return payload.get("symbols") or [], {"source": "HSI", "date": date_tag, "cached": True}
    symbols, error = fetch_hsi_constituents()
    if symbols:
        payload = {"symbols": symbols, "date": date_tag}
        today_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        return symbols, {"source": "HSI", "date": date_tag, "cached": False}
    fallback = sorted(cache_dir.glob("hsi_*.json"), reverse=True)
    if fallback:
        payload = json.loads(fallback[0].read_text(encoding="utf-8") or "{}")
        return payload.get("symbols") or [], {"source": "HSI", "date": payload.get("date"), "cached": True}
    json_out({"ok": False, "error": "无法获取恒生指数成分股列表", "detail": error}, 1)


def load_universe_symbols(args):
    symbols = []
    if getattr(args, "universe", None):
        symbols.extend(args.universe)
    if getattr(args, "universe_file", None):
        path = Path(args.universe_file).expanduser().resolve()
        if not path.exists():
            json_out({"ok": False, "error": f"未找到标的池文件: {path}"}, 1)
        content = path.read_text(encoding="utf-8")
        symbols.extend(parse_symbol_list(content))
    if not symbols:
        # 优先从JSON文件读取标的池
        try:
            symbols = get_all_universe_symbols()
            data = load_universe_json()
            meta = {
                "source": "universe_json",
                "hsi_count": data.get("stats", {}).get("hsi_count", 0),
                "user_count": data.get("stats", {}).get("user_count", 0),
                "total_count": data.get("stats", {}).get("total_count", 0),
                "last_updated": data.get("last_updated", "")
            }
        except Exception:
            # 失败则回退到旧的获取方式
            symbols, meta = load_default_hsi_universe()
        setattr(args, "_universe_meta", meta)
    else:
        setattr(args, "_universe_meta", {"source": "manual"})
    normalized = []
    seen = set()
    invalid_symbols = []
    for symbol in symbols:
        norm = try_normalize_hk_symbol(symbol)
        if not norm:
            invalid_symbols.append(symbol)
            continue
        if norm in seen:
            continue
        seen.add(norm)
        normalized.append(norm)
    if invalid_symbols:
        meta = getattr(args, "_universe_meta", {})
        meta["invalid_symbols"] = invalid_symbols
        setattr(args, "_universe_meta", meta)
    valid_symbols, quote_invalid_symbols = filter_symbols_with_quote_probe(normalized)
    if quote_invalid_symbols:
        meta = getattr(args, "_universe_meta", {})
        meta["quote_invalid_symbols"] = quote_invalid_symbols
        setattr(args, "_universe_meta", meta)
    return valid_symbols


def chunk_list(values, size):
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]


def fetch_quotes(symbols, batch_size=50):
    rows = []
    for batch in chunk_list(symbols, batch_size):
        payload = call_futu_skill(["quote", "--symbols", *batch])
        data = payload.get("data") or []
        rows.extend(data)
    return rows


def filter_symbols_with_quote_probe(symbols, batch_size=50):
    valid_symbols = []
    invalid_symbols = []
    seen = set()
    for batch in chunk_list(symbols, batch_size):
        payload = call_futu_skill_safe(["quote", "--symbols", *batch])
        if payload.get("ok"):
            returned = set()
            for row in payload.get("data") or []:
                symbol = extract_quote_symbol(row)
                if symbol:
                    returned.add(symbol)
            for symbol in batch:
                if symbol in returned and symbol not in seen:
                    seen.add(symbol)
                    valid_symbols.append(symbol)
                elif symbol not in returned:
                    invalid_symbols.append({"symbol": symbol, "error": "quote_not_returned"})
            continue
        if len(batch) == 1:
            invalid_symbols.append({"symbol": batch[0], "error": payload.get("error") or "quote_probe_failed"})
            continue
        for symbol in batch:
            single = call_futu_skill_safe(["quote", "--symbols", symbol])
            if single.get("ok"):
                if symbol not in seen:
                    seen.add(symbol)
                    valid_symbols.append(symbol)
            else:
                invalid_symbols.append({"symbol": symbol, "error": single.get("error") or payload.get("error") or "quote_probe_failed"})
    return valid_symbols, invalid_symbols


def extract_quote_liquidity(row):
    return pick_metric(
        row,
        [
            "turnover",
            "turnover_value",
            "turnover_vol",
            "volume",
            "volume_value",
            "turnover_rate",
        ],
    )


def extract_quote_symbol(row):
    for key in ["code", "symbol", "stock_code", "security", "security_code", "ticker"]:
        value = row.get(key)
        if value:
            return normalize_hk_symbol(str(value))
    return None


def get_report_root():
    env_keys = ["OPENCLAW_WORKSPACE", "OPENCLAW_WORKSPACE_DIR", "WORKSPACE_DIR"]
    workspace_root = None
    for key in env_keys:
        value = os.environ.get(key)
        if value:
            workspace_root = Path(value).expanduser().resolve()
            break
    if workspace_root is None:
        workspace_root = Path.cwd()
    root = workspace_root / "HKAutoTradeReports"
    root.mkdir(parents=True, exist_ok=True)
    return root


def resolve_report_dir(report_dir):
    if report_dir:
        target = Path(report_dir).expanduser().resolve()
    else:
        date_tag = datetime.now().strftime("%Y%m%d")
        target = get_report_root() / "reports" / date_tag
    target.mkdir(parents=True, exist_ok=True)
    return target


def estimate_return_7d(return_21d):
    if return_21d is None:
        return None
    try:
        return (1 + return_21d) ** (7 / 21) - 1
    except Exception:
        return None


def format_percent(value):
    if value is None:
        return "N/A"
    return f"{value * 100:.2f}%"


def compute_portfolio_7d_forecast(allocation, screened):
    if not allocation:
        return None
    metric_map = {item["symbol"]: item.get("metrics", {}) for item in screened}
    weighted = 0.0
    coverage = 0.0
    for item in allocation:
        symbol = item.get("symbol")
        weight = item.get("suggested_weight", 0.0)
        metrics = metric_map.get(symbol, {})
        r21 = metrics.get("return_21d")
        r7 = estimate_return_7d(r21)
        if r7 is None:
            continue
        weighted += weight * r7
        coverage += weight
    if coverage == 0:
        return None
    return weighted / coverage


def resolve_trade_price_map(symbols, price_mode, collect_data):
    price_mode = (price_mode or "last").lower()
    if price_mode == "close":
        price_map = {}
        for symbol in symbols:
            entry = collect_data.get(symbol, {})
            kline = entry.get("kline_summary", {})
            price_map[symbol] = parse_float(kline.get("last_close"))
        return price_map
    quote_rows = fetch_quotes(symbols)
    price_map = {}
    for row in quote_rows:
        symbol = extract_quote_symbol(row)
        if not symbol:
            continue
        price = pick_metric(row, ["last_price", "price", "last", "close", "close_price", "prev_close"])
        price_map[symbol] = price
    return price_map


def allocate_trade_qty(allocation, total_qty):
    total_qty = int(total_qty)
    if total_qty <= 0:
        return {}
    weight_total = sum(item.get("suggested_weight", 0.0) for item in allocation)
    if weight_total <= 0:
        weight_total = len(allocation)
        if weight_total == 0:
            return {}
    qty_map = {}
    for item in allocation:
        symbol = item.get("symbol")
        weight = item.get("suggested_weight", 0.0)
        if weight_total > 0:
            ratio = weight / weight_total
        else:
            ratio = 1.0 / len(allocation)
        qty = int(round(total_qty * ratio))
        qty_map[symbol] = max(1, qty)
    return qty_map


def allocate_trade_qty_by_budget(allocation, price_map, total_budget):
    total_budget = float(total_budget)
    if total_budget <= 0:
        return {}
    weight_total = sum(item.get("suggested_weight", 0.0) for item in allocation)
    if weight_total <= 0:
        weight_total = len(allocation)
        if weight_total == 0:
            return {}
    qty_map = {}
    for item in allocation:
        symbol = item.get("symbol")
        weight = item.get("suggested_weight", 0.0)
        price = price_map.get(symbol)
        if price is None or price <= 0:
            qty_map[symbol] = 0
            continue
        if weight_total > 0:
            ratio = weight / weight_total
        else:
            ratio = 1.0 / len(allocation)
        budget = total_budget * ratio
        qty = int(budget // price)
        qty_map[symbol] = max(0, qty)
    return qty_map


def execute_trades(allocation, collect_data, args):
    if not getattr(args, "auto_trade", False):
        return {"enabled": False, "orders": []}
    symbols = [item.get("symbol") for item in allocation if item.get("symbol")]
    if not symbols:
        return {"enabled": True, "orders": [], "errors": ["no_symbols"]}
    total_qty = int(args.trade_qty or 0)
    total_budget = float(args.trade_budget or 0)
    if total_qty <= 0 and total_budget <= 0:
        json_out({"ok": False, "error": "auto_trade 开启时需要设置 trade_qty 或 trade_budget"}, 1)
    side = (args.trade_side or "buy").lower()
    order_type = (args.trade_order_type or "limit").lower()
    price_mode = (args.trade_price_mode or "last").lower()
    price_map = resolve_trade_price_map(symbols, price_mode, collect_data)
    if total_budget > 0:
        qty_map = allocate_trade_qty_by_budget(allocation, price_map, total_budget)
    else:
        qty_map = allocate_trade_qty(allocation, total_qty)
    orders = []
    for symbol in symbols:
        qty = qty_map.get(symbol, 0)
        price = price_map.get(symbol)
        if qty <= 0:
            orders.append({"symbol": symbol, "status": "skipped", "error": "qty_invalid"})
            continue
        if price is None:
            orders.append({"symbol": symbol, "status": "skipped", "error": "price_unavailable"})
            continue
        try:
            payload = call_futu_skill(
                [
                    "buy" if side == "buy" else "sell",
                    "--symbol",
                    symbol,
                    "--qty",
                    str(qty),
                    "--price",
                    str(price),
                    "--order-type",
                    order_type,
                ]
            )
            orders.append(
                {
                    "symbol": symbol,
                    "qty": qty,
                    "price": price,
                    "side": side,
                    "order_type": order_type,
                    "status": "submitted",
                    "data": payload.get("data"),
                }
            )
        except SystemExit:
            raise
        except Exception as exc:
            orders.append(
                {
                    "symbol": symbol,
                    "qty": qty,
                    "price": price,
                    "side": side,
                    "order_type": order_type,
                    "status": "failed",
                    "error": str(exc),
                }
            )
    return {
        "enabled": True,
        "side": side,
        "order_type": order_type,
        "price_mode": price_mode,
        "total_qty": total_qty,
        "total_budget": total_budget,
        "orders": orders,
    }


def build_report_markdown(payload):
    data = payload.get("data", {})
    config = data.get("config", {})
    report_context = data.get("report_context") or {}
    report_type = report_context.get("report_type") or "ad_hoc_screen"
    report_window_id = report_context.get("window_id")
    report_profile = pick_report_profile(report_type)
    auto_ctx = data.get("auto") or {}
    screened = data.get("screened") or []
    dropped = data.get("dropped") or []
    allocation = data.get("portfolio", {}).get("allocation") or []
    portfolio = data.get("portfolio", {}) or {}
    trades = data.get("trades") or {}
    decision = data.get("decision") or {}
    summary = data.get("summary") or {}
    event_alerts = data.get("event_alerts") or []
    target_projection = portfolio.get("target_projection") or {}
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = []
    lines.append(f"# {report_profile['headline']}")
    lines.append("")
    lines.append("## 报告摘要")
    lines.append(f"- 报告时间：{now}")
    lines.append(f"- 报告类型：{report_type}")
    if report_window_id:
        lines.append(f"- 汇报窗口：{report_window_id}")
    lines.append(f"- 市场范围：{config.get('market', 'HK')}")
    if auto_ctx:
        lines.append(f"- 标的池规模：{auto_ctx.get('universe_total', 0)}")
        lines.append(f"- 流动性与波动通过数：{auto_ctx.get('quote_kline_passed', 0)}")
        lines.append(f"- 信号筛选通过数：{auto_ctx.get('signal_passed', 0)}")
        universe_meta = auto_ctx.get("universe_meta") or {}
        if universe_meta.get("source"):
            lines.append(f"- 标的池来源：{universe_meta.get('source')}")
        if universe_meta.get("date"):
            lines.append(f"- 标的池日期：{universe_meta.get('date')}")
    lines.append(f"- 最终入选数量：{len(screened)}")
    if summary.get("negative_information_flags") is not None:
        lines.append(f"- 信息面风险标记数：{summary.get('negative_information_flags')}")
    lines.append("")
    lines.append("## 当前结论")
    if report_type == "event_alert":
        if event_alerts:
            lines.append(f"- 当前触发 {len(event_alerts)} 条事件快报条件，建议立即复核动作优先级")
        else:
            lines.append("- 当前未命中事件快报规则")
    elif report_type == "post_close":
        lines.append(f"- 收盘后确认：成交 {len([item for item in trades.get('orders', []) if item.get('status') == 'submitted'])} 笔，失败 {len([item for item in trades.get('orders', []) if item.get('status') == 'failed'])} 笔")
    elif report_type == "night_review":
        lines.append(f"- 夜间复盘结论：今日入选 {len(screened)}，排除 {len(dropped)}，次日继续跟踪 {min(len(dropped), 3)} 只重点标的")
    elif report_type == "pre_close_risk":
        lines.append(f"- 收盘前判断：当前建议仓位 {len(allocation)}，现金权重 {format_percent(portfolio.get('cash_weight'))}")
    else:
        lines.append(f"- 团队当前判断：{report_profile.get('trade' if allocation else 'watch' if screened else 'no_action')}")
    lines.append("")
    lines.append("## 市场与机会概览")
    candidates = []
    if auto_ctx:
        for item in auto_ctx.get("signal_candidates") or []:
            if item.get("passed"):
                candidates.append(item)
    if candidates:
        lines.append("")
        lines.append("| 标的 | 21日收益 | 63日收益 | 最大回撤 | 年化波动 |")
        lines.append("| --- | --- | --- | --- | --- |")
        for item in candidates:
            lines.append(
                f"| {item.get('symbol')} | {format_percent(item.get('return_21d'))} | {format_percent(item.get('return_63d'))} | {format_percent(item.get('max_drawdown'))} | {format_percent(item.get('volatility_252d'))} |"
            )
    elif screened:
        lines.append("")
        lines.append("| 标的 | 21日收益 | 63日收益 | 最大回撤 |")
        lines.append("| --- | --- | --- | --- |")
        for item in screened:
            metrics = item.get("metrics", {})
            lines.append(
                f"| {item.get('symbol')} | {format_percent(metrics.get('return_21d'))} | {format_percent(metrics.get('return_63d'))} | {format_percent(metrics.get('max_drawdown'))} |"
            )
    else:
        lines.append("- 本轮未发现符合条件的潜在机会")
    lines.append("")
    lines.append("## 信息面与事件窗口")
    info_rows = screened if screened else dropped[: min(len(dropped), 5)]
    if config.get("disable_information"):
        lines.append("- 已关闭信息面采集")
    elif info_rows:
        lines.append("")
        lines.append("| 标的 | 信息分 | 新闻 | 公告 | 下次业绩窗口 | 风险标签 |")
        lines.append("| --- | --- | --- | --- | --- | --- |")
        for item in info_rows:
            info = item.get("information") or {}
            news = info.get("news", {})
            announcements = info.get("announcements", {})
            earnings = info.get("earnings_calendar", {})
            next_event = "N/A"
            if earnings.get("items"):
                next_item = earnings.get("items")[0]
                next_event = f"{next_item.get('bm_date')} ({next_item.get('days_until')}d)"
            risk_flags = ",".join(info.get("risk_flags") or []) or "-"
            lines.append(
                f"| {item.get('symbol')} | {item.get('scores', {}).get('information') or 'N/A'} | "
                f"+{news.get('positive_count', 0)}/-{news.get('negative_count', 0)} | "
                f"+{announcements.get('positive_count', 0)}/-{announcements.get('negative_count', 0)} | "
                f"{next_event} | {risk_flags} |"
            )
        lines.append("")
        for item in info_rows[:3]:
            info = item.get("information") or {}
            top_news = (info.get("news", {}).get("items") or [])[:1]
            top_announcement = (info.get("announcements", {}).get("items") or [])[:1]
            if top_news:
                lines.append(f"- {item.get('symbol')} 新闻：{top_news[0].get('title')}")
            if top_announcement:
                lines.append(f"- {item.get('symbol')} 公告：{top_announcement[0].get('title')}")
    else:
        lines.append("- 未抓到可用的信息面数据")
    lines.append("")
    if event_alerts:
        lines.append("## 事件快报触发")
        for item in event_alerts:
            lines.append(f"- [{item.get('severity')}] {item.get('summary')}")
        lines.append("")
    lines.append("## 决策框架与规则")
    lines.append(f"- 因子权重：{json.dumps(config.get('weights', {}), ensure_ascii=False)}")
    lines.append(f"- 阈值设置：{json.dumps(config.get('thresholds', {}), ensure_ascii=False)}")
    if auto_ctx:
        lines.append(f"- 信号阈值：{json.dumps(auto_ctx.get('signal_thresholds', {}), ensure_ascii=False)}")
    if config.get("dynamic_threshold"):
        lines.append("- 启用动态阈值调节以匹配月度目标")
    if config.get("enforce_target"):
        lines.append("- 启用目标收益不足剔除")
    lines.append("")
    focus_sectors = decision.get("focus_sectors") or []
    if focus_sectors:
        lines.append("## 重点领域增补分析")
        lines.append(f"- 关注领域：{', '.join(focus_sectors)}")
        lines.append(f"- 入选数量：{decision.get('focus_screened', 0)}")
        if decision.get("focus_note") == "missing_sector_map":
            lines.append("- 领域映射缺失：无法基于领域进一步聚合")
        lines.append("")
    if decision.get("focus") is not None:
        lines.append("## 机会对比与取舍")
        metric_name = decision.get("comparison_metric")
        base_metric = decision.get("base_metric")
        focus_metric = decision.get("focus_metric")
        if metric_name == "forecast_7d":
            lines.append(f"- 标准组合7天预估：{format_percent(base_metric)}")
            lines.append(f"- 重点领域7天预估：{format_percent(focus_metric)}")
        else:
            lines.append(f"- 标准组合1月预估：{format_percent(base_metric)}")
            lines.append(f"- 重点领域1月预估：{format_percent(focus_metric)}")
        selected_source = decision.get("selected_source")
        if selected_source == "focus":
            lines.append("- 决策结论：采用重点领域组合执行交易")
        else:
            lines.append("- 决策结论：采用标准组合执行交易")
        lines.append("")
    lines.append("## 组合建议与交易执行")
    if allocation:
        for item in allocation:
            lines.append(
                f"- {item.get('symbol')}：建议权重{format_percent(item.get('suggested_weight'))}，评分{item.get('score')}"
            )
    else:
        lines.append("- 本轮无入选标的")
    if trades.get("enabled"):
        order_list = trades.get("orders") or []
        if trades.get("total_budget"):
            lines.append(f"- 预算总额：{trades.get('total_budget')}")
        if trades.get("total_qty"):
            lines.append(f"- 下单总量：{trades.get('total_qty')}")
        if order_list:
            lines.append("")
            lines.append("| 标的 | 方向 | 数量 | 价格 | 类型 | 状态 |")
            lines.append("| --- | --- | --- | --- | --- | --- |")
            for order in order_list:
                lines.append(
                    f"| {order.get('symbol')} | {order.get('side')} | {order.get('qty')} | {order.get('price')} | {order.get('order_type')} | {order.get('status')} |"
                )
        else:
            lines.append("- 交易执行：未产生有效订单")
    else:
        lines.append("- 交易执行：未启用自动下单或本轮未产生交易指令")
    if trades.get("orders"):
        lines.append("- 交易价位：见本段订单明细")
    else:
        lines.append("- 交易价位：无")
    lines.append("")
    if report_type == "pre_close_risk":
        lines.append("## 隔夜风险检查")
        if event_alerts:
            lines.append("- 隔夜前已有事件触发，需优先复核公告、财报和执行异常")
        else:
            lines.append("- 当前未触发新增隔夜异常，保持常规风险观察")
        if dropped:
            lines.append(f"- 主要风险标的：{', '.join(item.get('symbol') for item in dropped[:3])}")
        lines.append("")
    if report_type == "post_close":
        lines.append("## 执行偏差与归因")
        failed_orders = [item for item in trades.get("orders", []) if item.get("status") == "failed"]
        if failed_orders:
            for item in failed_orders[:5]:
                lines.append(f"- 执行失败：{item.get('symbol')}，原因={item.get('error') or 'unknown'}")
        elif trades.get("orders"):
            lines.append("- 今日订单已完成提交，未发现执行失败")
        else:
            lines.append("- 今日无订单执行，主要偏差来自信号与阈值未满足")
        if target_projection.get("gap") is not None:
            lines.append(f"- 月目标偏差：{format_percent(target_projection.get('gap'))}")
        lines.append("")
    if report_type == "night_review":
        lines.append("## 夜间复盘结论")
        if dropped:
            for item in dropped[:3]:
                lines.append(f"- 失效点：{item.get('symbol')} 因 {', '.join(item.get('reasons') or ['阈值未满足'])} 被排除")
        else:
            lines.append("- 今日无明显失效样本")
        lines.append("- 规则更新建议：若连续多日无入选，可评估扩充标的池或重审阈值")
        lines.append("- 次日观察重点：优先跟踪近 7 天财报窗口和信息面风险标的")
        lines.append("")
    lines.append("## 7天收益预测")
    forecast = compute_portfolio_7d_forecast(allocation, screened)
    if forecast is None:
        lines.append("- 组合7天收益预估：N/A（无持仓或缺少收益数据）")
    else:
        lines.append(f"- 组合7天收益预估：{format_percent(forecast)}")
    if dropped:
        lines.append("")
        lines.append("## 未入选标的与主要原因")
        for item in dropped:
            reasons = item.get("reasons") or []
            if reasons:
                lines.append(f"- {item.get('symbol')}：{', '.join(reasons)}")
            else:
                lines.append(f"- {item.get('symbol')}：未满足阈值或约束")
    lines.append("")
    return "\n".join(lines)


def get_report_delivery_priority(report_type):
    if report_type == "event_alert":
        return "high"
    if report_type in {"pre_close_risk", "post_close", "night_review"}:
        return "normal"
    return "normal"


def parse_non_negative_int_env(name, default_value):
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default_value
    try:
        value = int(raw)
    except ValueError:
        return default_value
    return max(0, value)


def resolve_owner_endpoint(priority):
    if priority == "high":
        return (
            os.environ.get("OPENCLAW_OWNER_WEBHOOK_HIGH_PRIORITY")
            or os.environ.get("OPENCLAW_OWNER_ENDPOINT_HIGH_PRIORITY")
            or os.environ.get("OPENCLAW_OWNER_WEBHOOK")
            or os.environ.get("OPENCLAW_OWNER_ENDPOINT")
        )
    return os.environ.get("OPENCLAW_OWNER_WEBHOOK") or os.environ.get("OPENCLAW_OWNER_ENDPOINT")


def build_delivery_retry_config(priority):
    if priority == "high":
        return {
            "max_attempts": max(
                1,
                parse_non_negative_int_env("OPENCLAW_OWNER_DELIVERY_ATTEMPTS_HIGH_PRIORITY", 3),
            ),
            "backoff_ms": parse_non_negative_int_env("OPENCLAW_OWNER_DELIVERY_BACKOFF_MS_HIGH_PRIORITY", 500),
        }
    return {
        "max_attempts": max(1, parse_non_negative_int_env("OPENCLAW_OWNER_DELIVERY_ATTEMPTS", 2)),
        "backoff_ms": parse_non_negative_int_env("OPENCLAW_OWNER_DELIVERY_BACKOFF_MS", 1500),
    }


def deliver_report_to_endpoint(endpoint, report_path, content, report_meta, priority):
    payload = json.dumps(
        {
            "report_path": report_path,
            "content": content,
            "report_id": report_meta.get("report_id"),
            "report_type": report_meta.get("report_type"),
            "parent_report_id": report_meta.get("parent_report_id"),
            "priority": priority,
        },
        ensure_ascii=False,
    ).encode("utf-8")
    request = Request(endpoint, data=payload, headers={"Content-Type": "application/json"})
    with urlopen(request, timeout=10) as response:
        status = getattr(response, "status", 200)
        mode = "webhook_high_priority" if priority == "high" else "webhook"
        return {
            "delivered": 200 <= status < 300,
            "mode": mode,
            "status": status,
            "priority": priority,
        }


def deliver_report_to_owner(report_path, content, report_meta):
    priority = get_report_delivery_priority(report_meta.get("report_type"))
    endpoint = resolve_owner_endpoint(priority)
    if not endpoint:
        return {"delivered": False, "mode": "stdout", "priority": priority, "attempts": 0, "retry_count": 0}
    retry_config = build_delivery_retry_config(priority)
    attempts = 0
    errors = []
    last_result = None
    while attempts < retry_config["max_attempts"]:
        attempts += 1
        try:
            last_result = deliver_report_to_endpoint(endpoint, report_path, content, report_meta, priority)
            if last_result.get("delivered"):
                return {
                    **last_result,
                    "attempts": attempts,
                    "retry_count": max(0, attempts - 1),
                    "errors": errors,
                }
            status = last_result.get("status")
            errors.append(f"http_status={status}")
        except URLError as exc:
            errors.append(str(exc))
            last_result = {
                "delivered": False,
                "mode": "webhook_high_priority" if priority == "high" else "webhook",
                "priority": priority,
                "error": str(exc),
            }
        if attempts < retry_config["max_attempts"] and retry_config["backoff_ms"] > 0:
            time.sleep(retry_config["backoff_ms"] / 1000.0)
    return {
        **(last_result or {"delivered": False, "mode": "webhook", "priority": priority}),
        "attempts": attempts,
        "retry_count": max(0, attempts - 1),
        "errors": errors,
        "error": (last_result or {}).get("error") or (errors[-1] if errors else None),
    }


def emit_archived_report_bundle(report_root, payload):
    report_content = build_report_markdown(payload)
    team_report = build_screener_team_report(payload)
    archived_report = archive_team_report(report_root, team_report, report_content)
    delivery = deliver_report_to_owner(archived_report["markdown_path"], report_content, team_report["meta"])
    archived_report = update_archived_report_delivery(report_root, team_report, report_content, delivery)
    return {
        "team_report": team_report,
        "content": report_content,
        "delivery": delivery,
        "archived": archived_report,
    }


def sync_report_retry_queue(report_root, bundle):
    report = bundle["team_report"]
    archived = bundle["archived"]
    delivery = bundle["delivery"]
    report_id = report["meta"]["report_id"]
    if delivery.get("delivered") or delivery.get("mode") == "stdout":
        remove_retry_queue_entry(report_root, report_id)
        return None
    return upsert_retry_queue_entry(
        report_root,
        report,
        archived["json_path"],
        archived["markdown_path"],
        delivery,
    )


def retry_report_entries(report_root, entries):
    queue_path = list_retry_queue(report_root, due_only=False, limit=0)["queue_path"]
    results = []
    for entry in entries:
        report_id = entry.get("report_id")
        json_path = entry.get("json_path")
        markdown_path = entry.get("markdown_path")
        report = load_json_file(json_path, None)
        if not report:
            remove_retry_queue_entry(report_root, report_id)
            results.append(
                {
                    "report_id": report_id,
                    "ok": False,
                    "removed_from_queue": True,
                    "error": f"missing_json:{json_path}",
                }
            )
            continue
        try:
            content = Path(markdown_path).read_text(encoding="utf-8")
        except OSError as exc:
            remove_retry_queue_entry(report_root, report_id)
            results.append(
                {
                    "report_id": report_id,
                    "report_type": report["meta"].get("report_type"),
                    "ok": False,
                    "removed_from_queue": True,
                    "error": str(exc),
                }
            )
            continue
        delivery = deliver_report_to_owner(markdown_path, content, report["meta"])
        archived = update_archived_report_delivery(report_root, report, content, delivery)
        bundle = {"team_report": report, "archived": archived, "delivery": delivery}
        retry_entry = sync_report_retry_queue(report_root, bundle)
        results.append(
            {
                "report_id": report["meta"]["report_id"],
                "report_type": report["meta"]["report_type"],
                "delivered": delivery.get("delivered"),
                "mode": delivery.get("mode"),
                "priority": delivery.get("priority"),
                "delivery_attempt": delivery.get("attempts"),
                "retry_count": delivery.get("retry_count"),
                "error": delivery.get("error"),
                "retry_queue": retry_entry,
            }
        )
    return {
        "retried": len(results),
        "results": results,
        "queue_path": queue_path,
    }


def process_due_retry_queue(report_root, limit=5):
    due_queue = list_retry_queue(report_root, due_only=True, limit=int(limit))
    due_entries = due_queue["entries"]
    if not due_entries:
        return {
            "processed": False,
            "retried": 0,
            "delivered": 0,
            "failed": 0,
            "queue_path": due_queue["queue_path"],
            "due_total": 0,
        }
    retried = retry_report_entries(report_root, due_entries)
    delivered = len([item for item in retried["results"] if item.get("delivered")])
    failed = len([item for item in retried["results"] if not item.get("delivered")])
    return {
        "processed": True,
        "retried": retried["retried"],
        "delivered": delivered,
        "failed": failed,
        "queue_path": retried["queue_path"],
        "due_total": len(due_entries),
        "results": retried["results"],
    }


def resolve_date_window(args):
    lookback_days = int(args.lookback_days)
    end = args.end or datetime.now().strftime("%Y-%m-%d")
    start = args.start or (datetime.strptime(end, "%Y-%m-%d") - timedelta(days=lookback_days * 2)).strftime("%Y-%m-%d")
    return start, end


def build_auto_universe(symbols, start, end, args):
    quote_rows = fetch_quotes(symbols)
    quote_map = {}
    for row in quote_rows:
        symbol = extract_quote_symbol(row)
        if symbol:
            quote_map[symbol] = row
    min_turnover = float(args.auto_min_turnover)
    min_volume = float(args.auto_min_volume)
    min_price = float(args.auto_min_price)
    vol_min = float(args.auto_volatility_min)
    vol_max = float(args.auto_volatility_max)
    dd_max = float(args.auto_max_drawdown)
    quote_passed = []
    excluded = []
    for symbol in symbols:
        row = quote_map.get(symbol, {})
        liquidity = extract_quote_liquidity(row)
        volume = pick_metric(row, ["volume", "turnover_vol"])
        price = pick_metric(row, ["last_price", "price", "last", "close", "close_price", "prev_close"])
        reasons = []
        if min_turnover > 0 and (liquidity is None or liquidity < min_turnover):
            reasons.append("turnover")
        if min_volume > 0 and (volume is None or volume < min_volume):
            reasons.append("volume")
        if min_price > 0 and (price is None or price < min_price):
            reasons.append("price")
        payload = {
            "symbol": symbol,
            "turnover": liquidity,
            "volume": volume,
            "price": price,
        }
        if reasons:
            payload["reasons"] = reasons
            excluded.append(payload)
        else:
            quote_passed.append(payload)
    kline_passed = []
    for item in quote_passed:
        symbol = item["symbol"]
        kline_rows = fetch_kline(symbol, start, end, args.ktype, args.autype, args.max_count)
        prices = extract_prices(kline_rows)
        volatility = compute_volatility(prices) if prices else None
        max_drawdown = compute_max_drawdown(prices) if prices else None
        reasons = []
        if vol_min > 0 and (volatility is None or volatility < vol_min):
            reasons.append("volatility_min")
        if vol_max > 0 and volatility is not None and volatility > vol_max:
            reasons.append("volatility_max")
        if dd_max > 0 and max_drawdown is not None and max_drawdown > dd_max:
            reasons.append("max_drawdown")
        item["volatility_252d"] = volatility
        item["max_drawdown"] = max_drawdown
        if reasons:
            item["reasons"] = reasons
            excluded.append(item)
        else:
            kline_passed.append(item)
    return [item["symbol"] for item in kline_passed], {"passed": kline_passed, "excluded": excluded}


def normalize_weights(weights, available_keys):
    total = sum(weights.get(k, 0.0) for k in available_keys)
    if total <= 0:
        return {k: 0.0 for k in available_keys}
    return {k: weights.get(k, 0.0) / total for k in available_keys}


def adjust_thresholds_for_target(thresholds, monthly_target):
    if monthly_target is None:
        return thresholds
    target_adj = clamp((monthly_target - 0.01) / 0.01, -0.5, 0.5)
    adjusted = dict(thresholds)
    for key, delta in [
        ("quality", 5.0 * target_adj),
        ("growth", 5.0 * target_adj),
        ("momentum", 10.0 * target_adj),
        ("valuation", -5.0 * target_adj),
        ("risk", -5.0 * target_adj),
        ("overall", 5.0 * target_adj),
    ]:
        if key in adjusted:
            adjusted[key] = clamp(adjusted[key] + delta, 0.0, 100.0)
    return adjusted


def compute_target_stress(monthly_target, recent_1m_return, recent_3m_return):
    if monthly_target is None:
        return None
    implied_daily = (1 + monthly_target) ** (1 / 21) - 1
    gap_1m = None if recent_1m_return is None else monthly_target - recent_1m_return
    gap_3m = None if recent_3m_return is None else monthly_target - recent_3m_return
    return {
        "monthly_target": monthly_target,
        "implied_daily_return": implied_daily,
        "recent_1m_return": recent_1m_return,
        "recent_3m_return": recent_3m_return,
        "gap_1m": gap_1m,
        "gap_3m": gap_3m,
    }


def compute_position_budget(base_position, max_position, risk_budget, monthly_target, monthly_vol):
    if monthly_vol is None or monthly_vol == 0:
        return None
    scale = clamp(monthly_target / 0.01 if monthly_target else 1.0, 0.5, 1.5)
    raw = (risk_budget / monthly_vol) * scale
    return clamp(raw, base_position, max_position)


def normalize_sector_map(raw_map):
    if not raw_map:
        return {}
    normalized = {}
    for symbol, sector in raw_map.items():
        normalized[normalize_hk_symbol(symbol)] = str(sector)
    return normalized


def parse_float_map(raw_map):
    if not raw_map:
        return {}
    parsed = {}
    for key, value in raw_map.items():
        parsed[str(key)] = float(value)
    return parsed


def build_symbol_caps(screened, args, capacity_caps):
    symbol_caps = {}
    for item in screened:
        symbol = item["symbol"]
        budget_cap = item.get("metrics", {}).get("position_budget")
        caps = [float(args.max_position), capacity_caps.get(symbol, float(args.max_position))]
        if budget_cap is not None:
            caps.append(budget_cap)
        symbol_caps[symbol] = min(caps)
    return symbol_caps


def pick_projection_metric(projection):
    if not projection:
        return None
    return projection.get("projected_1m_return")


def allocate_portfolio(screened, sector_map, sector_caps, symbol_caps):
    symbols = [item["symbol"] for item in screened]
    scores = {}
    for item in screened:
        overall = item.get("scores", {}).get("overall")
        scores[item["symbol"]] = overall if overall is not None else 0.0
    total_score = sum(scores.values())
    weights = {}
    if total_score <= 0:
        equal = 1.0 / len(symbols) if symbols else 0.0
        for symbol in symbols:
            weights[symbol] = equal
    else:
        for symbol in symbols:
            weights[symbol] = scores.get(symbol, 0.0) / total_score
    for symbol in symbols:
        cap = symbol_caps.get(symbol, 1.0)
        if weights[symbol] > cap:
            weights[symbol] = cap
    if sector_caps:
        sector_totals = {}
        for symbol in symbols:
            sector = sector_map.get(symbol, "未分类")
            sector_totals[sector] = sector_totals.get(sector, 0.0) + weights[symbol]
        for sector, cap in sector_caps.items():
            if sector in sector_totals and sector_totals[sector] > cap:
                scale = cap / sector_totals[sector]
                for symbol in symbols:
                    if sector_map.get(symbol, "未分类") == sector:
                        weights[symbol] *= scale
    total_weight = sum(weights.values())
    cash_weight = 1.0 - total_weight if total_weight < 1.0 else 0.0
    sector_summary = {}
    for symbol in symbols:
        sector = sector_map.get(symbol, "未分类")
        sector_summary[sector] = sector_summary.get(sector, 0.0) + weights[symbol]
    allocation = []
    for item in screened:
        symbol = item["symbol"]
        allocation.append(
            {
                "symbol": symbol,
                "sector": sector_map.get(symbol, "未分类"),
                "score": scores.get(symbol),
                "suggested_weight": weights.get(symbol, 0.0),
                "cap": symbol_caps.get(symbol, 1.0),
            }
        )
    allocation.sort(key=lambda x: x["suggested_weight"], reverse=True)
    return allocation, sector_summary, cash_weight


def compute_portfolio_target_projection(allocation, screened, monthly_target):
    symbol_map = {item["symbol"]: item for item in screened}
    weighted_return_1m = 0.0
    weight_coverage = 0.0
    for item in allocation:
        symbol = item["symbol"]
        weight = item["suggested_weight"]
        metrics = symbol_map.get(symbol, {}).get("metrics", {})
        recent = metrics.get("return_21d")
        if recent is not None:
            weighted_return_1m += weight * recent
            weight_coverage += weight
    projected = None if weight_coverage == 0 else weighted_return_1m / weight_coverage
    gap = None if projected is None else monthly_target - projected
    return {
        "monthly_target": monthly_target,
        "projected_1m_return": projected,
        "coverage_weight": weight_coverage,
        "gap": gap,
    }


def compute_market_signal(symbol, start, end, ktype, autype, max_count, return_21, return_63):
    kline_rows = fetch_kline(symbol, start, end, ktype, autype, max_count)
    prices = extract_prices(kline_rows)
    r21 = compute_returns(prices, 21) if prices else None
    r63 = compute_returns(prices, 63) if prices else None
    passed = True
    if r21 is not None and r21 < return_21:
        passed = False
    if r63 is not None and r63 < return_63:
        passed = False
    return {
        "symbol": symbol,
        "return_21d": r21,
        "return_63d": r63,
        "passed": passed,
    }


def compute_signal_candidates(symbols, start, end, ktype, autype, max_count, args):
    candidates = []
    for symbol in symbols:
        kline_rows = fetch_kline(symbol, start, end, ktype, autype, max_count)
        prices = extract_prices(kline_rows)
        r21 = compute_returns(prices, 21) if prices else None
        r63 = compute_returns(prices, 63) if prices else None
        max_drawdown = compute_max_drawdown(prices) if prices else None
        volatility = compute_volatility(prices) if prices else None
        passed = True
        if r21 is None or r21 < float(args.signal_return_21):
            passed = False
        if r63 is None or r63 < float(args.signal_return_63):
            passed = False
        if max_drawdown is not None and max_drawdown > float(args.signal_max_drawdown):
            passed = False
        score = 0.0
        if r21 is not None:
            score += r21 * 0.6
        if r63 is not None:
            score += r63 * 0.4
        if max_drawdown is not None:
            score -= max_drawdown * 0.3
        candidates.append(
            {
                "symbol": symbol,
                "return_21d": r21,
                "return_63d": r63,
                "max_drawdown": max_drawdown,
                "volatility_252d": volatility,
                "score": score,
                "passed": passed,
            }
        )
    passed = [item for item in candidates if item["passed"]]
    passed.sort(key=lambda x: x["score"], reverse=True)
    if int(args.signal_top) > 0:
        passed = passed[: int(args.signal_top)]
    return passed, candidates


def build_quality_info(rows, metric_keys):
    missing_fields = []
    for row in rows:
        missing = [key for key in metric_keys if row.get(key) is None]
        if missing:
            missing_fields.append({"symbol": row.get("symbol"), "fields": missing})
    return {
        "missing_fields": missing_fields,
        "outliers": [],
        "source_checks": [],
    }


def build_summary_info(screened, dropped):
    highlights = []
    for item in sorted(screened, key=lambda x: x.get("scores", {}).get("overall") or 0.0, reverse=True)[:3]:
        overall = item.get("scores", {}).get("overall")
        if overall is None:
            continue
        highlights.append(f"{item.get('symbol')}:{overall:.1f}")
    risks = []
    if dropped:
        risks.append(f"dropped:{len(dropped)}")
    next_actions = []
    if not screened:
        next_actions.append("lower_thresholds_or_expand_universe")
    return {
        "highlights": highlights,
        "risks": risks,
        "next_actions": next_actions,
    }


def resolve_cache_dir(cache_dir):
    if cache_dir:
        return Path(cache_dir).expanduser().resolve()
    return get_report_root() / "cache"


def next_cache_version(cache_dir, stage, scope, date_tag):
    pattern = f"{stage}_{scope}_{date_tag}_v*.json"
    versions = []
    for path in cache_dir.glob(pattern):
        stem = path.stem
        if "_v" in stem:
            try:
                versions.append(int(stem.split("_v")[-1]))
            except ValueError:
                continue
    return max(versions) + 1 if versions else 1


def write_cache_payload(cache_dir, stage, scope, date_tag, payload, version=None):
    target_dir = cache_dir / date_tag / stage
    target_dir.mkdir(parents=True, exist_ok=True)
    if version is None:
        version = next_cache_version(target_dir, stage, scope, date_tag)
    filename = f"{stage}_{scope}_{date_tag}_v{version}.json"
    path = target_dir / filename
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False)
    return str(path)


def build_workflow_spec():
    return {
        "数据源": [
            "clawtrade-futu-paper-trade（港股行情与财务指标）",
            "Google News RSS（公司新闻）",
            "HKEX Title Search（正式公告）",
            "HKEX Board Meeting Notifications（财报日历）",
        ],
        "定时任务": build_schedule_spec(),
        "LLM约束策略": {
            "流程解耦": ["数据采集", "信息筛选", "因子分析", "决策汇总", "反思更新"],
            "缓存产物": ["原始数据快照", "清洗后表格", "指标派生结果", "评分与权重", "结论摘要"],
            "质量提升": ["去噪与一致性检查", "缺失补全与异常标注", "多源交叉验证"],
            "上下文控制": ["逐步摘要", "分层裁剪", "只传结构化要点"],
        },
        "缓存规范": {
            "目录": "{openclaw_workspace}/HKAutoTradeReports/cache/{date}/{stage}/",
            "文件名模板": "{stage}_{symbol|portfolio}_{yyyymmdd}_{version}.json",
            "版本规则": "同日同阶段从 v1 递增",
            "通用字段": {
                "meta": {
                    "version": "v1",
                    "as_of": "2025-01-31",
                    "stage": "collect|filter|analyze|decide|review",
                    "scope": "symbol|portfolio",
                    "symbols": ["HK.00700", "HK.09988"],
                },
                "data": {},
                "quality": {
                    "missing_fields": [],
                    "outliers": [],
                    "source_checks": [],
                },
                "summary": {
                    "highlights": [],
                    "risks": [],
                    "next_actions": [],
                },
            },
            "阶段产物": {
                "collect": {
                    "data": {
                        "company": {
                            "HK.00700": {
                                "kline_summary": {},
                                "financial_snapshot": {},
                                "indicator_count": 0,
                            }
                        },
                        "market": {"lookback": {}},
                        "macro": [],
                        "industry": [],
                        "events": [],
                    }
                },
                "filter": {
                    "data": {
                        "eligible_symbols": [],
                        "excluded_symbols": [],
                        "filter_rules": [],
                    }
                },
                "analyze": {
                    "data": {
                        "factors": {},
                        "scores": {},
                        "metrics": {},
                    }
                },
                "decide": {
                    "data": {
                        "rankings": [],
                        "allocation": [],
                        "sector_allocation": {},
                        "cash_weight": 0.0,
                        "target_projection": {},
                    }
                },
                "review": {
                    "data": {
                        "outcomes": [],
                        "deviations": [],
                        "rule_updates": [],
                    }
                },
            },
        },
        "信息搜集": {
            "宏观": ["利率与通胀", "流动性变化", "汇率与美元指数"],
            "行业": ["景气周期", "供需结构", "政策与监管", "竞争格局"],
            "公司": ["商业模式", "核心财务指标", "竞争壁垒"],
            "市场": ["价格趋势", "估值分位", "资金面"],
            "事件": ["财报与指引", "产品与技术催化"],
        },
        "分析建模": {
            "质量": ["ROE/ROIC", "利润率", "资产负债表稳健性"],
            "增长": ["收入与利润增速", "TAM 与渗透率", "单位经济学"],
            "估值": ["相对估值", "估值区间", "安全边际"],
            "动量": ["中短期价格趋势", "相对强弱", "催化节奏"],
            "风险": ["波动率", "回撤", "治理与外部风险"],
            "目标约束": ["月度收益压力测试", "阈值动态调整", "仓位预算"],
        },
        "决策执行": {
            "结论": ["入选/观察/排除"],
            "触发条件": ["买入/加仓/止盈/止损阈值"],
            "仓位": ["基础仓位", "最大仓位", "分批规则"],
            "组合层": ["行业分层", "资金容量", "净值目标"],
        },
        "反思迭代": {
            "结果对照": ["预测假设 vs 实际表现"],
            "偏差归因": ["信息不足", "估值误判", "时机错误", "结构性风险"],
            "规则更新": ["新增指标", "调整权重", "优化信息源"],
            "缓存复盘": ["保留每步产物", "追溯指标变化", "回测修正规则"],
        },
    }


def screen_symbols(args):
    report_root = get_report_root()
    retry_processing = None
    if not getattr(args, "disable_retry_drain", False):
        retry_processing = process_due_retry_queue(report_root, int(getattr(args, "retry_due_limit", 5)))
    default_weights = {
        "quality": 0.25,
        "growth": 0.2,
        "valuation": 0.15,
        "momentum": 0.15,
        "risk": 0.1,
        "information": 0.15,
    }
    default_thresholds = {
        "overall": 60.0,
        "quality": 55.0,
        "growth": 50.0,
        "valuation": 40.0,
        "momentum": 50.0,
        "risk": 40.0,
        "information": 45.0,
    }
    weights = parse_weights(args.weights) or default_weights
    thresholds = parse_thresholds(args.thresholds) or default_thresholds
    monthly_target = float(args.monthly_target)
    thresholds = adjust_thresholds_for_target(thresholds, monthly_target) if args.dynamic_threshold else thresholds
    sector_map = normalize_sector_map(parse_json_map(args.sector_map))
    sector_caps = parse_float_map(parse_json_map(args.sector_caps))
    capacity_caps = parse_float_map(parse_json_map(args.capacity_caps))
    portfolio_target = float(args.portfolio_target) if args.portfolio_target else monthly_target
    lookback_days = int(args.lookback_days)
    start, end = resolve_date_window(args)
    rows = []
    collect_data = {}
    quote_rows = fetch_quotes(args.symbols)
    quote_map = {}
    for row in quote_rows:
        symbol = extract_quote_symbol(row)
        if symbol:
            quote_map[symbol] = row
    for symbol in args.symbols:
        norm_symbol = normalize_hk_symbol(symbol)
        quote_row = quote_map.get(norm_symbol, {})
        company_name = (
            quote_row.get("name")
            or quote_row.get("stock_name")
            or quote_row.get("stock_name_cn")
            or fetch_hkex_stock_lookup(norm_symbol).get("name")
            or norm_symbol
        )
        kline_rows = fetch_kline(norm_symbol, start, end, args.ktype, args.autype, args.max_count)
        prices = extract_prices(kline_rows)
        indicator_rows = fetch_financial_indicators(norm_symbol, args.period, args.start, args.end)
        latest = extract_latest_row(indicator_rows)
        roe = pick_metric(
            latest or {},
            ["roe", "ROE", "净资产收益率(%)", "净资产收益率", "ROE(%)", "股东权益回报率(%)"],
        )
        gross_margin = pick_metric(latest or {}, ["gross_margin", "销售毛利率(%)", "毛利率", "毛利率(%)", "毛利率TTM(%)"])
        net_margin = pick_metric(latest or {}, ["net_margin", "销售净利率(%)", "净利率", "净利率(%)"])
        debt_ratio = pick_metric(latest or {}, ["debt_ratio", "资产负债率(%)", "资产负债率", "总负债/总资产(%)"])
        revenue_growth = pick_metric(
            latest or {},
            ["revenue_growth", "营业收入同比增长率(%)", "营业收入增长率", "营业总收入滚动环比增长(%)"],
        )
        profit_growth = pick_metric(
            latest or {},
            ["profit_growth", "净利润同比增长率(%)", "归母净利润同比增长率(%)", "净利润滚动环比增长(%)"],
        )
        pe_ttm = pick_metric(latest or {}, ["pe_ttm", "PE_TTM", "市盈率(PE)", "市盈率", "PE", "市盈率TTM"])
        pb = pick_metric(latest or {}, ["pb", "PB", "市净率(PB)", "市净率", "市账率"])
        returns_21 = compute_returns(prices, 21) if prices else None
        returns_63 = compute_returns(prices, 63) if prices else None
        returns_126 = compute_returns(prices, 126) if prices else None
        returns_252 = compute_returns(prices, 252) if prices else None
        volatility = compute_volatility(prices) if prices else None
        max_drawdown = compute_max_drawdown(prices) if prices else None
        monthly_vol = compute_monthly_volatility(volatility)
        stress = compute_target_stress(monthly_target, returns_21, returns_63)
        position_budget = compute_position_budget(
            float(args.base_position),
            float(args.max_position),
            float(args.risk_budget),
            monthly_target,
            monthly_vol,
        )
        information = None
        if not getattr(args, "disable_information", False):
            try:
                information = build_information_snapshot(norm_symbol, company_name, args)
            except Exception:
                information = {"available": False, "score": None, "risk_flags": [], "news": {}, "announcements": {}, "earnings_calendar": {}}
        rows.append(
            {
                "symbol": norm_symbol,
                "company_name": company_name,
                "market": "HK",
                "roe": roe,
                "gross_margin": gross_margin,
                "net_margin": net_margin,
                "debt_ratio": debt_ratio,
                "revenue_growth": revenue_growth,
                "profit_growth": profit_growth,
                "pe_ttm": pe_ttm,
                "pb": pb,
                "return_21d": returns_21,
                "return_63d": returns_63,
                "return_126d": returns_126,
                "return_252d": returns_252,
                "volatility_252d": volatility,
                "monthly_volatility": monthly_vol,
                "max_drawdown": max_drawdown,
                "target_stress": stress,
                "position_budget": position_budget,
                "news_score": information.get("news", {}).get("score") if information else None,
                "announcement_score": information.get("announcements", {}).get("score") if information else None,
                "event_score": information.get("earnings_calendar", {}).get("score") if information else None,
                "news_count": len(information.get("news", {}).get("items") or []) if information else None,
                "announcement_count": len(information.get("announcements", {}).get("items") or []) if information else None,
                "next_earnings_in_days": information.get("earnings_calendar", {}).get("next_in_days") if information else None,
                "information_score": information.get("score") if information else None,
                "information_risk_flags": information.get("risk_flags") if information else [],
            }
        )
        last_close = prices[-1] if prices else None
        collect_data[norm_symbol] = {
            "company_name": company_name,
            "kline_summary": {
                "start": start,
                "end": end,
                "count": len(kline_rows),
                "last_close": last_close,
            },
            "financial_snapshot": latest or {},
            "indicator_count": len(indicator_rows),
            "information_snapshot": information or {"available": False},
        }
    quality_metrics = ["roe", "gross_margin", "net_margin", "debt_ratio"]
    growth_metrics = ["revenue_growth", "profit_growth"]
    valuation_metrics = ["pe_ttm", "pb"]
    momentum_metrics = ["return_21d", "return_63d", "return_126d", "return_252d"]
    risk_metrics = ["volatility_252d", "max_drawdown"]
    higher_better = {
        "roe": True,
        "gross_margin": True,
        "net_margin": True,
        "debt_ratio": False,
        "revenue_growth": True,
        "profit_growth": True,
        "pe_ttm": False,
        "pb": False,
        "return_21d": True,
        "return_63d": True,
        "return_126d": True,
        "return_252d": True,
        "volatility_252d": False,
        "max_drawdown": False,
    }
    quality_scores, quality_metric_scores, quality_meta = score_factor(rows, quality_metrics, higher_better)
    growth_scores, growth_metric_scores, growth_meta = score_factor(rows, growth_metrics, higher_better)
    valuation_scores, valuation_metric_scores, valuation_meta = score_factor(rows, valuation_metrics, higher_better)
    momentum_scores, momentum_metric_scores, momentum_meta = score_factor(rows, momentum_metrics, higher_better)
    risk_scores, risk_metric_scores, risk_meta = score_factor(rows, risk_metrics, higher_better)
    information_scores = [row.get("information_score") for row in rows]
    information_meta = []
    for row in rows:
        available_metrics = len(
            [
                item
                for item in (
                    row.get("news_score"),
                    row.get("announcement_score"),
                    row.get("event_score"),
                )
                if item is not None
            ]
        )
        information_meta.append(
            {
                "valid_metrics": available_metrics,
                "total_metrics": 3,
                "coverage": available_metrics / 3 if available_metrics else 0.0,
            }
        )
    factor_map = {
        "quality": quality_scores,
        "growth": growth_scores,
        "valuation": valuation_scores,
        "momentum": momentum_scores,
        "risk": risk_scores,
        "information": information_scores,
    }
    available_factors = [key for key, values in factor_map.items() if any(v is not None for v in values)]
    norm_weights = normalize_weights(weights, available_factors)
    screened = []
    dropped = []
    for idx, row in enumerate(rows):
        factor_scores = {
            "quality": quality_scores[idx],
            "growth": growth_scores[idx],
            "valuation": valuation_scores[idx],
            "momentum": momentum_scores[idx],
            "risk": risk_scores[idx],
            "information": information_scores[idx],
        }
        weighted_scores = []
        for key, score in factor_scores.items():
            weight = norm_weights.get(key, 0.0)
            if score is not None and weight > 0:
                weighted_scores.append(score * weight)
        overall = sum(weighted_scores) if weighted_scores else None
        reasons = []
        for key, threshold in thresholds.items():
            if key == "overall":
                if overall is not None and overall < threshold:
                    reasons.append(f"overall<{threshold}")
                continue
            score_value = factor_scores.get(key)
            if score_value is not None and score_value < threshold:
                reasons.append(f"{key}<{threshold}")
        stress = row.get("target_stress") or {}
        tolerance = float(args.target_tolerance)
        if args.enforce_target and stress.get("recent_1m_return") is not None:
            if stress["recent_1m_return"] < monthly_target * tolerance:
                reasons.append("target_underperform_1m")
        payload = {
            "symbol": row["symbol"],
            "company_name": row.get("company_name"),
            "market": row["market"],
            "metrics": row,
            "information": collect_data.get(row["symbol"], {}).get("information_snapshot"),
            "scores": {**factor_scores, "overall": overall},
            "score_meta": {
                "quality": quality_meta[idx],
                "growth": growth_meta[idx],
                "valuation": valuation_meta[idx],
                "momentum": momentum_meta[idx],
                "risk": risk_meta[idx],
                "information": information_meta[idx],
            },
            "metric_scores": {
                "quality": {k: quality_metric_scores[k][idx] for k in quality_metrics},
                "growth": {k: growth_metric_scores[k][idx] for k in growth_metrics},
                "valuation": {k: valuation_metric_scores[k][idx] for k in valuation_metrics},
                "momentum": {k: momentum_metric_scores[k][idx] for k in momentum_metrics},
                "risk": {k: risk_metric_scores[k][idx] for k in risk_metrics},
                "information": {
                    "news_score": row.get("news_score"),
                    "announcement_score": row.get("announcement_score"),
                    "event_score": row.get("event_score"),
                },
            },
        }
        if reasons:
            payload["reasons"] = reasons
            dropped.append(payload)
        else:
            screened.append(payload)
    focus_sectors = parse_symbol_list(getattr(args, "focus_sectors", None))
    focus_screened = []
    focus_note = None
    if focus_sectors:
        if not sector_map:
            focus_note = "missing_sector_map"
        else:
            focus_screened = [item for item in screened if sector_map.get(item["symbol"]) in focus_sectors]
    base_symbol_caps = build_symbol_caps(screened, args, capacity_caps)
    base_allocation, base_sector_allocation, base_cash_weight = allocate_portfolio(
        screened, sector_map, sector_caps, base_symbol_caps
    )
    base_projection = compute_portfolio_target_projection(base_allocation, screened, portfolio_target)
    base_forecast = compute_portfolio_7d_forecast(base_allocation, screened)
    focus_allocation = []
    focus_sector_allocation = {}
    focus_cash_weight = 0.0
    focus_projection = {}
    focus_forecast = None
    if focus_screened:
        focus_symbol_caps = build_symbol_caps(focus_screened, args, capacity_caps)
        focus_allocation, focus_sector_allocation, focus_cash_weight = allocate_portfolio(
            focus_screened, sector_map, sector_caps, focus_symbol_caps
        )
        focus_projection = compute_portfolio_target_projection(focus_allocation, focus_screened, portfolio_target)
        focus_forecast = compute_portfolio_7d_forecast(focus_allocation, focus_screened)
    base_metric = pick_projection_metric(base_projection)
    focus_metric = pick_projection_metric(focus_projection)
    comparison_metric = "projected_1m_return"
    if base_metric is None and focus_metric is None:
        base_metric = base_forecast
        focus_metric = focus_forecast
        comparison_metric = "forecast_7d"
    selected_source = "base"
    if focus_metric is not None and (base_metric is None or focus_metric > base_metric):
        selected_source = "focus"
    if selected_source == "focus":
        allocation = focus_allocation
        sector_allocation = focus_sector_allocation
        cash_weight = focus_cash_weight
        portfolio_projection = focus_projection
    else:
        allocation = base_allocation
        sector_allocation = base_sector_allocation
        cash_weight = base_cash_weight
        portfolio_projection = base_projection
    decision = {
        "focus_sectors": focus_sectors,
        "focus_screened": len(focus_screened),
        "focus_note": focus_note,
        "selected_source": selected_source,
        "comparison_metric": comparison_metric,
        "base_metric": base_metric,
        "focus_metric": focus_metric,
        "base": {
            "allocation": base_allocation,
            "sector_allocation": base_sector_allocation,
            "cash_weight": base_cash_weight,
            "target_projection": base_projection,
            "forecast_7d": base_forecast,
        },
        "focus": {
            "allocation": focus_allocation,
            "sector_allocation": focus_sector_allocation,
            "cash_weight": focus_cash_weight,
            "target_projection": focus_projection,
            "forecast_7d": focus_forecast,
        }
        if focus_sectors
        else None,
    }
    output_payload = {
        "ok": True,
        "data": {
            "config": {
                "weights": weights,
                "thresholds": thresholds,
                "lookback_days": lookback_days,
                "start": start,
                "end": end,
                "market": "HK",
                "data_source": [
                    "clawtrade-futu-paper-trade",
                    "google-news-rss",
                    "hkex-title-search",
                    "hkex-board-meeting-notifications",
                ],
                "ktype": args.ktype,
                "autype": args.autype,
                "monthly_target": monthly_target,
                "dynamic_threshold": bool(args.dynamic_threshold),
                "target_tolerance": float(args.target_tolerance),
                "risk_budget": float(args.risk_budget),
                "base_position": float(args.base_position),
                "max_position": float(args.max_position),
                "sector_map": sector_map,
                "sector_caps": sector_caps,
                "capacity_caps": capacity_caps,
                "portfolio_target": portfolio_target,
                "focus_sectors": focus_sectors,
                "disable_information": bool(getattr(args, "disable_information", False)),
                "news_lookback_days": int(getattr(args, "news_lookback_days", 14)),
                "news_limit": int(getattr(args, "news_limit", 5)),
                "announcement_lookback_days": int(getattr(args, "announcement_lookback_days", 30)),
                "announcement_limit": int(getattr(args, "announcement_limit", 6)),
                "earnings_lookahead_days": int(getattr(args, "earnings_lookahead_days", 120)),
            },
            "summary": {
                "symbols": len(rows),
                "screened": len(screened),
                "dropped": len(dropped),
                "upcoming_earnings_7d": len(
                    [row for row in rows if row.get("next_earnings_in_days") is not None and row.get("next_earnings_in_days") <= 7]
                ),
                "negative_information_flags": len([row for row in rows if row.get("information_risk_flags")]),
            },
            "screened": screened,
            "dropped": dropped,
            "portfolio": {
                "allocation": allocation,
                "sector_allocation": sector_allocation,
                "cash_weight": cash_weight,
                "target_projection": portfolio_projection,
            },
            "decision": decision,
        },
    }
    if retry_processing:
        output_payload["data"]["retry_processing"] = retry_processing
    if getattr(args, "_auto_context", None):
        output_payload["data"]["auto"] = args._auto_context
    trades = execute_trades(allocation, collect_data, args)
    output_payload["data"]["trades"] = trades
    output_payload["data"]["report_context"] = {
        "report_type": getattr(args, "report_type", None) or "ad_hoc_screen",
        "window_id": getattr(args, "report_window_id", None),
        "task_id": getattr(args, "task_id", None),
        "workflow_id": getattr(args, "workflow_id", None),
        "trigger_type": "scheduled" if getattr(args, "_auto_context", None) else "manual",
    }
    output_payload["data"]["event_alerts"] = detect_event_alerts(output_payload)
    report_dir = resolve_report_dir(getattr(args, "report_dir", None))
    report_content = build_report_markdown(output_payload)
    report_name = f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    report_path = str((report_dir / report_name).resolve())
    Path(report_path).write_text(report_content, encoding="utf-8")
    primary_bundle = emit_archived_report_bundle(report_root, output_payload)
    primary_retry_entry = sync_report_retry_queue(report_root, primary_bundle)
    team_report = primary_bundle["team_report"]
    delivery = primary_bundle["delivery"]
    archived_report = primary_bundle["archived"]
    output_payload["data"]["report"] = {
        "report_id": team_report["meta"]["report_id"],
        "report_type": team_report["meta"]["report_type"],
        "priority": delivery.get("priority"),
        "delivery_attempt": delivery.get("attempts"),
        "retry_count": delivery.get("retry_count"),
        "path": report_path,
        "archive_markdown_path": archived_report["markdown_path"],
        "archive_json_path": archived_report["json_path"],
        "manifest_path": archived_report["manifest_path"],
        "delivered": delivery.get("delivered"),
        "mode": delivery.get("mode"),
        "status": delivery.get("status"),
        "error": delivery.get("error"),
        "delivery_status": archived_report.get("delivery_status"),
    }
    supplemental_reports = []
    report_context = output_payload["data"].get("report_context") or {}
    if output_payload["data"].get("event_alerts") and report_context.get("report_type") != "event_alert":
        event_payload = copy.deepcopy(output_payload)
        event_payload["data"]["report_context"] = {
            **report_context,
            "report_type": "event_alert",
            "parent_report_id": team_report["meta"]["report_id"],
            "trigger_type": "event_driven",
        }
        event_bundle = emit_archived_report_bundle(report_root, event_payload)
        event_retry_entry = sync_report_retry_queue(report_root, event_bundle)
        event_report = event_bundle["team_report"]
        event_delivery = event_bundle["delivery"]
        event_archived = event_bundle["archived"]
        supplemental_reports.append(
            {
                "report_id": event_report["meta"]["report_id"],
                "report_type": event_report["meta"]["report_type"],
                "priority": event_delivery.get("priority"),
                "delivery_attempt": event_delivery.get("attempts"),
                "retry_count": event_delivery.get("retry_count"),
                "parent_report_id": event_report["meta"].get("parent_report_id"),
                "archive_markdown_path": event_archived["markdown_path"],
                "archive_json_path": event_archived["json_path"],
                "manifest_path": event_archived["manifest_path"],
                "delivered": event_delivery.get("delivered"),
                "mode": event_delivery.get("mode"),
                "status": event_delivery.get("status"),
                "error": event_delivery.get("error"),
                "delivery_status": event_archived.get("delivery_status"),
                "retry_queue": event_retry_entry,
            }
        )
    if supplemental_reports:
        output_payload["data"]["supplemental_reports"] = supplemental_reports
    if primary_retry_entry:
        output_payload["data"]["report"]["retry_queue"] = primary_retry_entry
    owner_messages = []
    if not delivery.get("delivered"):
        output_payload["data"]["owner_message"] = report_content
        owner_messages.append(
            {
                "report_id": team_report["meta"]["report_id"],
                "report_type": team_report["meta"]["report_type"],
                "priority": delivery.get("priority"),
                "content": primary_bundle["content"],
            }
        )
    for item in supplemental_reports:
        if not item.get("delivered"):
            try:
                supplemental_content = Path(item["archive_markdown_path"]).read_text(encoding="utf-8")
            except OSError:
                supplemental_content = None
            owner_messages.append(
                {
                    "report_id": item.get("report_id"),
                    "report_type": item.get("report_type"),
                    "priority": item.get("priority"),
                    "content": supplemental_content,
                }
            )
    if owner_messages:
        output_payload["data"]["owner_messages"] = owner_messages
    if args.save_cache:
        stage_list = parse_stage_list(args.cache_stages) or ["decide"]
        allowed = {"collect", "filter", "analyze", "decide", "review"}
        invalid = [stage for stage in stage_list if stage not in allowed]
        if invalid:
            json_out({"ok": False, "error": f"cache_stages 含不支持阶段: {','.join(invalid)}"}, 1)
        cache_dir = resolve_cache_dir(args.cache_dir)
        dt = parse_date(end) or datetime.now()
        date_tag = dt.strftime("%Y%m%d")
        scope = "symbol" if len(args.symbols) == 1 else "portfolio"
        metric_keys = [
            "roe",
            "gross_margin",
            "net_margin",
            "debt_ratio",
            "revenue_growth",
            "profit_growth",
            "pe_ttm",
            "pb",
            "return_21d",
            "return_63d",
            "return_126d",
            "return_252d",
            "volatility_252d",
            "max_drawdown",
        ]
        if not getattr(args, "disable_information", False):
            metric_keys.extend(["news_score", "announcement_score", "event_score", "information_score"])
        quality_info = build_quality_info(rows, metric_keys)
        summary_info = build_summary_info(screened, dropped)
        cache_paths = []
        for stage in stage_list:
            meta = {
                "version": "v1",
                "as_of": end,
                "stage": stage,
                "scope": scope,
                "symbols": [item["symbol"] for item in rows],
            }
            if stage == "collect":
                events = []
                for symbol, snapshot in collect_data.items():
                    info = snapshot.get("information_snapshot") or {}
                    for item in info.get("announcements", {}).get("items") or []:
                        events.append(
                            {
                                "symbol": symbol,
                                "type": "announcement",
                                "date": item.get("release_time"),
                                "title": item.get("title"),
                                "signal": item.get("signal"),
                                "link": item.get("link"),
                            }
                        )
                    for item in info.get("earnings_calendar", {}).get("items") or []:
                        events.append(
                            {
                                "symbol": symbol,
                                "type": "earnings_calendar",
                                "date": item.get("bm_date"),
                                "title": item.get("purpose"),
                                "signal": "negative" if (item.get("days_until") or 999) <= 7 else "neutral",
                            }
                        )
                data = {
                    "company": collect_data,
                    "market": {
                        "lookback": {
                            "start": start,
                            "end": end,
                            "ktype": args.ktype,
                            "autype": args.autype,
                        }
                    },
                    "macro": [],
                    "industry": [],
                    "events": events,
                }
                if getattr(args, "_auto_context", None):
                    data["auto"] = args._auto_context
            elif stage == "filter":
                data = {
                    "eligible_symbols": [item["symbol"] for item in screened],
                    "excluded_symbols": [item["symbol"] for item in dropped],
                    "filter_rules": {
                        "thresholds": thresholds,
                        "monthly_target": monthly_target,
                        "dynamic_threshold": bool(args.dynamic_threshold),
                        "enforce_target": bool(args.enforce_target),
                        "target_tolerance": float(args.target_tolerance),
                    },
                }
            elif stage == "analyze":
                data = {
                    "factors": weights,
                    "scores": {item["symbol"]: item.get("scores") for item in screened},
                    "metrics": {item["symbol"]: item.get("metrics") for item in screened},
                    "information": {item["symbol"]: item.get("information") for item in screened},
                }
            elif stage == "decide":
                rankings = []
                for item in sorted(screened, key=lambda x: x.get("scores", {}).get("overall") or 0.0, reverse=True):
                    rankings.append(
                        {
                            "symbol": item["symbol"],
                            "score": item.get("scores", {}).get("overall"),
                        }
                    )
                data = {
                    "rankings": rankings,
                    "allocation": allocation,
                    "sector_allocation": sector_allocation,
                    "cash_weight": cash_weight,
                    "target_projection": portfolio_projection,
                }
            else:
                outcomes = []
                for item in screened:
                    outcomes.append({"symbol": item["symbol"], "decision": "screened"})
                for item in dropped:
                    outcomes.append(
                        {
                            "symbol": item["symbol"],
                            "decision": "dropped",
                            "reasons": item.get("reasons", []),
                        }
                    )
                deviations = []
                for item in screened:
                    stress = item.get("metrics", {}).get("target_stress") or {}
                    gap_1m = stress.get("gap_1m")
                    if gap_1m is not None and gap_1m > 0:
                        deviations.append({"symbol": item["symbol"], "gap_1m": gap_1m})
                rule_updates = []
                if not screened:
                    rule_updates.append("lower_thresholds_or_expand_universe")
                data = {
                    "outcomes": outcomes,
                    "deviations": deviations,
                    "rule_updates": rule_updates,
                }
            payload = {
                "meta": meta,
                "data": data,
                "quality": quality_info,
                "summary": summary_info,
            }
            path = write_cache_payload(cache_dir, stage, scope, date_tag, payload)
            cache_paths.append(path)
        output_payload["data"]["cache"] = {
            "saved": True,
            "paths": cache_paths,
        }
    if getattr(args, "_return_payload", False):
        return output_payload
    json_out(output_payload)


def output_workflow(_args):
    json_out({"ok": True, "data": build_workflow_spec()})


def output_report_history(args):
    report_root = get_report_root()
    result = query_report_history(
        report_root,
        report_id=getattr(args, "report_id", None),
        report_date=getattr(args, "date", None),
        symbol=normalize_hk_symbol(args.symbol) if getattr(args, "symbol", None) else None,
        report_type=getattr(args, "report_type", None),
        window_id=getattr(args, "window_id", None),
        delivery_status=getattr(args, "delivery_status", None),
        limit=int(getattr(args, "limit", 20)),
    )
    if getattr(args, "include_retry_queue", False):
        queue_all = list_retry_queue(report_root, due_only=False, limit=0)
        queue_due = list_retry_queue(report_root, due_only=True, limit=0)
        queue_map = {item.get("report_id"): item for item in queue_all["entries"]}
        result["entries"] = [{**entry, "retry_queue": queue_map.get(entry.get("report_id"))} for entry in result["entries"]]
        result["retry_queue"] = {
            "queue_path": queue_all["queue_path"],
            "total": queue_all["total"],
            "due_total": queue_due["total"],
        }
    payload = {
        "ok": True,
        "data": result,
    }
    if getattr(args, "show_content", False) and result["entries"]:
        entry = result["entries"][0]
        payload["data"]["content"] = {
            "report_id": entry.get("report_id"),
            "format": args.content_format,
            "value": read_report_content(entry, args.content_format),
        }
    json_out(payload)


def retry_report_delivery(args):
    report_root = get_report_root()
    if getattr(args, "view", False):
        json_out({"ok": True, "data": list_retry_queue(report_root, due_only=bool(getattr(args, "due_only", False)), limit=int(args.limit))})
    queue = list_retry_queue(report_root, due_only=bool(getattr(args, "all_due", False)), limit=int(args.limit))
    entries = queue["entries"]
    if getattr(args, "report_id", None):
        entries = [item for item in list_retry_queue(report_root, due_only=False, limit=0)["entries"] if item.get("report_id") == args.report_id]
    if not entries:
        json_out({"ok": True, "data": {"retried": 0, "results": [], "queue_path": queue["queue_path"]}})
    json_out({"ok": True, "data": retry_report_entries(report_root, entries)})


def manage_universe(args):
    """标的池管理命令"""
    invalid_stocks = get_invalid_universe_stocks()
    # 初始化
    if getattr(args, "init", False):
        data = initialize_universe_json()
        json_out({
            "ok": True,
            "message": "标的池已初始化",
            "data": {
                "hsi_count": data["stats"]["hsi_count"],
                "total_count": data["stats"]["total_count"],
                "path": str(resolve_universe_json_path())
            }
        })
    
    # 刷新恒生指数成分股
    elif getattr(args, "refresh_hsi", False):
        data, msg = refresh_hsi_from_api()
        if data:
            json_out({
                "ok": True,
                "message": msg,
                "data": {
                    "hsi_count": data["stats"]["hsi_count"],
                    "refresh_date": data.get("hsi_refresh_date")
                }
            })
        else:
            json_out({"ok": False, "error": msg})
    
    # 添加股票 - 支持格式: HK.02015 或 "HK.02015,理想汽车"
    elif getattr(args, "add", None):
        added = []
        for item in args.add:
            # 处理 "HK.02015,理想汽车" 格式
            if "," in item:
                parts = item.split(",")
                symbol = parts[0].strip()
                name = parts[1].strip() if len(parts) > 1 else None
            else:
                symbol = item.strip()
                name = None
            data, msg = add_user_stock(symbol, name)
            if data:
                added.append({"symbol": symbol, "message": msg})
        json_out({
            "ok": True,
            "message": f"已添加 {len(added)} 只股票",
            "data": {"added": added}
        })
    
    # 按行业查看
    elif getattr(args, "sectors", False):
        sectors = get_universe_by_sector()
        json_out({
            "ok": True,
            "data": {
                "sectors": sectors,
                "total": sum(len(v) for v in sectors.values()),
                "invalid_stocks": invalid_stocks,
            }
        })
    
    # 默认: 查看标的池
    else:
        data = load_universe_json()
        json_out({
            "ok": True,
            "data": {
                "version": data.get("version"),
                "last_updated": data.get("last_updated"),
                "hsi_count": data["stats"]["hsi_count"],
                "user_count": data["stats"]["user_count"],
                "total_count": data["stats"]["total_count"],
                "hsi_refresh_date": data.get("hsi_refresh_date"),
                "path": str(resolve_universe_json_path()),
                "invalid_stocks": invalid_stocks,
            }
        })


def auto_screen(args):
    if not getattr(args, "allow_non_trading", False):
        calendar = load_trading_calendar(getattr(args, "calendar_file", None))
        now = datetime.now()
        if not is_trading_day(now, calendar):
            json_out(
                {
                    "ok": True,
                    "data": {
                        "skipped": True,
                        "reason": "non_trading_day",
                        "date": now.strftime("%Y-%m-%d"),
                    },
                }
            )
    universe = load_universe_symbols(args)
    start, end = resolve_date_window(args)
    auto_symbols, auto_details = build_auto_universe(universe, start, end, args)
    signal_passed, signal_candidates = compute_signal_candidates(
        auto_symbols,
        start,
        end,
        args.ktype,
        args.autype,
        args.max_count,
        args,
    )
    final_symbols = [item["symbol"] for item in signal_passed]
    args.symbols = final_symbols
    args.start = start
    args.end = end
    args._return_payload = True
    args._auto_context = {
        "universe_total": len(universe),
        "quote_kline_passed": len(auto_symbols),
        "signal_passed": len(final_symbols),
        "signal_candidates": signal_candidates,
        "universe_detail": auto_details,
        "universe_meta": getattr(args, "_universe_meta", {}),
        "signal_thresholds": {
            "return_21d": float(args.signal_return_21),
            "return_63d": float(args.signal_return_63),
            "max_drawdown": float(args.signal_max_drawdown),
            "top": int(args.signal_top),
        },
    }
    payload = screen_symbols(args)
    json_out(payload)


def build_tcc_result_payload(payload, command=None):
    result = {
        "ok": bool(payload.get("ok")),
        "command": command,
    }
    if payload.get("error"):
        result["error"] = payload.get("error")
    if payload.get("message"):
        result["message"] = payload.get("message")
    data = payload.get("data") or {}
    summary = data.get("summary")
    if summary:
        result["summary"] = summary
    report = data.get("report") or {}
    if report.get("path"):
        result["report"] = {
            "report_id": report.get("report_id"),
            "report_type": report.get("report_type"),
            "priority": report.get("priority"),
            "delivery_attempt": report.get("delivery_attempt"),
            "retry_count": report.get("retry_count"),
            "path": report.get("path"),
            "archive_markdown_path": report.get("archive_markdown_path"),
            "archive_json_path": report.get("archive_json_path"),
            "manifest_path": report.get("manifest_path"),
            "delivered": report.get("delivered"),
            "mode": report.get("mode"),
            "delivery_status": report.get("delivery_status"),
        }
    supplemental_reports = data.get("supplemental_reports") or []
    if supplemental_reports:
        result["supplemental_reports"] = supplemental_reports
    retry_queue = data.get("retry_queue") or data.get("report", {}).get("retry_queue")
    if retry_queue:
        result["retry_queue"] = retry_queue
    retry_processing = data.get("retry_processing")
    if retry_processing:
        result["retry_processing"] = {
            "processed": retry_processing.get("processed"),
            "retried": retry_processing.get("retried"),
            "delivered": retry_processing.get("delivered"),
            "failed": retry_processing.get("failed"),
            "due_total": retry_processing.get("due_total"),
            "queue_path": retry_processing.get("queue_path"),
        }
    portfolio = data.get("portfolio") or {}
    allocation = portfolio.get("allocation") or []
    if allocation:
        result["selected_symbols"] = [item.get("symbol") for item in allocation if item.get("symbol")]
    trades = data.get("trades") or {}
    orders = trades.get("orders") or []
    if trades:
        result["trades"] = {
            "enabled": trades.get("enabled"),
            "submitted": len([item for item in orders if item.get("status") == "submitted"]),
            "failed": len([item for item in orders if item.get("status") == "failed"]),
            "skipped": len([item for item in orders if item.get("status") == "skipped"]),
        }
    auto_ctx = data.get("auto") or {}
    if auto_ctx:
        result["auto"] = {
            "universe_total": auto_ctx.get("universe_total"),
            "signal_passed": auto_ctx.get("signal_passed"),
        }
    return result


def payload_to_argv(task_id, payload):
    if not isinstance(payload, dict):
        raise RuntimeError("TCC payload 必须为 JSON 对象")
    command = payload.get("command") or payload.get("subcommand") or payload.get("action")
    if not command:
        raise RuntimeError("TCC payload 缺少 command")
    raw_args = payload.get("args") if isinstance(payload.get("args"), dict) else payload
    ignore_keys = {
        "command",
        "subcommand",
        "action",
        "args",
        "task_id",
        "workflow_id",
        "project_path",
        "project_name",
        "project_type",
        "user_prompt",
        "target_skill",
    }
    argv = ["--task_id", task_id, str(command)]
    for key, value in raw_args.items():
        if key in ignore_keys or value is None:
            continue
        option = f"--{str(key).replace('_', '-')}"
        if isinstance(value, bool):
            if value:
                argv.append(option)
            continue
        argv.append(option)
        if isinstance(value, (list, tuple)):
            argv.extend(str(item) for item in value)
        elif isinstance(value, dict):
            argv.append(json.dumps(value, ensure_ascii=False))
        else:
            argv.append(str(value))
    return argv


def prepare_argv(argv):
    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument("--task_id")
    pre_parser.add_argument("--payload-json")
    known, _ = pre_parser.parse_known_args(argv)
    task_id = known.task_id
    payload = {}
    if known.payload_json:
        payload = json.loads(known.payload_json)
    if not task_id:
        return argv, None, payload
    task_payload = get_tcc_task_payload(task_id)
    merged_payload = dict(task_payload)
    merged_payload.update(payload)
    commands = {"workflow", "universe", "schedule", "screen", "auto", "reports", "report-retry", "run-scheduled", "scheduled-runs", "cron-export", "cron-install", "schedule-preferences", "openclaw-automation-spec", "report-settings", "report-settings-request"}
    if any(token in commands for token in argv):
        return argv, task_id, merged_payload
    return payload_to_argv(task_id, merged_payload), task_id, merged_payload


def build_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--task_id")
    parser.add_argument("--payload-json")
    sub = parser.add_subparsers(dest="command", required=True)
    workflow = sub.add_parser("workflow")
    workflow.set_defaults(func=output_workflow)
    
    # 标的池管理命令
    universe_cmd = sub.add_parser("universe")
    universe_cmd.add_argument("--view", action="store_true", help="查看当前标的池")
    universe_cmd.add_argument("--add", nargs="+", help="添加股票到标的池 (代码 [名称])")
    universe_cmd.add_argument("--refresh-hsi", action="store_true", help="从API刷新恒生指数成分股")
    universe_cmd.add_argument("--sectors", action="store_true", help="按行业查看标的池")
    universe_cmd.add_argument("--init", action="store_true", help="初始化标的池JSON文件")
    universe_cmd.set_defaults(func=manage_universe)
    
    schedule = sub.add_parser("schedule")
    schedule.add_argument("--schedule-start")
    schedule.add_argument("--schedule-end")
    schedule.add_argument("--schedule-interval")
    schedule.add_argument("--schedule-timezone")
    schedule.add_argument("--schedule-days")
    schedule.add_argument("--schedule-command")
    schedule.add_argument("--calendar-file")
    schedule.add_argument("--retry-schedule-start")
    schedule.add_argument("--retry-schedule-end")
    schedule.add_argument("--retry-schedule-interval")
    schedule.add_argument("--retry-limit", default="10")
    schedule.add_argument("--disable-retry-maintenance", action="store_true")
    schedule.set_defaults(func=output_schedule)

    schedule_preferences = sub.add_parser("schedule-preferences")
    schedule_preferences.add_argument("--view", action="store_true")
    schedule_preferences.add_argument("--reset", action="store_true")
    schedule_preferences.add_argument("--profile", choices=["default", "compact", "minimal"])
    schedule_preferences.add_argument("--scheduler-mode", choices=["openclaw_primary", "cron_primary"])
    schedule_preferences.add_argument("--enable-report-types", nargs="+")
    schedule_preferences.add_argument("--disable-report-types", nargs="+")
    schedule_preferences.add_argument("--report-times")
    schedule_preferences.add_argument("--enable-retry-maintenance", action="store_true")
    schedule_preferences.add_argument("--disable-retry-maintenance", action="store_true")
    schedule_preferences.add_argument("--retry-schedule-start")
    schedule_preferences.add_argument("--retry-schedule-end")
    schedule_preferences.add_argument("--retry-schedule-interval")
    schedule_preferences.add_argument("--retry-limit")
    schedule_preferences.set_defaults(func=manage_schedule_preferences)

    run_scheduled = sub.add_parser("run-scheduled")
    run_scheduled.add_argument("--job")
    run_scheduled.add_argument("--list", action="store_true")
    run_scheduled.add_argument("--dry-run", action="store_true")
    run_scheduled.set_defaults(func=run_scheduled_job)

    scheduled_runs = sub.add_parser("scheduled-runs")
    scheduled_runs.add_argument("--job")
    scheduled_runs.add_argument("--status")
    scheduled_runs.add_argument("--limit", default="20")
    scheduled_runs.set_defaults(func=output_scheduled_runs)

    cron_export = sub.add_parser("cron-export")
    cron_export.add_argument("--output")
    cron_export.add_argument("--python-path")
    cron_export.add_argument("--base-dir")
    cron_export.set_defaults(func=export_cron)

    cron_install = sub.add_parser("cron-install")
    cron_install.add_argument("--output")
    cron_install.add_argument("--python-path")
    cron_install.add_argument("--base-dir")
    cron_install.set_defaults(func=install_cron)

    automation_spec = sub.add_parser("openclaw-automation-spec")
    automation_spec.set_defaults(func=output_openclaw_automation_spec)

    report_settings = sub.add_parser("report-settings")
    report_settings.add_argument("--format", choices=["json", "markdown"], default="json")
    report_settings.set_defaults(func=output_report_settings)

    report_settings_request = sub.add_parser("report-settings-request")
    report_settings_request.add_argument("--message", required=True)
    report_settings_request.add_argument("--dry-run", action="store_true")
    report_settings_request.add_argument("--format", choices=["json", "markdown"], default="json")
    report_settings_request.set_defaults(func=output_report_settings_request)

    screen = sub.add_parser("screen")
    screen.add_argument("--symbols", nargs="+", required=True)
    screen.add_argument("--start")
    screen.add_argument("--end")
    screen.add_argument("--lookback-days", default="360")
    screen.add_argument("--weights")
    screen.add_argument("--thresholds")
    screen.add_argument("--period", default="QUARTER")
    screen.add_argument("--ktype", default="DAY")
    screen.add_argument("--autype", default="QFQ")
    screen.add_argument("--max-count", default="1000")
    screen.add_argument("--monthly-target", default="0.01")
    screen.add_argument("--dynamic-threshold", action="store_true")
    screen.add_argument("--enforce-target", action="store_true")
    screen.add_argument("--target-tolerance", default="0.5")
    screen.add_argument("--risk-budget", default="0.02")
    screen.add_argument("--base-position", default="0.05")
    screen.add_argument("--max-position", default="0.15")
    screen.add_argument("--sector-map")
    screen.add_argument("--sector-caps")
    screen.add_argument("--capacity-caps")
    screen.add_argument("--portfolio-target")
    screen.add_argument("--save-cache", action="store_true")
    screen.add_argument("--cache-dir")
    screen.add_argument("--cache-stages", default="filter,analyze,decide")
    screen.add_argument("--report-dir")
    screen.add_argument("--report-type", default="ad_hoc_screen")
    screen.add_argument("--report-window-id")
    screen.add_argument("--disable-retry-drain", action="store_true")
    screen.add_argument("--retry-due-limit", default="5")
    screen.add_argument("--focus-sectors")
    screen.add_argument("--disable-information", action="store_true")
    screen.add_argument("--news-lookback-days", default="14")
    screen.add_argument("--news-limit", default="5")
    screen.add_argument("--announcement-lookback-days", default="30")
    screen.add_argument("--announcement-limit", default="6")
    screen.add_argument("--earnings-lookahead-days", default="120")
    screen.add_argument("--auto-trade", action="store_true")
    screen.add_argument("--trade-qty")
    screen.add_argument("--trade-budget")
    screen.add_argument("--trade-side", default="buy")
    screen.add_argument("--trade-order-type", default="limit")
    screen.add_argument("--trade-price-mode", default="last")
    screen.set_defaults(func=screen_symbols)
    auto = sub.add_parser("auto")
    auto.add_argument("--universe", nargs="+")
    auto.add_argument("--universe-file")
    auto.add_argument("--start")
    auto.add_argument("--end")
    auto.add_argument("--lookback-days", default="360")
    auto.add_argument("--weights")
    auto.add_argument("--thresholds")
    auto.add_argument("--period", default="QUARTER")
    auto.add_argument("--ktype", default="DAY")
    auto.add_argument("--autype", default="QFQ")
    auto.add_argument("--max-count", default="1000")
    auto.add_argument("--monthly-target", default="0.01")
    auto.add_argument("--dynamic-threshold", action="store_true")
    auto.add_argument("--enforce-target", action="store_true")
    auto.add_argument("--target-tolerance", default="0.5")
    auto.add_argument("--risk-budget", default="0.02")
    auto.add_argument("--base-position", default="0.05")
    auto.add_argument("--max-position", default="0.15")
    auto.add_argument("--sector-map")
    auto.add_argument("--sector-caps")
    auto.add_argument("--capacity-caps")
    auto.add_argument("--focus-sectors")
    auto.add_argument("--portfolio-target")
    auto.add_argument("--save-cache", action="store_true")
    auto.add_argument("--cache-dir")
    auto.add_argument("--cache-stages", default="filter,analyze,decide")
    auto.add_argument("--report-dir")
    auto.add_argument("--report-type", default="pre_trade")
    auto.add_argument("--report-window-id")
    auto.add_argument("--disable-retry-drain", action="store_true")
    auto.add_argument("--retry-due-limit", default="5")
    auto.add_argument("--disable-information", action="store_true")
    auto.add_argument("--news-lookback-days", default="14")
    auto.add_argument("--news-limit", default="5")
    auto.add_argument("--announcement-lookback-days", default="30")
    auto.add_argument("--announcement-limit", default="6")
    auto.add_argument("--earnings-lookahead-days", default="120")
    auto.add_argument("--auto-trade", action="store_true")
    auto.add_argument("--trade-qty")
    auto.add_argument("--trade-budget")
    auto.add_argument("--trade-side", default="buy")
    auto.add_argument("--trade-order-type", default="limit")
    auto.add_argument("--trade-price-mode", default="last")
    auto.add_argument("--auto-min-turnover", default="0")
    auto.add_argument("--auto-min-volume", default="0")
    auto.add_argument("--auto-min-price", default="0")
    auto.add_argument("--auto-volatility-min", default="0")
    auto.add_argument("--auto-volatility-max", default="0")
    auto.add_argument("--auto-max-drawdown", default="0")
    auto.add_argument("--signal-return-21", default="0")
    auto.add_argument("--signal-return-63", default="0")
    auto.add_argument("--signal-max-drawdown", default="0.4")
    auto.add_argument("--signal-top", default="50")
    auto.add_argument("--calendar-file")
    auto.add_argument("--allow-non-trading", action="store_true")
    auto.set_defaults(func=auto_screen)

    reports = sub.add_parser("reports")
    reports.add_argument("--report-id")
    reports.add_argument("--date")
    reports.add_argument("--symbol")
    reports.add_argument("--report-type")
    reports.add_argument("--window-id")
    reports.add_argument("--delivery-status")
    reports.add_argument("--limit", default="20")
    reports.add_argument("--include-retry-queue", action="store_true")
    reports.add_argument("--show-content", action="store_true")
    reports.add_argument("--content-format", choices=["markdown", "json"], default="markdown")
    reports.set_defaults(func=output_report_history)

    report_retry = sub.add_parser("report-retry")
    report_retry.add_argument("--view", action="store_true")
    report_retry.add_argument("--due-only", action="store_true")
    report_retry.add_argument("--all-due", action="store_true")
    report_retry.add_argument("--report-id")
    report_retry.add_argument("--limit", default="20")
    report_retry.set_defaults(func=retry_report_delivery)
    return parser


def main(argv=None):
    global TCC_CAPTURE_MODE
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument("--task_id")
    known, _ = pre_parser.parse_known_args(raw_argv)
    task_id = known.task_id
    TCC_CAPTURE_MODE = bool(task_id)
    command = None
    try:
        prepared_argv, task_id, _task_payload = prepare_argv(raw_argv)
        ensure_venv()
        parser = build_parser()
        args = parser.parse_args(prepared_argv)
        command = getattr(args, "command", None)
        with runtime_execution_lock(command, task_id):
            args.func(args)
        return 0
    except SkillExit as exc:
        if task_id:
            result = build_tcc_result_payload(exc.payload, command)
            if exc.exit_code == 0:
                update_tcc_task_status(task_id, "completed", result)
            else:
                error_message = exc.payload.get("error") if isinstance(exc.payload, dict) else str(exc)
                record_tcc_task_failure(task_id, error_message or "skill_failed", result)
        return exc.exit_code
    except SystemExit as exc:
        exit_code = exc.code if isinstance(exc.code, int) else 1
        if task_id and exit_code != 0:
            error_payload = {"ok": False, "error": "参数解析失败"}
            record_tcc_task_failure(task_id, error_payload["error"], build_tcc_result_payload(error_payload, command))
        return exit_code
    except Exception as exc:
        payload = {"ok": False, "error": str(exc)}
        print(json.dumps(payload, ensure_ascii=False))
        if task_id:
            record_tcc_task_failure(task_id, str(exc), build_tcc_result_payload(payload, command))
            return 1
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
