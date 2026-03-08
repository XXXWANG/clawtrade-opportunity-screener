import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen


def json_out(payload, exit_code=0):
    print(json.dumps(payload, ensure_ascii=False))
    sys.exit(exit_code)


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
    if python_path.exists():
        venv_version = get_python_version(str(python_path))
        if not is_compatible_python(venv_version):
            shutil.rmtree(venv_dir, ignore_errors=True)
    if not python_path.exists():
        subprocess.check_call([python_cmd, "-m", "venv", str(venv_dir)])
    if not pip_path.exists():
        raise RuntimeError("虚拟环境创建失败")
    requirements = base_dir / "requirements.txt"
    subprocess.check_call([str(pip_path), "install", "-r", str(requirements)])
    env = os.environ.copy()
    env["SCREEN_SKILL_VENV"] = "1"
    subprocess.check_call([str(python_path), str(Path(__file__).resolve()), *sys.argv[1:]], env=env)
    sys.exit(0)


def resolve_futu_skill_path():
    env_path = os.environ.get("FUTU_PAPER_TRADE_DIR") or os.environ.get("FUTU_PAPER_TRADE_PATH")
    if env_path:
        base_dir = Path(env_path).expanduser().resolve()
    else:
        base_dir = Path(__file__).resolve().parent.parent / "clawtrade-futu-paper-trade"
    script = base_dir / "futu_skill.py"
    if not script.exists():
        json_out(
            {
                "ok": False,
                "error": "未找到 clawtrade-futu-paper-trade 技能入口",
                "next_steps": [
                    "确认 clawtrade-futu-paper-trade 技能已安装",
                    "设置 FUTU_PAPER_TRADE_DIR 指向 clawtrade-futu-paper-trade 目录",
                ],
            },
            1,
        )
    return script


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


def normalize_hk_symbol(symbol):
    upper = symbol.upper()
    if upper.startswith("HK."):
        code = upper.split(".", 1)[1]
        return "HK." + code.zfill(5)
    if "." in upper:
        json_out({"ok": False, "error": "仅支持港股代码，格式应为 HK.00700"}, 1)
    return "HK." + upper.zfill(5)


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
        scores[idx] = score * 100
    return scores


def score_factor(rows, metric_keys, higher_better_map):
    metrics = {key: [] for key in metric_keys}
    for row in rows:
        for key in metric_keys:
            metrics[key].append(row.get(key))
    scored = {key: percentile_scores(metrics[key], higher_better_map.get(key, True)) for key in metric_keys}
    factor_scores = []
    for idx in range(len(rows)):
        per_metric = []
        for key in metric_keys:
            value = scored[key][idx]
            if value is not None:
                per_metric.append(value)
        if per_metric:
            factor_scores.append(sum(per_metric) / len(per_metric))
        else:
            factor_scores.append(None)
    return factor_scores, scored


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


def build_schedule_spec(args=None):
    start_value = getattr(args, "schedule_start", None) if args else None
    end_value = getattr(args, "schedule_end", None) if args else None
    interval_value = getattr(args, "schedule_interval", None) if args else None
    timezone = getattr(args, "schedule_timezone", None) if args else None
    days = parse_schedule_days(getattr(args, "schedule_days", None) if args else None)
    start_text, start_minutes = parse_hhmm(start_value, "08:30")
    end_text, end_minutes = parse_hhmm(end_value, "17:30")
    interval = parse_interval_minutes(interval_value, 30)
    timezone = timezone or "Asia/Hong_Kong"
    times = build_schedule_times(start_minutes, end_minutes, interval)
    command = getattr(args, "schedule_command", None) if args else None
    if not command:
        command = "python3 {baseDir}/screener_skill.py auto"
    return {
        "timezone": timezone,
        "window": {"start": start_text, "end": end_text},
        "interval_minutes": interval,
        "days": days,
        "times": times,
        "command": command,
        "trade_day_mode": "weekday",
        "calendar_file": getattr(args, "calendar_file", None) if args else None,
    }


def resolve_schedule_path():
    target = get_report_root() / "schedule.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    return target


def output_schedule(args):
    spec = build_schedule_spec(args)
    path = resolve_schedule_path()
    path.write_text(json.dumps(spec, ensure_ascii=False), encoding="utf-8")
    json_out({"ok": True, "data": {"schedule": spec, "path": str(path)}})


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
    if value is None:
        return None
    text = str(value).strip().upper()
    if text.startswith("HK."):
        return normalize_hk_symbol(text)
    if text.endswith(".HK"):
        text = text.split(".", 1)[0]
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return None
    return normalize_hk_symbol(digits)


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
        # 尝试从 Futu API 获取
        try:
            from futu import OpenQuoteContext
            quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
            ret, plate_data = quote_ctx.get_stock_company(code=norm_symbol)
            if ret == 0 and plate_data is not None and len(plate_data) > 0:
                row = plate_data.iloc[0]
                if not auto_name:
                    auto_name = row.get('name') or row.get('stock_name') or norm_symbol
                # 尝试获取行业板块
                if not auto_sector:
                    # 从 plate_list 获取行业信息
                    ret2, plate_list = quote_ctx.get_plate_list(code=norm_symbol)
                    if ret2 == 0 and plate_list is not None:
                        plates = plate_list['plate_name'].tolist() if hasattr(plate_list, 'tolist') else []
                        if plates:
                            auto_sector = plates[0]  # 取第一个板块
            quote_ctx.close()
        except Exception:
            pass  # 自动获取失败时使用默认值
    
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
        code = stock.get("code")
        if code:
            symbols.append(code)
    return symbols


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
    for symbol in symbols:
        norm = normalize_hk_symbol(symbol)
        if norm in seen:
            continue
        seen.add(norm)
        normalized.append(norm)
    return normalized


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
    auto_ctx = data.get("auto") or {}
    screened = data.get("screened") or []
    dropped = data.get("dropped") or []
    allocation = data.get("portfolio", {}).get("allocation") or []
    trades = data.get("trades") or {}
    decision = data.get("decision") or {}
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = []
    lines.append("# 港股机会筛选与交易执行报告")
    lines.append("")
    lines.append("## 报告摘要")
    lines.append(f"- 报告时间：{now}")
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


def deliver_report_to_owner(report_path, content):
    endpoint = os.environ.get("OPENCLAW_OWNER_WEBHOOK") or os.environ.get("OPENCLAW_OWNER_ENDPOINT")
    if not endpoint:
        return {"delivered": False, "mode": "stdout"}
    payload = json.dumps({"report_path": report_path, "content": content}, ensure_ascii=False).encode("utf-8")
    request = Request(endpoint, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urlopen(request, timeout=10) as response:
            status = getattr(response, "status", 200)
            return {"delivered": 200 <= status < 300, "mode": "webhook", "status": status}
    except URLError as exc:
        return {"delivered": False, "mode": "webhook", "error": str(exc)}


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
        "数据源": "clawtrade-futu-paper-trade（港股）",
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
    default_weights = {
        "quality": 0.3,
        "growth": 0.25,
        "valuation": 0.2,
        "momentum": 0.15,
        "risk": 0.1,
    }
    default_thresholds = {
        "overall": 60.0,
        "quality": 55.0,
        "growth": 50.0,
        "valuation": 40.0,
        "momentum": 50.0,
        "risk": 40.0,
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
    for symbol in args.symbols:
        norm_symbol = normalize_hk_symbol(symbol)
        kline_rows = fetch_kline(norm_symbol, start, end, args.ktype, args.autype, args.max_count)
        prices = extract_prices(kline_rows)
        indicator_rows = fetch_financial_indicators(norm_symbol, args.period, args.start, args.end)
        latest = extract_latest_row(indicator_rows)
        roe = pick_metric(latest or {}, ["roe", "ROE", "净资产收益率(%)", "净资产收益率", "ROE(%)"])
        gross_margin = pick_metric(latest or {}, ["gross_margin", "销售毛利率(%)", "毛利率", "毛利率(%)"])
        net_margin = pick_metric(latest or {}, ["net_margin", "销售净利率(%)", "净利率", "净利率(%)"])
        debt_ratio = pick_metric(latest or {}, ["debt_ratio", "资产负债率(%)", "资产负债率"])
        revenue_growth = pick_metric(latest or {}, ["revenue_growth", "营业收入同比增长率(%)", "营业收入增长率"])
        profit_growth = pick_metric(latest or {}, ["profit_growth", "净利润同比增长率(%)", "归母净利润同比增长率(%)"])
        pe_ttm = pick_metric(latest or {}, ["pe_ttm", "PE_TTM", "市盈率(PE)", "市盈率", "PE"])
        pb = pick_metric(latest or {}, ["pb", "PB", "市净率(PB)", "市净率"])
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
        rows.append(
            {
                "symbol": norm_symbol,
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
            }
        )
        last_close = prices[-1] if prices else None
        collect_data[norm_symbol] = {
            "kline_summary": {
                "start": start,
                "end": end,
                "count": len(kline_rows),
                "last_close": last_close,
            },
            "financial_snapshot": latest or {},
            "indicator_count": len(indicator_rows),
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
    quality_scores, quality_metric_scores = score_factor(rows, quality_metrics, higher_better)
    growth_scores, growth_metric_scores = score_factor(rows, growth_metrics, higher_better)
    valuation_scores, valuation_metric_scores = score_factor(rows, valuation_metrics, higher_better)
    momentum_scores, momentum_metric_scores = score_factor(rows, momentum_metrics, higher_better)
    risk_scores, risk_metric_scores = score_factor(rows, risk_metrics, higher_better)
    factor_map = {
        "quality": quality_scores,
        "growth": growth_scores,
        "valuation": valuation_scores,
        "momentum": momentum_scores,
        "risk": risk_scores,
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
            "market": row["market"],
            "metrics": row,
            "scores": {**factor_scores, "overall": overall},
            "metric_scores": {
                "quality": {k: quality_metric_scores[k][idx] for k in quality_metrics},
                "growth": {k: growth_metric_scores[k][idx] for k in growth_metrics},
                "valuation": {k: valuation_metric_scores[k][idx] for k in valuation_metrics},
                "momentum": {k: momentum_metric_scores[k][idx] for k in momentum_metrics},
                "risk": {k: risk_metric_scores[k][idx] for k in risk_metrics},
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
                "data_source": "clawtrade-futu-paper-trade",
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
            },
            "summary": {
                "symbols": len(rows),
                "screened": len(screened),
                "dropped": len(dropped),
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
    if getattr(args, "_auto_context", None):
        output_payload["data"]["auto"] = args._auto_context
    trades = execute_trades(allocation, collect_data, args)
    output_payload["data"]["trades"] = trades
    report_dir = resolve_report_dir(getattr(args, "report_dir", None))
    report_content = build_report_markdown(output_payload)
    report_name = f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    report_path = str((report_dir / report_name).resolve())
    Path(report_path).write_text(report_content, encoding="utf-8")
    delivery = deliver_report_to_owner(report_path, report_content)
    output_payload["data"]["report"] = {
        "path": report_path,
        "delivered": delivery.get("delivered"),
        "mode": delivery.get("mode"),
        "status": delivery.get("status"),
        "error": delivery.get("error"),
    }
    if not delivery.get("delivered"):
        output_payload["data"]["owner_message"] = report_content
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
                    "events": [],
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


def manage_universe(args):
    """标的池管理命令"""
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
            "data": {"sectors": sectors, "total": sum(len(v) for v in sectors.values())}
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
                "path": str(resolve_universe_json_path())
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


def build_parser():
    parser = argparse.ArgumentParser()
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
    schedule.set_defaults(func=output_schedule)

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
    screen.add_argument("--focus-sectors")
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
    return parser


def main():
    ensure_venv()
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        json_out({"ok": False, "error": str(exc)}, 1)
