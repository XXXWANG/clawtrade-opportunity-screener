import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


REPORT_TYPE_PROFILES = {
    "ad_hoc_screen": {
        "headline": "港股机会筛选团队报告",
        "no_action": "本轮无建议执行交易，继续保留观察视角",
        "trade": "本轮已有候选动作，等待执行与盘中确认",
        "watch": "本轮形成观察名单，等待更清晰触发条件",
    },
    "pre_open": {
        "headline": "港股盘前总览",
        "no_action": "盘前判断偏观察，暂不预设激进行动",
        "trade": "盘前已形成候选清单，等待开盘前确认",
        "watch": "盘前建立观察池，等待开盘前二次筛选",
    },
    "pre_trade": {
        "headline": "港股开盘前策略结论",
        "no_action": "开盘前未形成执行结论，维持谨慎",
        "trade": "开盘前已形成执行候选，准备跟踪触发",
        "watch": "开盘前形成观察名单，等待价格与量能确认",
    },
    "midday": {
        "headline": "港股午间进度报告",
        "no_action": "上午未发生有效执行，下午继续观察",
        "trade": "上午已有执行或明确计划，下午继续跟踪",
        "watch": "上午主要完成验证，下午决定是否动作",
    },
    "pre_close_risk": {
        "headline": "港股收盘前风险检查",
        "no_action": "收盘前以风险检查为主，暂无新增动作",
        "trade": "收盘前存在仓位与风险调整需求",
        "watch": "收盘前继续观察隔夜风险与事件窗口",
    },
    "post_close": {
        "headline": "港股收盘后执行总结",
        "no_action": "收盘后确认今日未执行新增动作",
        "trade": "收盘后确认今日有执行或仓位变化",
        "watch": "收盘后完成结果核对，保留观察结论",
    },
    "night_review": {
        "headline": "港股夜间复盘",
        "no_action": "夜间复盘以规则和偏差复核为主",
        "trade": "夜间复盘聚焦已执行决策的有效性",
        "watch": "夜间复盘聚焦观察名单与次日假设",
    },
    "event_alert": {
        "headline": "港股事件驱动快报",
        "no_action": "事件已记录，当前无需追加动作",
        "trade": "事件可能触发即时动作，需快速确认",
        "watch": "事件已触发重点观察，等待进一步明确信号",
    },
    "settlement": {
        "headline": "30天结算报告",
        "no_action": "当前结算窗口无新增执行动作",
        "trade": "结算窗口对执行结果进行统一结转",
        "watch": "结算窗口以状态复核与续命判断为主",
    },
}

REPORT_TYPE_SEQUENCE = {
    "pre_open": ("pre_trade", "08:30", "09:15"),
    "pre_trade": ("midday", "09:15", "11:50"),
    "midday": ("pre_close_risk", "11:50", "15:30"),
    "pre_close_risk": ("post_close", "15:30", "16:30"),
    "post_close": ("night_review", "16:30", "20:30"),
    "night_review": ("pre_open", "20:30", "08:30"),
}


def iso_utc_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_iso_datetime(value):
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def build_default_window_id(report_type, created_at):
    if report_type == "ad_hoc_screen":
        return None
    dt = parse_iso_datetime(created_at).astimezone(ZoneInfo("Asia/Hong_Kong"))
    return f"{report_type}_{dt.strftime('%Y%m%d')}"


def pick_report_profile(report_type):
    return REPORT_TYPE_PROFILES.get(report_type) or REPORT_TYPE_PROFILES["ad_hoc_screen"]


def infer_market_regime(auto_ctx, allocation, negative_information_flags):
    if auto_ctx.get("signal_passed", 0) > 0 and negative_information_flags <= 0:
        return "risk_on"
    if negative_information_flags > 0 and not allocation:
        return "risk_off"
    return "neutral"


def infer_risk_mode(allocation, negative_information_flags, auto_ctx):
    if negative_information_flags >= 2:
        return "lockdown"
    if negative_information_flags > 0 and not allocation:
        return "defense"
    if allocation or auto_ctx.get("signal_passed", 0) > 0:
        return "attack"
    return "balanced"


def infer_window_status(report_type, target_projection):
    if report_type == "settlement":
        return "settled"
    gap = (target_projection or {}).get("gap")
    if gap is None:
        return None
    return "on_track" if gap <= 0 else "behind"


def infer_next_report_time(report_type, created_at):
    sequence = REPORT_TYPE_SEQUENCE.get(report_type)
    if not sequence:
        return None
    next_type, _current_label, next_label = sequence
    dt = parse_iso_datetime(created_at).astimezone(ZoneInfo("Asia/Hong_Kong"))
    hour, minute = [int(token) for token in next_label.split(":", 1)]
    next_dt = dt.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if next_dt <= dt:
        next_dt = next_dt + timedelta(days=1)
    return next_dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def build_event_flags(items):
    flags = []
    upcoming = [item for item in items if item.get("metrics", {}).get("next_earnings_in_days") is not None]
    if any((item.get("metrics", {}).get("next_earnings_in_days") or 999) <= 7 for item in upcoming):
        flags.append("earnings_within_7d")
    if any(item.get("metrics", {}).get("information_risk_flags") for item in items):
        flags.append("information_risk_flagged")
    return flags


def detect_event_alerts(payload):
    data = payload.get("data") or {}
    screened = data.get("screened") or []
    dropped = data.get("dropped") or []
    allocation = (data.get("portfolio") or {}).get("allocation") or []
    trades = data.get("trades") or {}
    alerts = []
    for order in trades.get("orders") or []:
        symbol = order.get("symbol") or "UNKNOWN"
        status = order.get("status")
        if status == "submitted":
            alerts.append(
                {
                    "alert_type": "trade_submitted",
                    "severity": "high",
                    "symbol": symbol,
                    "summary": f"{symbol} 已提交交易指令",
                }
            )
        elif status == "failed":
            alerts.append(
                {
                    "alert_type": "execution_failure",
                    "severity": "high",
                    "symbol": symbol,
                    "summary": f"{symbol} 交易执行失败: {order.get('error') or 'unknown'}",
                }
            )
    for item in screened + dropped:
        symbol = item.get("symbol") or "UNKNOWN"
        metrics = item.get("metrics") or {}
        info = item.get("information") or {}
        earnings_days = metrics.get("next_earnings_in_days")
        if earnings_days is not None and earnings_days <= 7:
            alerts.append(
                {
                    "alert_type": "earnings_within_7d",
                    "severity": "medium",
                    "symbol": symbol,
                    "summary": f"{symbol} 财报窗口进入 {earnings_days} 天内",
                }
            )
        for risk_flag in info.get("risk_flags") or []:
            alerts.append(
                {
                    "alert_type": "information_risk",
                    "severity": "medium",
                    "symbol": symbol,
                    "summary": f"{symbol} 出现信息面风险: {risk_flag}",
                }
            )
    if allocation:
        alerts.append(
            {
                "alert_type": "position_change",
                "severity": "medium",
                "symbol": None,
                "summary": f"当前形成 {len(allocation)} 个建议仓位",
            }
        )
    return alerts


def report_date_from_iso(value):
    text = str(value or "")
    if "T" in text:
        return text.split("T", 1)[0]
    return text[:10]


def stable_short_hash(payload):
    digest = hashlib.sha1(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
    return digest[:6]


def build_report_id(report_type, created_at, identity_payload):
    timestamp = datetime.fromisoformat(created_at.replace("Z", "+00:00")).strftime("%Y%m%d_%H%M%S")
    return f"{report_type}_{timestamp}_{stable_short_hash(identity_payload)}"


def load_manifest(manifest_path):
    manifest_path = Path(manifest_path)
    if manifest_path.exists():
        with manifest_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    return {
        "version": "v1",
        "updated_at": iso_utc_now(),
        "entries": [],
    }


def save_manifest(manifest_path, manifest):
    manifest_path = Path(manifest_path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest["updated_at"] = iso_utc_now()
    with manifest_path.open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, ensure_ascii=False, indent=2)


def load_json_file(path, default_value):
    path = Path(path)
    if path.exists():
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    return default_value


def save_json_file(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def collect_symbols_from_payload(payload):
    data = payload.get("data") or {}
    symbols = []
    seen = set()
    for section in (
        data.get("screened") or [],
        data.get("dropped") or [],
        data.get("portfolio", {}).get("allocation") or [],
        data.get("auto", {}).get("signal_candidates") or [],
    ):
        for item in section:
            symbol = item.get("symbol")
            if symbol and symbol not in seen:
                seen.add(symbol)
                symbols.append(symbol)
    return symbols


def build_screener_team_report(payload):
    data = payload.get("data") or {}
    config = data.get("config") or {}
    report_context = data.get("report_context") or {}
    screened = data.get("screened") or []
    dropped = data.get("dropped") or []
    allocation = data.get("portfolio", {}).get("allocation") or []
    trades = data.get("trades") or {}
    auto_ctx = data.get("auto") or {}
    event_alerts = data.get("event_alerts") or detect_event_alerts(payload)
    summary = data.get("summary") or {}
    portfolio_projection = (data.get("portfolio") or {}).get("target_projection") or {}
    created_at = iso_utc_now()
    symbols = collect_symbols_from_payload(payload)
    decision_tag = "trade" if allocation else "watch" if screened else "no_action"
    report_type = report_context.get("report_type") or "ad_hoc_screen"
    window_id = report_context.get("window_id") or build_default_window_id(report_type, created_at)
    profile = pick_report_profile(report_type)
    negative_information_flags = int(summary.get("negative_information_flags") or 0)
    market_regime = infer_market_regime(auto_ctx, allocation, negative_information_flags)
    risk_mode = infer_risk_mode(allocation, negative_information_flags, auto_ctx)
    window_status = infer_window_status(report_type, portfolio_projection)
    next_report_time = infer_next_report_time(report_type, created_at)
    report_id = build_report_id(
        report_type,
        created_at,
        {
            "symbols": symbols,
            "decision_tag": decision_tag,
            "end": config.get("end"),
            "window_id": window_id,
        },
    )
    highlights = []
    for item in screened[:3]:
        highlights.append(f"{item.get('symbol')} score={round(item.get('scores', {}).get('overall') or 0.0, 2)}")
    if not highlights and auto_ctx.get("signal_candidates"):
        for item in auto_ctx.get("signal_candidates", [])[:3]:
            highlights.append(f"{item.get('symbol')} signal={round(item.get('score') or 0.0, 4)}")
    actions = []
    for item in allocation:
        actions.append(
            {
                "action_type": "open",
                "symbol": item.get("symbol"),
                "summary": f"建议权重 {item.get('suggested_weight')}",
                "planned_weight_pct": item.get("suggested_weight"),
                "filled_weight_pct": None,
            }
        )
    if not actions:
        actions.append(
            {
                "action_type": "no_action",
                "symbol": None,
                "summary": "当前无建议执行交易",
                "planned_weight_pct": None,
                "filled_weight_pct": None,
            }
        )
    positions_snapshot = []
    for item in allocation:
        positions_snapshot.append(
            {
                "symbol": item.get("symbol"),
                "weight_pct": item.get("suggested_weight") or 0.0,
                "pnl_pct": None,
            }
        )
    orders_snapshot = []
    for order in trades.get("orders") or []:
        orders_snapshot.append(
            {
                "order_id": f"{report_id}:{order.get('symbol') or 'unknown'}",
                "symbol": order.get("symbol"),
                "status": order.get("status") or "failed",
            }
        )
    risk_list = []
    for item in dropped:
        reasons = item.get("reasons") or []
        if reasons:
            risk_list.append(f"{item.get('symbol')}: {', '.join(reasons[:3])}")
    if not risk_list:
        risk_list.append("当前未发现新增结构性风险")
    if auto_ctx.get("signal_passed") is not None:
        risk_list.append(f"signal_passed={auto_ctx.get('signal_passed')}")
    next_actions = []
    if allocation:
        next_actions.append("继续跟踪入选标的成交与盘后信息面变化")
    else:
        next_actions.append("保持观察，等待更优候选或调整阈值/标的池")
    next_actions.insert(0, profile.get(decision_tag) or profile["no_action"])
    source_refs = [{"source_type": "data_source", "ref_id": str(item)} for item in (config.get("data_source") or [])]
    return {
        "meta": {
            "report_id": report_id,
            "parent_report_id": report_context.get("parent_report_id"),
            "draft_bundle_id": None,
            "report_type": report_type,
            "trigger_type": report_context.get("trigger_type") or ("scheduled" if auto_ctx else "manual"),
            "version": 1,
            "delivery_attempt": 0,
            "created_at": created_at,
            "as_of": created_at,
        },
        "context": {
            "window_id": window_id,
            "task_id": report_context.get("task_id"),
            "workflow_id": report_context.get("workflow_id"),
            "symbols": symbols,
            "portfolio_state": {
                "nav": None,
                "cash_ratio_pct": (data.get("portfolio", {}).get("cash_weight") or 0.0) * 100.0,
                "gross_exposure_pct": sum((item.get("suggested_weight") or 0.0) for item in allocation) * 100.0,
                "net_exposure_pct": None,
                "position_count": len(allocation),
            },
            "source_refs": source_refs,
        },
        "executive_summary": {
            "headline": profile["headline"],
            "decision_summary": f"screened={len(screened)}, dropped={len(dropped)}, decision={decision_tag}",
            "risk_summary": f"negative_information_flags={negative_information_flags}, risk_mode={risk_mode}",
            "status_tags": [decision_tag],
        },
        "market_update": {
            "market_regime": market_regime,
            "highlights": highlights,
            "event_flags": [item.get("alert_type") for item in event_alerts],
            "watchlist_changes": [item.get("symbol") for item in screened[:5] if item.get("symbol")],
        },
        "portfolio_update": {
            "actions": actions,
            "positions_snapshot": positions_snapshot,
            "orders_snapshot": orders_snapshot,
        },
        "risk_update": {
            "risk_mode": risk_mode,
            "window_status": window_status,
            "top_risks": risk_list,
            "alerts": [item.get("summary") for item in event_alerts],
        },
        "next_actions": {
            "action_items": next_actions,
            "next_report_time": next_report_time,
        },
        "delivery": {
            "delivery_status": "pending",
            "delivery_channel": None,
            "sent_at": None,
            "error": None,
        },
    }


def manifest_entry_from_report(report, json_path, markdown_path):
    return {
        "report_id": report["meta"]["report_id"],
        "parent_report_id": report["meta"].get("parent_report_id"),
        "report_type": report["meta"]["report_type"],
        "trigger_type": report["meta"].get("trigger_type"),
        "report_date": report_date_from_iso(report["meta"]["created_at"]),
        "as_of": report["meta"]["as_of"],
        "window_id": report["context"].get("window_id"),
        "symbols": report["context"].get("symbols") or [],
        "decision_tag": (report.get("executive_summary") or {}).get("decision_summary"),
        "risk_mode": (report.get("risk_update") or {}).get("risk_mode"),
        "window_status": (report.get("risk_update") or {}).get("window_status"),
        "version": report["meta"]["version"],
        "delivery_attempt": report["meta"].get("delivery_attempt"),
        "delivery_status": (report.get("delivery") or {}).get("delivery_status"),
        "json_path": str(json_path),
        "markdown_path": str(markdown_path),
        "created_at": report["meta"]["created_at"],
        "sent_at": (report.get("delivery") or {}).get("sent_at"),
        "error": (report.get("delivery") or {}).get("error"),
    }


def get_retry_queue_path(report_root):
    return Path(report_root) / "index" / "report_retry_queue.json"


def load_retry_queue(report_root):
    return load_json_file(
        get_retry_queue_path(report_root),
        {
            "version": "v1",
            "updated_at": iso_utc_now(),
            "entries": [],
        },
    )


def save_retry_queue(report_root, queue):
    queue["updated_at"] = iso_utc_now()
    save_json_file(get_retry_queue_path(report_root), queue)


def build_retry_queue_entry(report, json_path, markdown_path, delivery):
    priority = "high" if report["meta"]["report_type"] == "event_alert" else "normal"
    base_delay_seconds = 60 if priority == "high" else 300
    retry_count = int(delivery.get("retry_count") or 0)
    next_retry_at = (
        datetime.now(timezone.utc) + timedelta(seconds=base_delay_seconds * max(1, 2**retry_count))
    ).isoformat().replace("+00:00", "Z")
    return {
        "report_id": report["meta"]["report_id"],
        "parent_report_id": report["meta"].get("parent_report_id"),
        "report_type": report["meta"]["report_type"],
        "priority": priority,
        "delivery_attempt": int(delivery.get("attempts") or 0),
        "retry_count": retry_count,
        "next_retry_at": next_retry_at,
        "last_error": delivery.get("error"),
        "delivery_channel": delivery.get("mode"),
        "json_path": str(json_path),
        "markdown_path": str(markdown_path),
        "created_at": report["meta"]["created_at"],
    }


def upsert_retry_queue_entry(report_root, report, json_path, markdown_path, delivery):
    queue = load_retry_queue(report_root)
    entry = build_retry_queue_entry(report, json_path, markdown_path, delivery)
    entries = queue.setdefault("entries", [])
    for index, item in enumerate(entries):
        if item.get("report_id") == entry["report_id"]:
            entries[index] = entry
            save_retry_queue(report_root, queue)
            return entry
    entries.append(entry)
    save_retry_queue(report_root, queue)
    return entry


def remove_retry_queue_entry(report_root, report_id):
    queue = load_retry_queue(report_root)
    original = queue.get("entries") or []
    queue["entries"] = [item for item in original if item.get("report_id") != report_id]
    if len(queue["entries"]) != len(original):
        save_retry_queue(report_root, queue)


def list_retry_queue(report_root, due_only=False, limit=20):
    queue = load_retry_queue(report_root)
    now = datetime.now(timezone.utc)
    entries = sorted(queue.get("entries") or [], key=lambda item: item.get("next_retry_at") or "")
    matched = []
    for item in entries:
        if due_only:
            next_retry_at = item.get("next_retry_at")
            if next_retry_at:
                next_dt = datetime.fromisoformat(next_retry_at.replace("Z", "+00:00"))
                if next_dt > now:
                    continue
        matched.append(item)
        if limit and len(matched) >= limit:
            break
    return {
        "queue_path": str(get_retry_queue_path(report_root)),
        "total": len(matched),
        "entries": matched,
    }


def upsert_manifest_entry(manifest, entry):
    entries = manifest.setdefault("entries", [])
    for index, item in enumerate(entries):
        if item.get("report_id") == entry.get("report_id"):
            entries[index] = entry
            return
    entries.append(entry)


def archive_team_report(report_root, report, markdown_content):
    report_root = Path(report_root)
    date_tag = report_date_from_iso(report["meta"]["created_at"]).replace("-", "")
    report_type = report["meta"]["report_type"]
    archive_dir = report_root / "archive" / date_tag / report_type
    archive_dir.mkdir(parents=True, exist_ok=True)
    json_path = archive_dir / f"{report['meta']['report_id']}.json"
    markdown_path = archive_dir / f"{report['meta']['report_id']}.md"
    with json_path.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)
    markdown_path.write_text(markdown_content, encoding="utf-8")
    manifest_path = report_root / "index" / "reports_manifest.json"
    manifest = load_manifest(manifest_path)
    upsert_manifest_entry(manifest, manifest_entry_from_report(report, json_path, markdown_path))
    save_manifest(manifest_path, manifest)
    return {
        "json_path": str(json_path),
        "markdown_path": str(markdown_path),
        "manifest_path": str(manifest_path),
    }


def update_archived_report_delivery(report_root, report, markdown_content, delivery):
    report_root = Path(report_root)
    report["meta"]["delivery_attempt"] = int(delivery.get("attempts") or 0)
    mode = delivery.get("mode")
    error = delivery.get("error")
    if delivery.get("delivered"):
        delivery_status = "sent"
        sent_at = iso_utc_now()
    elif mode == "stdout" and not error:
        delivery_status = "skipped"
        sent_at = None
    else:
        delivery_status = "failed"
        sent_at = None
    report["delivery"]["delivery_status"] = delivery_status
    report["delivery"]["delivery_channel"] = mode
    report["delivery"]["sent_at"] = sent_at
    report["delivery"]["error"] = error
    archived = archive_team_report(report_root, report, markdown_content)
    return {
        **archived,
        "delivery_status": report["delivery"]["delivery_status"],
    }


def sort_manifest_entries(entries):
    return sorted(
        entries or [],
        key=lambda item: (item.get("created_at") or "", item.get("report_id") or ""),
        reverse=True,
    )


def query_report_history(
    report_root,
    report_id=None,
    report_date=None,
    symbol=None,
    report_type=None,
    window_id=None,
    delivery_status=None,
    limit=20,
):
    report_root = Path(report_root)
    manifest_path = report_root / "index" / "reports_manifest.json"
    manifest = load_manifest(manifest_path)
    entries = sort_manifest_entries(manifest.get("entries") or [])
    matched = []
    for entry in entries:
        if report_id and entry.get("report_id") != report_id:
            continue
        if report_date and entry.get("report_date") != report_date:
            continue
        if symbol and symbol not in (entry.get("symbols") or []):
            continue
        if report_type and entry.get("report_type") != report_type:
            continue
        if window_id and entry.get("window_id") != window_id:
            continue
        if delivery_status and entry.get("delivery_status") != delivery_status:
            continue
        matched.append(entry)
        if report_id:
            break
        if limit and len(matched) >= limit:
            break
    return {
        "manifest_path": str(manifest_path),
        "total": len(matched),
        "entries": matched,
    }


def read_report_content(entry, content_format):
    if content_format == "json":
        path = entry.get("json_path")
        if not path:
            return None
        with Path(path).open("r", encoding="utf-8") as handle:
            return json.load(handle)
    path = entry.get("markdown_path")
    if not path:
        return None
    return Path(path).read_text(encoding="utf-8")
