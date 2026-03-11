# 🐉 ClawTrade Opportunity Screener

![ClawTrade Logo](./ClawTrade%20Logo.png)

> 专为 OpenClaw 打造的多因子智能选股引擎 - 帮 AI 发现被低估的港股

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Python: 3.10+](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)

**一句话概括：用多因子模型帮 OpenClaw 筛选出会上涨的港股。**

现在已接入真实信息面：
- Google News RSS：公司相关新闻流
- HKEX Title Search：公司正式公告
- HKEX Board Meeting Notifications：财报/业绩日历

---

## 🎯 核心理念

> **宁可错过机会，也不买垃圾股**

只有当股票满足以下条件时才推荐：
- ✅ 财务健康（盈利稳定、负债合理）
- ✅ 估值合理（PE、PB 在历史低位）
- ✅ 动量向上（短期走势强于大盘）
- ✅ 流动性充足（日均成交额达标）

---

## 🔬 多因子选股模型

| 因子类型 | 指标 | 作用 |
|----------|------|------|
| **价值** | PE、PB、股息率 | 找到被低估的股票 |
| **成长** | 营收增速、利润增速 | 找到增长潜力股 |
| **质量** | ROE、资产负债率、现金流 | 找到基本面扎实的公司 |
| **动量** | 近 1/3/6 月涨幅 | 找到趋势向上的股票 |
| **流动性** | 日均成交额 | 确保能顺利买卖 |

---

## ⚡ 快速开始

```bash
# 1. 克隆或复制到 OpenClaw skills 目录
git clone https://github.com/XXXWANG/clawtrade-opportunity-screener.git
# 或复制到 ~/.openclaw/skills/clawtrade-opportunity-screener

# 2. 多因子筛选
python screener_skill.py screen --symbols HK.00700 HK.09988

# 3. 设定月收益目标 1%
python screener_skill.py screen --symbols HK.00700 --monthly-target 0.01

# 4. 保守模式：强制目标达标才推荐
python screener_skill.py screen --enforce-target

# 5. 自定义权重
python screener_skill.py screen --symbols HK.00700 \
  --weights '{"quality":0.35,"growth":0.25,"valuation":0.2,"momentum":0.1,"risk":0.1}'

# 6. 接入真实信息面
python screener_skill.py screen --symbols HK.00700 HK.09988 \
  --news-lookback-days 14 --announcement-lookback-days 30 --earnings-lookahead-days 120

# 7. 查看历史报告和重试队列
python screener_skill.py reports --delivery-status failed --include-retry-queue --limit 10
python screener_skill.py report-retry --view --due-only --limit 10
python screener_skill.py report-retry --all-due --limit 10
python screener_skill.py run-scheduled --list
python screener_skill.py run-scheduled --job delivery_retry
python screener_skill.py scheduled-runs --limit 10
python screener_skill.py cron-export
python screener_skill.py cron-install
python screener_skill.py schedule-preferences --view
python screener_skill.py openclaw-automation-spec
python screener_skill.py report-settings --format markdown
python screener_skill.py report-settings-request --message "关闭午间报告，把夜间复盘改到21:30"
```

---

## 📊 输出示例

```json
{
  "ok": true,
  "symbol": "HK.00700",
  "score": 78.5,
  "factors": {
    "quality": 82,
    "growth": 75,
    "valuation": 70,
    "momentum": 85,
    "risk": 80
  },
  "recommendation": "BUY",
  "target_return": 0.025,
  "risk_level": "MEDIUM",
  "information": {
    "score": 63.5,
    "risk_flags": ["earnings_window_7d"],
    "news": {"items": []},
    "announcements": {"items": []},
    "earnings_calendar": {"items": []}
  }
}
```

---

## 🔗 黄金组合：+ ClawTrade Futu Paper Trade

> **强烈推荐搭配 [clawtrade-futu-paper-trade](https://github.com/XXXWANG/clawtrade-futu-paper-trade) 使用！**

```
选股 → 验证 → 交易 = 完整 AI 量化工作流
```

### 为什么需要两个技能？

| 技能 | 职责 | 能力 |
|------|------|------|
| **clawtrade-opportunity-screener** | 选股 | 多因子模型筛选上涨潜力股 |
| **clawtrade-futu-paper-trade** | 交易 | 验证行情、执行下单 |

### 典型工作流

```
┌─────────────────────────────────────────────────────────┐
│  09:00 盘前                                            │
│  └─ clawtrade-opportunity-screener → 筛选今日候选股票    │
│                                                         │
│  09:30-16:00 盘中监控                                   │
│  └─ clawtrade-futu-paper-trade → 验证实时行情           │
│  └─ 发现满足条件的股票 → 买入                          │
│                                                         │
│  21:00 盘后复盘                                         │
│  └─ 分析当日交易 → 调整次日策略                         │
└─────────────────────────────────────────────────────────┘
```

### 30 天生存挑战示例

用这两个技能，你可以搭建一个**全自动 AI 交易系统**：

1. 每天 09:00 用本技能筛选候选股票
2. 每 30 分钟用 clawtrade-futu-paper-trade 验证实时行情
3. 发现满足条件的股票 → 执行买入
4. 30 天后结算，目标 **+2% 收益**

---

## 📖 关键参数

### 目标约束
| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--monthly-target` | 月度目标收益率 | 1% |
| `--dynamic-threshold` | 动态调整阈值 | false |
| `--enforce-target` | 强制达标才推荐 | false |
| `--max-position` | 单票最大仓位 | 15% |
| `--disable-information` | 关闭新闻/公告/财报日历 | false |
| `--news-lookback-days` | 新闻回看窗口 | 14 |
| `--announcement-lookback-days` | 公告回看窗口 | 30 |
| `--earnings-lookahead-days` | 财报日历前瞻窗口 | 120 |
| `--disable-retry-drain` | 关闭主流程前的到期补发处理 | false |
| `--retry-due-limit` | 每次主流程预处理的到期补发上限 | 5 |

### 组合约束
| 参数 | 说明 | 示例 |
|------|------|------|
| `--sector-map` | 行业映射 | `'{"HK.00700":"互联网"}'` |
| `--sector-caps` | 行业上限 | `'{"互联网":0.3}'` |

### 报告回看与补发
| 命令 | 说明 |
|------|------|
| `reports --include-retry-queue` | 历史报告附带当前补发队列状态 |
| `report-retry --view --due-only` | 查看已到补发时间的报告 |
| `report-retry --all-due --limit 10` | 独立执行一次到期补发 |
| `scheduled-runs --limit 10` | 查看计划任务执行留痕 |
| `cron-export` | 导出外部 cron 可直接消费的调度文件 |
| `cron-install` | 安装或更新当前用户 crontab 中的 ClawTrade managed block |
| `schedule-preferences --view` | 查看或修改用户汇报时间偏好 |
| `openclaw-automation-spec` | 导出可映射到 OpenClaw automation 的固定报告任务 |
| `report-settings --format markdown` | 输出面向用户的汇报设置摘要与接触路径说明 |
| `report-settings-request --message "..."` | 解析用户自然语言汇报设置请求，并输出新的摘要与 automation 计划 |

### 日程输出

`schedule` 现在除固定时段报告外，还会输出一条独立维护任务：

- `delivery_retry`：默认 `08:35-21:05` 每 `5` 分钟执行一次
- 命令：`python3 {baseDir}/screener_skill.py run-scheduled --job delivery_retry`
- 可通过 `--retry-schedule-start`、`--retry-schedule-end`、`--retry-schedule-interval`、`--retry-limit` 调整
- 固定报告也统一走 `run-scheduled --job pre_open|pre_trade|midday|pre_close_risk|post_close|night_review`
- 每次 `run-scheduled` 执行都会留痕到 `HKAutoTradeReports/index/scheduled_run_log.json`
- `schedule.json` 额外提供扁平 `jobs` 列表，便于外部调度器直接消费
- `cron-export` 会生成 [HKAutoTradeReports/cron/clawtrade_schedule.cron](/Users/heatherli/Desktop/ClawSkillsDev/Clawtrade-opportunity-screener_Project/HKAutoTradeReports/cron/clawtrade_schedule.cron)
- 导出的 cron 会自动带 `SCREENER_RUN_CONTEXT=scheduled`，并与 TCC/手动执行共用运行锁，避免并发冲突
- `openclaw-automation-spec` 只导出固定时点报告任务；`delivery_retry` 因为是 5 分钟级别维护任务，不适合当前 OpenClaw automation RRULE 能力，继续保留 cron fallback
- 用户如不接受默认汇报时间，可用 `schedule-preferences --profile compact|minimal` 或 `--report-times '{"post_close":"17:00"}'` 调整后再重新生成 automation / cron
- 当前没有单独的 GUI 设置页；用户入口是“对话改偏好 + OpenClaw automation 卡片确认”，可用 `report-settings` 输出当前生效摘要
- 主 agent 如需直接承接用户原话，可调用 `report-settings-request --message "只保留盘前和收盘后两份"`，让 skill 自动更新偏好并生成新的 automation 计划

---

## 📦 相关项目

| 项目 | 说明 |
|------|------|
| [clawtrade-futu-paper-trade](https://github.com/XXXWANG/clawtrade-futu-paper-trade) | 📊 港股模拟交易 |
| [clawtrade](https://github.com/XXXWANG/clawtrade) | 🐉 技能箱主页 |

---

## 🤝 欢迎贡献

- 提交 Issue 报告问题
- 提交 PR 改进功能
- ⭐ Star 支持一下

---

**🧠 让 AI 成为你的港股选股专家**
