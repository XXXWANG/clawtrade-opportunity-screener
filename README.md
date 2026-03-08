# 🐉 ClawTrade Opportunity Screener

![ClawTrade Logo](./ClawTrade%20Logo.png)

> 专为 OpenClaw 打造的多因子智能选股引擎 - 帮 AI 发现被低估的港股

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Python: 3.10+](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)

**一句话概括：用多因子模型帮 OpenClaw 筛选出会上涨的港股。**

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
  "risk_level": "MEDIUM"
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

### 组合约束
| 参数 | 说明 | 示例 |
|------|------|------|
| `--sector-map` | 行业映射 | `'{"HK.00700":"互联网"}'` |
| `--sector-caps` | 行业上限 | `'{"互联网":0.3}'` |

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