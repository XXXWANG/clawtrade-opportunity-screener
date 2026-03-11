# Clawtrade Portfolio Survival Manager PRD

## 1. 文档定位

- Skill 名称: Clawtrade Portfolio Survival Manager
- 文档版本: v1.0
- 创建日期: 2026-03-10
- 文档角色: 第一个关键新 Skill 的单体设计 PRD
- 上位文档:
  - [PRD_trade_system_top_level.md](/Users/heatherli/Desktop/ClawSkillsDev/Clawtrade-opportunity-screener_Project/PRD_trade_system_top_level.md)
  - [PRD_v2_strategy_upgrade.md](/Users/heatherli/Desktop/ClawSkillsDev/Clawtrade-opportunity-screener_Project/PRD_v2_strategy_upgrade.md)

本 Skill 是整个交易系统的交易大脑，负责把“候选机会”转换为“围绕 30 天生死线的组合级决策”。

## 2. 为什么先做它

当前系统虽然已经具备:

- 机会发现能力
- 行情与交易接口能力
- 调度与自动化能力

但还缺少真正决定“现在该不该下注、该下多少、已经过线后是否该守”的中枢层。

如果没有这个 Skill:

- Opportunity Screener 会继续承担不属于它的生死线职责
- Futu Paper Trade 会被迫承接不属于它的高层交易决策
- 系统无法形成围绕 30 天窗口的统一风险控制

因此，`Portfolio Survival Manager` 是最先要补的 Skill。

## 3. Skill 使命

本 Skill 的使命不是“找股票”，也不是“直接下单”，而是:

- 以当前 30 天窗口净值为基准
- 结合当前候选机会、市场状态、持仓状态和剩余天数
- 生成面向生死线的组合层决策

一句话定义:

- 它负责决定“为了尽可能在结算日活下来，现在整个组合应该怎么做”

## 4. 目标

### 4.1 一级目标

- 最大化每个 30 天窗口在结算时点达到 `>= 1%` 的概率

### 4.2 二级目标

- 在已接近或达到 1% 时保护既有成果
- 在高把握场景下争取 `>= 2%` 的超额收益

### 4.3 非目标

- 不直接做底层行情抓取
- 不直接做 raw broker API 调用
- 不直接替代 backtest 和复盘系统
- 不直接承担单票 alpha 研究

## 5. 生死线与净值规则

### 5.1 窗口规则

- 每个窗口长度: 30 天
- 评判频率: 仅在窗口结算时点
- 收益分母: 以上一个窗口结束时的最新净值为基准，滚动复利

### 5.2 当前默认规则

- survival floor: `30 天 >= 1%`
- stretch target: `30 天 >= 2%`

### 5.3 核心原则

- 已过线时优先守住
- 未过线时优先提高过线概率，不允许无上限搏命
- 超额收益只在高置信窗口中追求

## 6. Skill 职责边界

### 6.1 它负责什么

- 记录和读取当前 30 天窗口状态
- 计算当前净值距离生死线和 stretch target 的差距
- 根据剩余天数切换风险模式
- 决定是否交易
- 决定组合总风险预算
- 决定现金仓位和持仓上限
- 决定是否进入守成模式
- 决定是否允许新的开仓
- 输出结构化组合计划给执行层

### 6.2 它不负责什么

- 不直接筛选候选股票
- 不直接调用 Futu 下单
- 不直接生成最终复盘报告
- 不直接做历史回测

## 7. 输入输出契约

### 7.1 主要输入

#### 来自 Opportunity Screener

- 候选标的列表
- 因子分数
- 候选排序
- 置信度
- 候选失效原因

#### 来自 Market Intel

- 事件风险标签
- 财报窗口标签
- 宏观/行业状态标签

#### 来自 Ledger & Review

- 当前净值
- 窗口起始净值
- 当前累计收益
- 当前持仓
- 已实现收益
- 未实现收益
- 历史窗口表现

#### 来自系统配置

- survival floor
- stretch target
- 最大仓位
- 最大单票风险预算
- 最大组合回撤容忍
- 结算日前若干天的保守模式参数

### 7.2 主要输出

输出一个结构化的 `portfolio_decision`，至少包含:

- 当前窗口状态
- 当前风险模式
- 是否允许交易
- 建议总仓位
- 建议现金比例
- 候选采纳列表
- 候选拒绝列表
- 每个持仓的目标权重
- 是否进入守成模式
- 是否禁止新增风险
- 需要交给 Execution Manager 的执行约束

## 8. 核心状态机

### 8.1 窗口状态

本 Skill 必须识别至少 3 种窗口状态:

- `ahead`
  - 已经明显领先当前窗口生死线
  - 优先守成和锁盈

- `on_track`
  - 与目标路径大体一致
  - 允许按常规模式配置风险

- `behind`
  - 明显落后目标
  - 可以适度提高胜率优先的风险预算，但禁止失控搏命

### 8.2 状态判断维度

建议至少考虑:

- 当前累计收益
- 剩余天数
- 当前持仓风险
- 候选机会质量
- 当前市场状态

### 8.3 风险模式

建议输出以下风险模式:

- `defense`
- `balanced`
- `attack`
- `lockdown`

含义:

- `defense`: 低仓位，优先防守
- `balanced`: 常规配置
- `attack`: 仅在高置信风险可控时使用
- `lockdown`: 达标后收缩风险，禁止不必要开仓

## 9. 关键决策逻辑

### 9.1 是否允许交易

本 Skill 必须先回答:

- 现在是否允许继续承担新增风险

典型拒绝交易场景:

- 市场状态为 risk_off
- 已接近结算且已过线
- 当前组合回撤过大
- 候选质量不足
- 候选事件风险过高

### 9.2 仓位预算

本 Skill 负责组合层仓位，不负责底层订单执行。

建议输出:

- 最大总仓位
- 单票上限
- 新增仓位预算
- 防守性现金比例

### 9.3 守成机制

当系统接近或达到 1% 生死线时，必须有显式守成机制。

典型规则:

- 降低总仓位
- 停止新增高波动标的
- 对已有浮盈仓位提高止盈保护
- 提前进入结算日前锁盈模式

### 9.4 落后窗口处理

当系统处于 `behind` 状态时，不能简单粗暴加仓。

原则:

- 优先提高胜率，不优先放大赔率
- 优先选择高流动性、低滑点、低波动的大票
- 优先减少噪音交易
- 禁止无上限提升杠杆或集中度

## 10. 与其他 Skill 的接口关系

### 10.1 上游

- `Clawtrade Market Intel`
- `Clawtrade Opportunity Screener`
- `Clawtrade Ledger & Review`

### 10.2 下游

- `Clawtrade Execution Manager`

### 10.3 平台协同

- `TCC` 负责调度
- `TaskRouter` 负责路由

## 11. 建议命令与运行模式

建议本 Skill 同时支持:

- direct mode
- TCC `--task_id` mode

建议初版提供以下原子命令:

- `evaluate-window`
  - 评估当前 30 天窗口状态

- `build-plan`
  - 基于当前窗口状态和候选池输出组合计划

- `settle-window`
  - 在窗口到期时结算是否过线，并切换到下一窗口

- `status`
  - 输出当前窗口的净值、生死线差距、状态机状态

## 12. 持久化与状态管理

### 12.1 必要状态

建议本 Skill 维护自己的结构化状态文件，例如:

- 当前窗口编号
- 窗口起始日期
- 窗口结束日期
- 窗口起始净值
- 当前净值
- 当前目标净值
- 当前状态
- 最近一次决策摘要

### 12.2 原则

- 业务状态以本 Skill 的结构化状态文件为准
- TCC 状态只表示调度层执行状态
- 不允许把业务真相完全放在 TCC 任务状态里

### 12.3 初版契约文件

当前 P0 已定义以下 schema，作为后续实现和跨 Skill 对接的基线:

- `schemas/portfolio_survival_manager/window_state.schema.json`
- `schemas/portfolio_survival_manager/decision_request.schema.json`
- `schemas/portfolio_survival_manager/portfolio_decision.schema.json`
- `schemas/portfolio_survival_manager/window_settlement.schema.json`

## 13. 报表与可解释性要求

本 Skill 的每次决策都应输出简洁但结构化的解释，包括:

- 当前处于哪个窗口状态
- 为什么允许或不允许继续交易
- 为什么收缩或扩大仓位
- 当前距离 1% 生死线还有多远
- 当前是否在争取 2% stretch

这部分解释必须能被后续复盘系统复用。

## 14. P0 功能清单

- 窗口状态文件定义
- `evaluate-window`
- `build-plan`
- `status`
- ahead / on_track / behind 状态机
- defense / balanced / attack / lockdown 风险模式
- 组合总仓位与现金比例输出
- 守成模式输出

## 15. P1 功能清单

- `settle-window`
- 结算日前自动切守成
- 结合事件风险调整仓位
- stretch target 驱动的超额收益策略开关

## 16. P2 功能清单

- 自适应状态切换阈值
- 多窗口生存率学习
- 与 Backtest Lab 的参数联动

## 17. 验收标准

- Skill 能正确读取当前窗口起始净值并计算当前目标净值
- Skill 能区分 ahead / on_track / behind
- Skill 能输出结构化组合计划，而不是仅输出自然语言建议
- Skill 不越权直接下单
- Skill 能在达标后进入守成模式
- Skill 能在落后时限制失控风险升级
- 后续开发者只看这份文档就能开始实现该 Skill

## 18. 对现有 Skill 的影响

该 Skill 上线后:

- `Opportunity Screener` 应迁出月度目标、生死线和组合层风险职责
- `Futu Paper Trade` 保持 broker adapter 定位
- 后续 `Execution Manager` 承接订单级决策
- 后续 `Ledger & Review` 承接净值和结算归因

这意味着它不是附加模块，而是整个交易系统正式进入分层架构的起点。
