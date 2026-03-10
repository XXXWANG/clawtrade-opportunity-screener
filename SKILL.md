---
name: clawtrade-opportunity-screener
description: 港股多因子机会筛选、自动筛选、组合建议与 TCC 兼容调度执行
metadata: {"openclaw":{"requires":{"bins":["python3"]},"os":["darwin","linux","win32"]}}
---

# 股票机会筛选 Skill

当用户需要进行港股机会筛选与识别时，调用此技能。使用本技能时：
- 统一通过 {baseDir}/screener_skill.py 执行
- 数据源为 clawtrade-futu-paper-trade 技能（futu-api）
- 需要本地 FutuOpenD 处于运行状态
- 支持 direct CLI 和 TCC `--task_id` 两种运行模式

环境变量
- FUTU_HOST：FutuOpenD 地址，默认 127.0.0.1
- FUTU_PORT：FutuOpenD 端口，默认 11111
- FUTU_TRD_MARKET：固定 HK
- FUTU_PAPER_TRADE_DIR：clawtrade-futu-paper-trade 技能目录或脚本路径（可选）
- SCREENER_TCC_TASKS_FILE：TCC `global_tasks.json` 覆盖路径（可选）
- SCREENER_TCC_LOCK_FILE：TCC `.tasks.lock` 覆盖路径（可选）

常用命令
- 输出工作流框架：
  python3 {baseDir}/screener_skill.py workflow
- TCC 任务执行：
  python3 {baseDir}/screener_skill.py --task_id task_xxxx
- 多因子筛选（港股）：
  python3 {baseDir}/screener_skill.py screen --symbols HK.00700 HK.09988
- 指定筛选区间：
  python3 {baseDir}/screener_skill.py screen --symbols HK.00700 --start 2024-01-01 --end 2025-01-31
- 月度目标约束筛选：
  python3 {baseDir}/screener_skill.py screen --symbols HK.00700 --monthly-target 0.01 --dynamic-threshold --enforce-target
- 组合约束筛选（行业分层与容量）：
  python3 {baseDir}/screener_skill.py screen --symbols HK.00700 HK.09988 --sector-map '{"HK.00700":"互联网","HK.09988":"消费"}' --sector-caps '{"互联网":0.3,"消费":0.3}' --capacity-caps '{"HK.00700":0.12,"HK.09988":0.08}' --portfolio-target 0.01
- 自动机会筛选（标的池 + 信号触发）：
  python3 {baseDir}/screener_skill.py auto --universe-file {baseDir}/universe.txt --auto-min-turnover 20000000 --signal-return-21 0.03 --signal-return-63 0.08 --signal-top 30
- 自动落盘缓存：
  python3 {baseDir}/screener_skill.py screen --symbols HK.00700 HK.09988 --save-cache --cache-dir {baseDir}/cache --cache-stages filter,analyze,decide
- 全阶段落盘缓存：
  python3 {baseDir}/screener_skill.py screen --symbols HK.00700 HK.09988 --save-cache --cache-stages collect,filter,analyze,decide,review
- 自定义权重：
  python3 {baseDir}/screener_skill.py screen --symbols HK.00700 --weights '{"quality":0.35,"growth":0.25,"valuation":0.2,"momentum":0.1,"risk":0.1}'
- 自定义阈值：
  python3 {baseDir}/screener_skill.py screen --symbols HK.00700 --thresholds '{"overall":60,"quality":55,"growth":50,"valuation":40,"momentum":50,"risk":40}'

目标约束参数
- monthly-target：月度目标收益，默认 0.01
- dynamic-threshold：根据目标收益动态调整阈值
- enforce-target：不足目标时直接剔除
- target-tolerance：目标达成容忍度，默认 0.5
- risk-budget：月度风险预算，默认 0.02
- base-position：基础仓位，默认 0.05
- max-position：最大仓位，默认 0.15

组合约束参数
- sector-map：行业映射 JSON，symbol -> 行业
- sector-caps：行业上限 JSON，行业 -> 权重上限
- capacity-caps：容量上限 JSON，symbol -> 权重上限
- portfolio-target：组合净值月度目标，默认等于 monthly-target

自动筛选参数
- universe / universe-file：标的池输入（列表或文件）
- auto-min-turnover：最小成交额过滤
- auto-min-volume：最小成交量过滤
- auto-min-price：最小股价过滤
- auto-volatility-min：最小年化波动率过滤
- auto-volatility-max：最大年化波动率过滤
- auto-max-drawdown：最大回撤过滤
- signal-return-21：近 21 日收益信号阈值
- signal-return-63：近 63 日收益信号阈值
- signal-max-drawdown：信号阶段最大回撤阈值
- signal-top：信号分数 Top N

缓存参数
- save-cache：启用自动落盘
- cache-dir：缓存根目录，默认 {baseDir}/cache
- cache-stages：缓存阶段列表，默认 filter,analyze,decide
- collect：包含价格回溯摘要与财务快照
- review：包含筛选结果与目标偏差复盘

输出说明
- 所有输出为 JSON
- 失败时返回 error 字段，包含原因与建议
- TCC 模式下会按协议从 `global_tasks.json` 读取 payload，并在完成后回写 `completed` / `pending` / `failed`

TCC payload 约定
- payload 需包含 `command`
- 推荐格式：
  {"command":"screen","args":{"symbols":["HK.00700","HK.09988"],"monthly_target":0.01,"dynamic_threshold":true}}
- 也兼容扁平格式：
  {"command":"auto","universe":["HK.00700"],"signal_return_21":0.03}
