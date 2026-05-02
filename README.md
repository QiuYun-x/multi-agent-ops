# 多Agent协同运营自动化系统

基于 **Python asyncio** 的去中心化多智能体协同系统，模拟电商订单处理全流程：  
`订单接收 → 库存检查 → 发货 → 用户通知`，并内置系统监控 Agent。

## 🎯 解决的核心痛点

- **单体脚本串行阻塞** – 一个环节卡住，整个流程瘫痪  
- **多系统人工割裂** – 订单、库存、发货、通知分散在不同系统，运营来回切换，易出错  
- **响应不实时、状态不可见** – 超卖、漏通知、进度无法追踪  

本项目用 **多个独立异步 Agent** 通过 **消息总线** 协作，实现 **弹性、可扩展、松耦合** 的运营自动化闭环。

## 🧱 系统架构

| Agent | 职责 |
|-------|------|
| `OrderAgent` | 接收订单，编排流程，根据库存结果路由决策 |
| `InventoryAgent` | 维护模拟库存，判断库存是否充足并返回结果 |
| `DeliveryAgent` | 模拟发货，生成物流单号，推送发货事件 |
| `NotificationAgent` | 模拟通知用户（邮件/短信） |
| `MonitorAgent` | 收集系统事件日志，可扩展告警 |
| `MessageBus` | 消息总线，基于 `asyncio.Queue` 实现 Agent 间异步通信 |

## ⚡ 核心协作流（长链流程）

```
外部事件 → OrderAgent → InventoryAgent (查库存)
         ↳ 库存充足 → DeliveryAgent (发货) → NotificationAgent + MonitorAgent
         ↳ 库存不足 → NotificationAgent (缺货通知)
```

- 一次订单处理最多跨越 **5 个 Agent、6 次消息交换**
- 每个 Agent **独立异步运行、无共享状态**，天然支持并发处理
- 流程分支完全由消息控制，并非写死的 if/else，易于扩展

## 🚀 快速开始

**环境要求**：Python 3.8+，无需任何第三方库。

```bash
# 1. 克隆仓库
git clone https://github.com/QiuYun-x/multi-agent-ops.git
cd multi-agent-ops

# 2. 运行
python 多Agent协同系统.py
```

## 📂 文件说明

```
.
├── 多Agent协同系统.py   # 主程序（含所有 Agent + 总线 + 示例）
├── README.md            # 本文件
└── .gitignore
```

## 🧪 运行示例

```
[OrderAgent] 收到新订单请求, 生成订单ID: 1000, 商品: 手机, 数量: 2
[InventoryAgent] 检查库存: 手机 需要 2, 库存 5 -> 充足
[DeliveryAgent] 订单 1000 已发货，物流单号: SF1234567
[NotificationAgent] >>> 发送用户通知: 订单 1000 - 已发货
[MonitorAgent] 系统事件记录: {'event': 'order_shipped', ...}
```

## 🛠️ 自定义与扩展

- 修改 `InventoryAgent` 中的 `self.stock` 调整初始库存  
- 如需真实扣减库存，打开 `handle_check_inventory` 方法中的 `# self.stock[item] -= quantity` 注释  
- 在 `main()` 中修改 `test_orders` 模拟不同订单场景  
- 新增 Agent：创建 `Agent` 子类，注册消息处理器，挂载到 `MessageBus` 即可，**无需改动已有 Agent**
