#!/usr/bin/env python3
"""
恢复持仓数据
从之前的记录恢复持仓信息
"""

import json
import os

# 根据之前的记录，持仓为:
positions = [
    {
        "code": "159887",
        "name": "银行ETF",
        "buy_price": 1.276,
        "shares": 30900,
        "stop_loss": 1.187,
        "target_price": 1.467,
        "buy_date": "2026-03-20"
    },
    {
        "code": "159611",
        "name": "电力ETF",
        "buy_price": 1.183,
        "shares": 19000,
        "stop_loss": 1.100,
        "target_price": 1.360,
        "buy_date": "2026-03-20"
    },
    {
        "code": "002352",
        "name": "顺丰控股",
        "buy_price": 37.633,
        "shares": 1000,
        "stop_loss": 34.999,
        "target_price": 43.278,
        "buy_date": "2026-03-20"
    },
    {
        "code": "159142",
        "name": "双创AI",
        "buy_price": 1.158,
        "shares": 22400,
        "stop_loss": 0.950,
        "target_price": 1.332,
        "buy_date": "2026-03-20"
    }
]

# 保存持仓数据
data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

with open(os.path.join(data_dir, "positions.json"), "w", encoding="utf-8") as f:
    json.dump(positions, f, ensure_ascii=False, indent=2)

print("✅ 持仓数据已恢复")
print(f"共 {len(positions)} 只持仓:")
for p in positions:
    print(f"  - {p['name']}({p['code']}): {p['shares']}股 @ ¥{p['buy_price']}")
