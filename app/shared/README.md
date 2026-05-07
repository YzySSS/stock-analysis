# shared/

该目录用于存放跨模块共享的基础能力，例如：

- 配置读取
- 策略注册表加载
- 统一日志
- 公共数据结构
- 工具函数

当前已加入：

- `strategy_loader.py`：从注册表动态加载默认策略或指定策略
- `settings.py`：统一读取 MySQL / SQLite 配置
- `db.py`：统一提供 MySQL / SQLite 连接入口
