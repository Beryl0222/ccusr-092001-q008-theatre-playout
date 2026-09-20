# 剧场异地同步播控

项目统一剧院中心与异地放映点之间的场次、连接和密钥称谓。`domain.json` 描述基础状态及控制事件序号约定。

运行 `python3 service.py --check` 检查配置，执行 `python3 -m unittest -v` 验证身份；启动服务后可通过 `/health` 巡检。
