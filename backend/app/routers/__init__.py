"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import replay, resource

# replay 提供 /api/replay 固定路径资源路由；resource 提供 /api/{module} 动态路由，
# 固定路径优先注册避免被动态模块路径抢先匹配。
ROUTERS = [replay, resource]
