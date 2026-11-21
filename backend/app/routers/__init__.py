"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。

注意顺序：resource 的 /api/{module}/{entry_id} 是通配路由，必须最后注册，
否则会抢先匹配 /api/pipeline/snapshot、/api/handover/manifest 等固定路径。
"""
from __future__ import annotations

from app.routers import resource, pipeline_router, handover_router

ROUTERS = [pipeline_router, handover_router, resource]
