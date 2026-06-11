"""通达信板块文件管理 REST API"""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, HTTPException

from ..services import tdx_block_service

router = APIRouter(prefix="/tdx-blocks", tags=["tdx-blocks"])


def _ensure_available():
    if not tdx_block_service.is_available():
        raise HTTPException(
            status_code=503,
            detail="通达信板块功能未启用: 请配置 TDX_BLOCK_DIR 环境变量指向通达信 blocknew 目录",
        )


@router.get("/available", summary="检查 TDX 板块功能是否可用")
def check_available() -> Dict[str, Any]:
    return {"available": tdx_block_service.is_available()}


@router.get("/list", summary="列出所有通达信板块")
def list_blocks() -> List[Dict[str, Any]]:
    _ensure_available()
    return tdx_block_service.list_blocks()


@router.post("/sync-from-category", summary="从自选分类同步到通达信板块")
def sync_from_category(
    category_name: str = Body(..., embed=True),
) -> Dict[str, Any]:
    _ensure_available()
    try:
        return tdx_block_service.sync_from_category(category_name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/{name}/stocks", summary="读取板块内股票列表")
def get_block_stocks(name: str) -> Dict[str, Any]:
    _ensure_available()
    stocks = tdx_block_service.get_block_stocks(name)
    return {"name": name, "stocks": stocks, "count": len(stocks)}


@router.post("/create", summary="创建新板块")
def create_block(
    name: str = Body(..., embed=True),
    display_name: Optional[str] = Body(None, embed=True),
    stocks: List[str] = Body(default=[], embed=True),
) -> Dict[str, Any]:
    _ensure_available()
    try:
        return tdx_block_service.create_block(
            name=name,
            display_name=display_name or name,
            stocks=stocks,
        )
    except (ValueError, FileExistsError) as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/{name}/add", summary="增量添加股票到板块")
def add_stocks(
    name: str,
    stocks: List[str] = Body(..., embed=True),
) -> Dict[str, Any]:
    _ensure_available()
    return tdx_block_service.add_stocks(name, stocks)


@router.post("/{name}/remove", summary="从板块移除股票")
def remove_stocks(
    name: str,
    stocks: List[str] = Body(..., embed=True),
) -> Dict[str, Any]:
    _ensure_available()
    return tdx_block_service.remove_stocks(name, stocks)


@router.delete("/{name}", summary="删除整个板块")
def delete_block(name: str) -> Dict[str, Any]:
    _ensure_available()
    try:
        return tdx_block_service.delete_block(name)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
