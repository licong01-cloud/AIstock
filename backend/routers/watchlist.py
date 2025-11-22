from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Query

from ..services import watchlist_service


router = APIRouter(prefix="/watchlist", tags=["watchlist"])


@router.get("/categories", summary="自选分类列表")
async def list_categories() -> List[Dict[str, Any]]:
    return watchlist_service.list_categories()


@router.post("/categories", summary="创建自选分类")
async def create_category(
    name: str = Body(..., embed=True),
    description: Optional[str] = Body(None, embed=True),
) -> Dict[str, Any]:
    cid = watchlist_service.create_category(name, description)
    return {"id": cid}


@router.patch("/categories/{category_id}", summary="重命名自选分类")
async def rename_category(
    category_id: int,
    name: str = Body(..., embed=True),
    description: Optional[str] = Body(None, embed=True),
) -> Dict[str, Any]:
    ok = watchlist_service.rename_category(category_id, name, description)
    return {"success": ok}


@router.delete("/categories/{category_id}", summary="删除自选分类")
async def delete_category(category_id: int) -> Dict[str, Any]:
    ok = watchlist_service.delete_category(category_id)
    return {"success": ok}


@router.get("/items", summary="自选股票列表（含实时行情）")
async def list_items(
    category_id: Optional[int] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    sort_by: str = Query("updated_at"),
    sort_dir: str = Query("desc"),
) -> Dict[str, Any]:
    return watchlist_service.list_items_with_quotes(
        category_id=category_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )


@router.post("/items/bulk-add", summary="批量加入自选")
async def bulk_add_items(
    codes: List[str] = Body(..., embed=True),
    category_id: int = Body(..., embed=True),
    on_conflict: str = Body("ignore", embed=True),
) -> Dict[str, int]:
    return watchlist_service.add_items_bulk(codes, category_id, on_conflict=on_conflict)


@router.post("/items/bulk-delete", summary="批量删除自选")
async def bulk_delete_items(
    ids: List[int] = Body(..., embed=True),
) -> Dict[str, Any]:
    cnt = watchlist_service.delete_items(ids)
    return {"deleted": cnt}


@router.post("/items/add", summary="单只加入自选（支持多分类）")
async def add_item(
    code: str = Body(..., embed=True),
    category_id: int = Body(..., embed=True),
    name: Optional[str] = Body(None, embed=True),
    extra_category_ids: Optional[List[int]] = Body(None, embed=True),
) -> Dict[str, Any]:
    """单只加入自选股票池。

    - code 可以是 6 位代码或 ts_code，服务端会尽量标准化为 ts_code；
    - category_id 为主分类；
    - extra_category_ids 为附加分类（可选，多对多映射）。
    """

    item_id = watchlist_service.add_single_item(
        code=code,
        category_id=category_id,
        name=name,
        extra_category_ids=extra_category_ids or [],
    )
    return {"id": item_id}


@router.post("/items/bulk-set-category", summary="批量替换分类")
async def bulk_set_category(
    ids: List[int] = Body(..., embed=True),
    category_id: int = Body(..., embed=True),
) -> Dict[str, Any]:
    """将一批自选条目的分类替换为单一分类（原分类全部清空）。"""

    updated = watchlist_service.update_items_category(ids, category_id)
    return {"updated": updated}


@router.post("/items/bulk-add-categories", summary="批量追加分类映射")
async def bulk_add_categories_to_items(
    ids: List[int] = Body(..., embed=True),
    category_ids: List[int] = Body(..., embed=True),
) -> Dict[str, Any]:
    """为一批自选条目追加多个分类映射。"""

    mapped = watchlist_service.add_categories_to_items(ids, category_ids)
    return {"mapped": mapped}


@router.post("/items/bulk-remove-categories", summary="批量移除分类映射")
async def bulk_remove_categories_from_items(
    ids: List[int] = Body(..., embed=True),
    category_ids: List[int] = Body(..., embed=True),
) -> Dict[str, Any]:
    """从一批自选条目上移除给定分类映射。"""

    removed = watchlist_service.remove_categories_from_items(ids, category_ids)
    return {"removed": removed}
