from typing import Any, Dict, List

from fastapi import APIRouter, Body

from ..core.data_source_manager_impl import data_source_manager


router = APIRouter(prefix="/stocks", tags=["stocks"])


@router.post("/prices", summary="获取股票实时价格")
async def get_stock_prices(
    codes: List[str] = Body(..., embed=True)
) -> Dict[str, Any]:
    """获取股票实时价格"""
    prices = {}
    
    for code in codes:
        try:
            # 转换股票代码格式
            base = code
            if "." in str(code):
                try:
                    base = data_source_manager._convert_from_ts_code(code)
                except Exception:
                    base = code
            
            # 获取实时价格
            q = data_source_manager.get_realtime_quotes(base)
            
            if isinstance(q, dict):
                prices[code] = {
                    "latestPrice": q.get("price"),
                    "openPrice": q.get("open"),
                    "closePrice": q.get("pre_close"),
                    "highPrice": q.get("high"),
                    "lowPrice": q.get("low"),
                    "rating": q.get("rating", "未评级")
                }
            else:
                prices[code] = None
        except Exception:
            prices[code] = None
    
    return {"prices": prices}
