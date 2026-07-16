"""
HMM 数据源异常定义

异常层次:
DataSourceError (基类)
  ├─ DateRangeError (日期范围错误)
  ├─ HorizonError (horizon 参数错误)
  ├─ CacheError (缓存错误)
  └─ DataNotFoundError (数据不存在)
"""


class DataSourceError(Exception):
    """数据源基础异常"""
    pass


class DateRangeError(DataSourceError):
    """日期范围错误"""
    pass


class HorizonError(DataSourceError):
    """horizon_days 参数错误"""
    pass


class CacheError(DataSourceError):
    """缓存操作错误"""
    pass


class DataNotFoundError(DataSourceError):
    """数据不存在"""
    pass
