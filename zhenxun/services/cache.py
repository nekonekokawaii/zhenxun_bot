from collections.abc import Callable
from datetime import datetime
from functools import wraps
import inspect
from typing import Any, ClassVar, Generic, TypeVar

from aiocache import Cache as AioCache
from aiocache.base import BaseCache
from aiocache.serializers import JsonSerializer
from nonebot.compat import model_dump
from nonebot.utils import is_coroutine_callable
from pydantic import BaseModel

from zhenxun.services.log import logger

__all__ = ["Cache", "CacheData", "CacheRoot"]

T = TypeVar("T")


class DbCacheException(Exception):
    """缓存相关异常"""

    def __init__(self, info: str):
        self.info = info

    def __str__(self) -> str:
        return self.info


def validate_name(func: Callable):
    """验证缓存名称是否存在的装饰器"""

    def wrapper(self, name: str, *args, **kwargs):
        _name = name.upper()
        if _name not in CacheManager._data:
            raise DbCacheException(f"缓存数据 {name} 不存在")
        return func(self, _name, *args, **kwargs)

    return wrapper


class CacheGetter(BaseModel, Generic[T]):
    """缓存数据获取器"""

    get_func: Callable[..., Any] | None = None

    async def get(self, cache_data: "CacheData", *args, **kwargs) -> T:
        """获取处理后的缓存数据"""
        if not self.get_func:
            return await cache_data.get_data()

        if is_coroutine_callable(self.get_func):
            return await self.get_func(cache_data, *args, **kwargs)
        return self.get_func(cache_data, *args, **kwargs)


class CacheData(BaseModel):
    """缓存数据模型"""

    name: str
    func: Callable[..., Any]
    getter: CacheGetter | None = None
    updater: Callable[..., Any] | None = None
    with_refresh: Callable[..., Any] | None = None
    expire: int = 600  # 默认10分钟过期
    reload_count: int = 0
    incremental_update: bool = True

    class Config:
        arbitrary_types_allowed = True

    @property
    def _cache(self) -> BaseCache:
        """获取aiocache实例"""
        return AioCache(
            AioCache.MEMORY,
            serializer=JsonSerializer(),
            namespace="zhenxun_cache",
            timeout=30,  # 操作超时时间
            ttl=self.expire,  # 设置默认过期时间
        )

    async def get_data(self) -> Any:
        """从缓存获取数据"""
        try:
            data = await self._cache.get(self.name)
            logger.debug(f"获取缓存 {self.name} 数据: {data}")

            # 如果数据为空，尝试重新加载
            if data is None:
                logger.debug(f"缓存 {self.name} 数据为空，尝试重新加载")
                try:
                    if self.has_args():
                        new_data = (
                            await self.func()
                            if is_coroutine_callable(self.func)
                            else self.func()
                        )
                    else:
                        new_data = (
                            await self.func()
                            if is_coroutine_callable(self.func)
                            else self.func()
                        )

                    await self.set_data(new_data)
                    self.reload_count += 1
                    logger.info(f"重新加载缓存 {self.name} 完成")
                    return new_data
                except Exception as e:
                    logger.error(f"重新加载缓存 {self.name} 失败: {e}")
                    return None

            return data
        except Exception as e:
            logger.error(f"获取缓存 {self.name} 失败: {e}")
            return None

    def _serialize_value(self, value: Any) -> Any:
        """序列化值，将数据转换为JSON可序列化的格式

        Args:
            value: 需要序列化的值

        Returns:
            JSON可序列化的值
        """
        if value is None:
            return None

        # 处理datetime
        if isinstance(value, datetime):
            return value.isoformat()

        # 处理Tortoise-ORM Model
        if hasattr(value, "_meta") and hasattr(value, "__dict__"):
            result = {}
            for field in value._meta.fields:
                try:
                    field_value = getattr(value, field)
                    # 跳过反向关系字段
                    if isinstance(field_value, list | set) and hasattr(
                        field_value, "_related_name"
                    ):
                        continue
                    result[field] = self._serialize_value(field_value)
                except AttributeError:
                    continue
            return result
        # 处理Pydantic模型
        elif isinstance(value, BaseModel):
            return model_dump(value)
        elif isinstance(value, dict):
            # 处理字典
            return {str(k): self._serialize_value(v) for k, v in value.items()}
        elif isinstance(value, list | tuple | set):
            # 处理列表、元组、集合
            return [self._serialize_value(item) for item in value]
        elif isinstance(value, int | float | str | bool):
            # 基本类型直接返回
            return value
        elif hasattr(value, "__dict__"):
            # 处理普通类对象
            return self._serialize_value(value.__dict__)
        else:
            # 其他类型转换为字符串
            return str(value)

    async def set_data(self, value: Any):
        """设置缓存数据"""
        try:
            # 1. 序列化数据
            serialized_value = self._serialize_value(value)
            logger.debug(f"设置缓存 {self.name} 原始数据: {value}")
            logger.debug(f"设置缓存 {self.name} 序列化后数据: {serialized_value}")

            # 2. 删除旧数据
            await self._cache.delete(self.name)
            logger.debug(f"删除缓存 {self.name} 旧数据")

            # 3. 设置新数据
            await self._cache.set(self.name, serialized_value, ttl=self.expire)
            logger.debug(f"设置缓存 {self.name} 新数据完成")

            # 4. 立即验证
            cached_data = await self._cache.get(self.name)
            if cached_data is None:
                logger.error(f"缓存 {self.name} 设置失败：数据验证失败")
                # 5. 如果验证失败，尝试重新设置
                await self._cache.set(self.name, serialized_value, ttl=self.expire)
                cached_data = await self._cache.get(self.name)
                if cached_data is None:
                    logger.error(f"缓存 {self.name} 重试设置失败")
                else:
                    logger.debug(f"缓存 {self.name} 重试设置成功: {cached_data}")
            else:
                logger.debug(f"缓存 {self.name} 数据验证成功: {cached_data}")
        except Exception as e:
            logger.error(f"设置缓存 {self.name} 失败: {e}")
            raise  # 重新抛出异常，让上层处理

    async def delete_data(self):
        """删除缓存数据"""
        try:
            await self._cache.delete(self.name)
        except Exception as e:
            logger.error(f"删除缓存 {self.name}", e=e)

    async def get(self, *args, **kwargs) -> Any:
        """获取缓存"""
        if not self.reload_count and not self.incremental_update:
            await self.reload(*args, **kwargs)

        if not self.getter:
            return await self.get_data()

        return await self.getter.get(self, *args, **kwargs)

    async def update(self, key: str, value: Any = None, *args, **kwargs):
        """更新单个缓存项"""
        if not self.updater:
            logger.warning(f"缓存 {self.name} 未配置更新方法")
            return

        current_data = await self.get_data() or {}
        if is_coroutine_callable(self.updater):
            await self.updater(current_data, key, value, *args, **kwargs)
        else:
            self.updater(current_data, key, value, *args, **kwargs)

        await self.set_data(current_data)
        logger.debug(f"更新缓存 {self.name}.{key}")

    async def refresh(self, *args, **kwargs):
        """刷新缓存数据"""
        if not self.with_refresh:
            return await self.reload(*args, **kwargs)

        current_data = await self.get_data()
        if current_data:
            if is_coroutine_callable(self.with_refresh):
                await self.with_refresh(current_data, *args, **kwargs)
            else:
                self.with_refresh(current_data, *args, **kwargs)
            await self.set_data(current_data)
            logger.debug(f"刷新缓存 {self.name}")

    async def reload(self, *args, **kwargs):
        """重新加载全部数据"""
        try:
            if self.has_args():
                new_data = (
                    await self.func(*args, **kwargs)
                    if is_coroutine_callable(self.func)
                    else self.func(*args, **kwargs)
                )
            else:
                new_data = (
                    await self.func()
                    if is_coroutine_callable(self.func)
                    else self.func()
                )

            await self.set_data(new_data)
            self.reload_count += 1
            logger.info(f"重新加载缓存 {self.name} 完成")
        except Exception as e:
            logger.error(f"重新加载缓存 {self.name} 失败: {e}")
            raise

    def has_args(self) -> bool:
        """检查函数是否需要参数"""
        sig = inspect.signature(self.func)
        return any(
            param.kind
            in (
                param.POSITIONAL_OR_KEYWORD,
                param.POSITIONAL_ONLY,
                param.VAR_POSITIONAL,
            )
            for param in sig.parameters.values()
        )

    async def get_key(self, key: str) -> Any:
        """获取缓存中指定键的数据

        Args:
            key: 要获取的键名

        Returns:
            键对应的值，如果不存在返回None
        """
        try:
            data = await self.get_data()
            return data.get(key) if isinstance(data, dict) else None
        except Exception as e:
            logger.error(f"获取缓存 {self.name}.{key} 失败: {e}")
            return None

    async def get_keys(self, keys: list[str]) -> dict[str, Any]:
        """获取缓存中多个键的数据

        Args:
            keys: 要获取的键名列表

        Returns:
            包含所有请求键值的字典，不存在的键值为None
        """
        try:
            data = await self.get_data()
            if isinstance(data, dict):
                return {key: data.get(key) for key in keys}
            return dict.fromkeys(keys)
        except Exception as e:
            logger.error(f"获取缓存 {self.name} 的多个键失败: {e}")
            return dict.fromkeys(keys)


class CacheManager:
    """全局缓存管理器"""

    _data: ClassVar[dict[str, CacheData]] = {}

    def new(self, name: str, incremental_update: bool = True, expire: int = 600):
        """注册新缓存"""

        def wrapper(func: Callable):
            _name = name.upper()
            if _name in self._data:
                raise DbCacheException(f"缓存 {name} 已存在")

            self._data[_name] = CacheData(
                name=_name,
                func=func,
                expire=expire,
                incremental_update=incremental_update,
            )
            return func

        return wrapper

    def listener(self, name: str):
        """创建缓存监听器"""

        def decorator(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                try:
                    return (
                        await func(*args, **kwargs)
                        if is_coroutine_callable(func)
                        else func(*args, **kwargs)
                    )
                finally:
                    cache = self._data.get(name.upper())
                    if cache and cache.with_refresh:
                        await cache.refresh()
                        logger.debug(f"监听器触发缓存 {name} 刷新")

            return wrapper

        return decorator

    @validate_name
    def updater(self, name: str):
        """设置缓存更新方法"""

        def wrapper(func: Callable):
            self._data[name].updater = func
            return func

        return wrapper

    @validate_name
    def getter(self, name: str, result_model: type):
        """设置缓存获取方法"""

        def wrapper(func: Callable):
            self._data[name].getter = CacheGetter[result_model](get_func=func)
            return func

        return wrapper

    @validate_name
    def with_refresh(self, name: str):
        """设置缓存刷新方法"""

        def wrapper(func: Callable):
            self._data[name].with_refresh = func
            return func

        return wrapper

    async def get_cache_data(self, name: str) -> Any | None:
        """获取缓存数据"""
        cache = await self.get_cache(name.upper())
        return await cache.get_data() if cache else None

    async def get_cache(self, name: str) -> CacheData | None:
        """获取缓存对象"""
        return self._data.get(name.upper())

    async def get(self, name: str, *args, **kwargs) -> Any:
        """获取缓存内容"""
        cache = await self.get_cache(name.upper())
        return await cache.get(*args, **kwargs) if cache else None

    async def update(self, name: str, key: str, value: Any = None, *args, **kwargs):
        """更新缓存项"""
        cache = await self.get_cache(name.upper())
        if cache:
            await cache.update(key, value, *args, **kwargs)

    async def reload(self, name: str, *args, **kwargs):
        """重新加载缓存"""
        cache = await self.get_cache(name.upper())
        if cache:
            await cache.reload(*args, **kwargs)


# 全局缓存管理器实例
CacheRoot = CacheManager()


class Cache(Generic[T]):
    """类型化缓存访问接口"""

    def __init__(self, module: str):
        self.module = module.upper()

    async def get(self, *args, **kwargs) -> T | None:
        """获取缓存"""
        return await CacheRoot.get(self.module, *args, **kwargs)

    async def update(self, key: str, value: Any = None, *args, **kwargs):
        """更新缓存项"""
        await CacheRoot.update(self.module, key, value, *args, **kwargs)

    async def reload(self, *args, **kwargs):
        """重新加载缓存"""
        await CacheRoot.reload(self.module, *args, **kwargs)
