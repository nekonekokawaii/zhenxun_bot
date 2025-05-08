from typing import Any

from zhenxun.models.ban_console import BanConsole
from zhenxun.models.bot_console import BotConsole
from zhenxun.models.group_console import GroupConsole
from zhenxun.models.level_user import LevelUser
from zhenxun.models.plugin_info import PluginInfo
from zhenxun.models.plugin_limit import PluginLimit
from zhenxun.models.user_console import UserConsole
from zhenxun.services.cache import CacheData, CacheRoot
from zhenxun.services.log import logger
from zhenxun.utils.enum import CacheType


@CacheRoot.new(CacheType.PLUGINS)
async def _():
    """初始化插件缓存"""
    data_list = await PluginInfo.get_plugins()
    return {p.module: p for p in data_list}


@CacheRoot.updater(CacheType.PLUGINS)
async def _(data: dict[str, PluginInfo], key: str, value: Any):
    """更新插件缓存"""
    if value:
        data[key] = value
    elif plugin := await PluginInfo.get_plugin(module=key):
        data[key] = plugin


@CacheRoot.getter(CacheType.PLUGINS, result_model=PluginInfo)
async def _(cache_data: CacheData, module: str):
    """获取插件缓存"""
    data = await cache_data.get_data() or {}
    if module not in data:
        if plugin := await PluginInfo.get_plugin(module=module):
            data[module] = plugin
            await cache_data.set_data(data)
            logger.debug(f"插件 {module} 数据已设置到缓存")
    return data.get(module)


@CacheRoot.with_refresh(CacheType.PLUGINS)
async def _(data: dict[str, PluginInfo] | None):
    """刷新插件缓存"""
    if not data:
        return
    plugins = await PluginInfo.filter(module__in=data.keys(), load_status=True).all()
    data.update({p.module: p for p in plugins})


@CacheRoot.new(CacheType.GROUPS)
async def _():
    """初始化群组缓存"""
    data_list = await GroupConsole.all()
    return {p.group_id: p for p in data_list if not p.channel_id}


@CacheRoot.updater(CacheType.GROUPS)
async def _(data: dict[str, GroupConsole], key: str, value: Any):
    """更新群组缓存"""
    if value:
        data[key] = value
    elif group := await GroupConsole.get_group(group_id=key):
        data[key] = group


@CacheRoot.getter(CacheType.GROUPS, result_model=GroupConsole)
async def _(cache_data: CacheData, group_id: str):
    """获取群组缓存"""
    data = await cache_data.get_data() or {}
    if group_id not in data:
        if group := await GroupConsole.get_group(group_id=group_id):
            data[group_id] = group
            await cache_data.set_data(data)
    return data.get(group_id)


@CacheRoot.with_refresh(CacheType.GROUPS)
async def _(data: dict[str, GroupConsole] | None):
    """刷新群组缓存"""
    if not data:
        return
    groups = await GroupConsole.filter(
        group_id__in=data.keys(), channel_id__isnull=True
    ).all()
    data.update({g.group_id: g for g in groups})


@CacheRoot.new(CacheType.BOT)
async def _():
    """初始化机器人缓存"""
    data_list = await BotConsole.all()
    return {p.bot_id: p for p in data_list}


@CacheRoot.updater(CacheType.BOT)
async def _(data: dict[str, BotConsole], key: str, value: Any):
    """更新机器人缓存"""
    if value:
        data[key] = value
    elif bot := await BotConsole.get_or_none(bot_id=key):
        data[key] = bot


@CacheRoot.getter(CacheType.BOT, result_model=BotConsole)
async def _(cache_data: CacheData, bot_id: str):
    """获取机器人缓存"""
    data = await cache_data.get_data() or {}
    if bot_id not in data:
        if bot := await BotConsole.get_or_none(bot_id=bot_id):
            data[bot_id] = bot
            await cache_data.set_data(data)
    return data.get(bot_id)


@CacheRoot.with_refresh(CacheType.BOT)
async def _(data: dict[str, BotConsole] | None):
    """刷新机器人缓存"""
    if not data:
        return
    bots = await BotConsole.filter(bot_id__in=data.keys()).all()
    data.update({b.bot_id: b for b in bots})


@CacheRoot.new(CacheType.USERS)
async def _():
    """初始化用户缓存"""
    data_list = await UserConsole.all()
    return {p.user_id: p for p in data_list}


@CacheRoot.updater(CacheType.USERS)
async def _(data: dict[str, UserConsole], key: str, value: Any):
    """更新用户缓存"""
    if value:
        data[key] = value
    elif user := await UserConsole.get_user(user_id=key):
        data[key] = user


@CacheRoot.getter(CacheType.USERS, result_model=UserConsole)
async def _(cache_data: CacheData, user_id: str):
    """获取用户缓存"""
    data = await cache_data.get_data() or {}
    if user_id not in data:
        if user := await UserConsole.get_user(user_id=user_id):
            data[user_id] = user
            await cache_data.set_data(data)
    return data.get(user_id)


@CacheRoot.with_refresh(CacheType.USERS)
async def _(data: dict[str, UserConsole] | None):
    """刷新用户缓存"""
    if not data:
        return
    users = await UserConsole.filter(user_id__in=data.keys()).all()
    data.update({u.user_id: u for u in users})


@CacheRoot.new(CacheType.LEVEL, False)
async def _():
    """初始化等级缓存"""
    return await LevelUser().all()


@CacheRoot.getter(CacheType.LEVEL, result_model=list[LevelUser])
async def _(cache_data: CacheData, user_id: str, group_id: str | None = None):
    """获取等级缓存"""
    data = await cache_data.get_data() or []
    if not group_id:
        return [d for d in data if d.user_id == user_id and not d.group_id]
    return [d for d in data if d.user_id == user_id and d.group_id == group_id]


@CacheRoot.new(CacheType.BAN, False)
async def _():
    """初始化封禁缓存"""
    return await BanConsole.all()


@CacheRoot.getter(CacheType.BAN, result_model=list[BanConsole])
async def _(cache_data: CacheData, user_id: str | None, group_id: str | None = None):
    """获取封禁缓存"""
    data = await cache_data.get_data() or []
    if user_id:
        if group_id:
            return [d for d in data if d.user_id == user_id and d.group_id == group_id]
        return [d for d in data if d.user_id == user_id and not d.group_id]
    if group_id:
        return [d for d in data if not d.user_id and d.group_id == group_id]
    return None


@CacheRoot.new(CacheType.LIMIT)
async def _():
    """初始化限制缓存"""
    data_list = await PluginLimit.filter(status=True).all()
    result_data = {}
    for data in data_list:
        if not result_data.get(data.module):
            result_data[data.module] = []
        result_data[data.module].append(data)
    return result_data


@CacheRoot.updater(CacheType.LIMIT)
async def _(data: dict[str, list[PluginLimit]], key: str, value: Any):
    """更新限制缓存"""
    if value:
        data[key] = value
    elif limits := await PluginLimit.filter(module=key, status=True):
        data[key] = limits


@CacheRoot.getter(CacheType.LIMIT, result_model=list[PluginLimit])
async def _(cache_data: CacheData, module: str):
    """获取限制缓存"""
    data = await cache_data.get_data() or {}
    if module not in data:
        if limits := await PluginLimit.filter(module=module, status=True):
            data[module] = limits
            await cache_data.set_data(data)
    return data.get(module)


@CacheRoot.with_refresh(CacheType.LIMIT)
async def _(data: dict[str, list[PluginLimit]] | None):
    """刷新限制缓存"""
    if not data:
        return
    limits = await PluginLimit.filter(module__in=data.keys(), load_status=True).all()
    new_data = {}
    for limit in limits:
        if not new_data.get(limit.module):
            new_data[limit.module] = []
        new_data[limit.module].append(limit)
    data.clear()
    data.update(new_data)
