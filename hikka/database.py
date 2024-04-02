# ©️ Dan Gazizullin, 2021-2023
# This file is a part of Hikka Userbot
# 🌐 https://github.com/hikariatama/Hikka
# You can redistribute it and/or modify it under the terms of the GNU AGPLv3
# 🔑 https://www.gnu.org/licenses/agpl-3.0.html

import asyncio
import collections
import json
import orjson
import logging
import os
import time
import aiofiles

try:
    import redis
except ImportError as e:
    if "RAILWAY" in os.environ:
        raise e


import typing

from hikkatl.errors.rpcerrorlist import ChannelsTooMuchError
from hikkatl.tl.types import Message, User

from . import main, utils
from .pointers import (
    BaseSerializingMiddlewareDict,
    BaseSerializingMiddlewareList,
    NamedTupleMiddlewareDict,
    NamedTupleMiddlewareList,
    PointerDict,
    PointerList,
)
from .tl_cache import CustomTelegramClient
from .types import JSONSerializable

__all__ = [
    "Database",
    "PointerList",
    "PointerDict",
    "NamedTupleMiddlewareDict",
    "NamedTupleMiddlewareList",
    "BaseSerializingMiddlewareDict",
    "BaseSerializingMiddlewareList",
]

logger = logging.getLogger(__name__)


class NoAssetsChannel(Exception):
    """Raised when trying to read/store asset with no asset channel present"""


class Database(dict):
    def __init__(self, client: CustomTelegramClient):
        super().__init__()
        self._client: CustomTelegramClient = client
        self._next_revision_call: int = 0
        self._revisions: typing.List[dict] = []
        self._assets: int = None
        self._me: User = None
        self._redis: redis.Redis = None
        self._saving_task: asyncio.Future = None
        self._save_lock: asyncio.Lock = asyncio.Lock()

    def __repr__(self):
        return object.__repr__(self)

    def _redis_save_sync(self):
        with self._redis.pipeline() as pipe:
            pipe.set(
                f'hikka-{self._client.tg_id}',
                orjson.dumps(self),
            )
            pipe.execute()

    async def remote_force_save(self) -> bool:
        """Force save database to remote endpoint without waiting"""
        if self._redis:
            await utils.run_sync(self._redis_save_sync)
        else:
            await self._local_save_async()

        return True

    async def _redis_save(self) -> bool:
        """Save database to redis"""
        await asyncio.sleep(5)
        await utils.run_sync(self._redis_save_sync)
        self._saving_task = None
        
        return True

    async def redis_init(self) -> bool:
        """Init redis database"""
        if REDIS_URI := (
            os.environ.get("REDIS_URL") or main.get_config_key("redis_uri")
        ):
            self._redis = redis.Redis.from_url(REDIS_URI)
            logger.info('Redis database succefully inited!')
        else:
            return False

    async def init(self):
        """Asynchronous initialization unit"""
        if os.environ.get("REDIS_URL") or main.get_config_key("redis_uri"):
            await self.redis_init()

        self._db_file = main.BASE_PATH / f"config-{self._client.tg_id}.json"
        self.read()

        try:
            self._assets, _ = await utils.asset_channel(
                self._client,
                "hikka-assets",
                "🌆 Your Hikka assets will be stored here",
                archive=True,
                avatar="https://raw.githubusercontent.com/hikariatama/assets/master/hikka-assets.png",
            )
        except ChannelsTooMuchError:
            self._assets = None
            logger.error(
                "Can't find and/or create assets folder\n"
                "This may cause several consequences, such as:\n"
                "- Non working assets feature (e.g. notes)\n"
                "- This error will occur every restart\n\n"
                "You can solve this by leaving some channels/groups"
            )

    def read(self):
        """Read database and stores it in self"""
        if self._redis:
            try:
                self.update(
                    **orjson.loads(
                        self._redis.get(
                            f'hikka-{self._client.tg_id}',
                        )
                        or '{}'
                    )
                )
            except Exception:
                logger.exception("Error reading redis database")
            return

        try:
            self.update(**orjson.loads(self._db_file.read_text()))
        except orjson.JSONDecodeError:
            logger.warning("Database read failed! Creating new one...")
        except FileNotFoundError:
            logger.debug("Database file not found, creating new one...")

    def save(self) -> bool:
        """Save database"""
        if not self._saving_task:
            method = self._redis_save() if self._redis else self._local_save()
            self._saving_task = asyncio.ensure_future(method)

        return True
    
    async def _local_save(self):
        await asyncio.sleep(1)
        return await self._local_save_async()

    async def _local_save_async(self) -> bool:
        try:
            async with self._save_lock:
                async with aiofiles.open(self._db_file, 'wb') as f:
                    await f.write(orjson.dumps(self))
        except Exception:
            logger.exception("Database save failed!")
            return False
        return True
    
    async def store_asset(self, message: Message) -> int:
        """
        Save assets
        returns asset_id as integer
        """
        if not self._assets:
            raise NoAssetsChannel("Tried to save asset to non-existing asset channel")

        return (
            (await self._client.send_message(self._assets, message)).id
            if isinstance(message, Message)
            else (
                await self._client.send_message(
                    self._assets,
                    file=message,
                    force_document=True,
                )
            ).id
        )

    async def fetch_asset(self, asset_id: int) -> typing.Optional[Message]:
        """Fetch previously saved asset by its asset_id"""
        if not self._assets:
            raise NoAssetsChannel(
                "Tried to fetch asset from non-existing asset channel"
            )

        asset = await self._client.get_messages(self._assets, ids=[asset_id])

        return asset[0] if asset else None

    def get(
        self,
        owner: str,
        key: str,
        default: typing.Optional[JSONSerializable] = None,
    ) -> JSONSerializable:
        """Get database key"""
        try:
            return self[owner][key]
        except KeyError:
            return default

    def set(self, owner: str, key: str, value: JSONSerializable) -> bool:
        """Set database key"""
        super().setdefault(owner, {})[key] = value
        return self.save()

    def pointer(
        self,
        owner: str,
        key: str,
        default: typing.Optional[JSONSerializable] = None,
        item_type: typing.Optional[typing.Any] = None,
    ) -> typing.Union[JSONSerializable, PointerList, PointerDict]:
        """Get a pointer to database key"""
        value = self.get(owner, key, default)
        mapping = {
            list: PointerList,
            dict: PointerDict,
            collections.abc.Hashable: lambda v: v,
        }

        pointer_constructor = next(
            (pointer for type_, pointer in mapping.items() if isinstance(value, type_)),
            None,
        )

        if pointer_constructor is None:
            raise ValueError(
                f"Pointer for type {type(value).__name__} is not implemented"
            )

        if item_type is not None:
            if isinstance(value, list):
                for item in self.get(owner, key, default):
                    if not isinstance(item, dict):
                        raise ValueError(
                            "Item type can only be specified for dedicated keys and"
                            " can't be mixed with other ones"
                        )

                return NamedTupleMiddlewareList(
                    pointer_constructor(self, owner, key, default),
                    item_type,
                )
            if isinstance(value, dict):
                for item in self.get(owner, key, default).values():
                    if not isinstance(item, dict):
                        raise ValueError(
                            "Item type can only be specified for dedicated keys and"
                            " can't be mixed with other ones"
                        )

                return NamedTupleMiddlewareDict(
                    pointer_constructor(self, owner, key, default),
                    item_type,
                )

        return pointer_constructor(self, owner, key, default)