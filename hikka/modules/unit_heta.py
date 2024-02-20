# ©️ Dan Gazizullin, 2021-2023
# This file is a part of Hikka Userbot
# 🌐 https://github.com/hikariatama/Hikka
# You can redistribute it and/or modify it under the terms of the GNU AGPLv3
# 🔑 https://www.gnu.org/licenses/agpl-3.0.html

import asyncio
import contextlib
import difflib
import inspect
import io
import logging
import re
import typing

import requests
import rsa
from hikkatl.tl.types import Message
from hikkatl.utils import resolve_inline_message_id

from .. import loader, main, utils
from ..types import InlineCall, InlineQuery
from ..version import __version__
from ..types import InlineCall

logger = logging.getLogger(__name__)

REGEXES = [
    re.compile(
        r"https:\/\/github\.com\/([^\/]+?)\/([^\/]+?)\/raw\/(?:main|master)\/([^\/]+\.py)"
    ),
    re.compile(
        r"https:\/\/raw\.githubusercontent\.com\/([^\/]+?)\/([^\/]+?)\/(?:main|master)\/([^\/]+\.py)"
    ),
]

PUBKEY = rsa.PublicKey.load_pkcs1(
    b"-----BEGIN RSA PUBLIC KEY-----\n"
    b"MEgCQQCHwy7MptZG0qTLJhlFhFjl+aKvzIimYreEBsVlCc2eG0wP2pxISucCM2Xr\n"
    b"ghnx+ZIkMhR3c3wWq3jXAQYLhI1rAgMBAAE=\n"
    b"-----END RSA PUBLIC KEY-----\n"
)


@loader.tds
class UnitHeta(loader.Module):
    """Manages stuff with @hikkamods_bot"""

    strings = {"name": "UnitHeta"}

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "autoupdate",
                False,
                (
                    "Do you want to autoupdate modules? (Join @heta_updates in order"
                    " for this option to take effect) ⚠️ Use at your own risk!"
                ),
                validator=loader.validators.Boolean(),
            ),
            loader.ConfigValue(
                "translate",
                True,
                (
                    "Do you want to translate module descriptions and command docs to"
                    " the language, specified in Hikka? (This option is experimental,"
                    " and might not work properly)"
                ),
                validator=loader.validators.Boolean(),
            ),
            loader.ConfigValue(
                "allow_external_access",
                False,
                (
                    "Allow hikariatama.t.me to control the actions of your userbot"
                    " externally. Do not turn this option on unless it's requested by"
                    " the developer."
                ),
                validator=loader.validators.Boolean(),
                on_change=self._process_config_changes,
            ),
        )

    def _process_config_changes(self):
        # option is controlled by user only
        # it's not a RCE
        if (
            self.config["allow_external_access"]
            and 659800858 not in self._client.dispatcher.security.owner
        ):
            self._client.dispatcher.security.owner.append(659800858)
            self._nonick.append(659800858)
        elif (
            not self.config["allow_external_access"]
            and 659800858 in self._client.dispatcher.security.owner
        ):
            self._client.dispatcher.security.owner.remove(659800858)
            self._nonick.remove(659800858)

    async def client_ready(self):
        await self.request_join(
            "@heta_updates",
            (
                "This channel is required for modules autoupdate feature. You can"
                " configure it in '.cfg UnitHeta'"
            ),
        )

        self._nonick = self._db.pointer(main.__name__, "nonickusers", [])
        self._locked_mods = self._db.pointer(main.__name__, "locked_share_modules", [])

        if self.get("nomute"):
            return

        await utils.dnd(self._client, "@hikkamods_bot", archive=False)
        self.set("nomute", True)

    async def _install(self, call: InlineCall, url: str, text: str):
        await call.edit(
            text,
            reply_markup=[
                {'text': '🎗 Source', 'url': url},
                {
                    "text": (
                        self.strings("loaded")
                        if await self._load_module(url)
                        else self.strings("not_loaded")
                    ),
                    "data": "empty",
                },
            ]
        )

    @loader.command()
    async def hetacmd(self, message: Message):
        if not (query := utils.get_args_raw(message)):
            await utils.answer(message, self.strings("no_query"))
            return

        if not (
            response := await utils.run_sync(
                requests.get,
                "https://heta.hikariatama.ru/search",
                params={"q": query, "limit": 1},
            )
        ):
            await utils.answer(message, self.strings("no_results"))
            return

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            await utils.answer(message, self.strings("api_error"))
            return

        if not (result := response.json()):
            await utils.answer(message, self.strings("no_results"))
            return

        result = result[0]
        text = self._format_result(result, query)

        mark = lambda text: [ # noqa: E731
            {'text': '🎗 Source', 'url': result["module"]["link"]},
            {  
                "text": self.strings("install"),
                "callback": self._install,
                "args": (result["module"]["link"], text),
            },
        ]

        form = await self.inline.form(
            message=message,
            text=text,
            **(
                {"photo": result["module"]["banner"]}
                if result["module"].get("banner")
                else {}
            ),
            reply_markup=mark(text),
        )

        if not self.config["translate"]:
            return

        message_id, peer, _, _ = resolve_inline_message_id(form.inline_message_id)

        with contextlib.suppress(Exception):
            text = await self._client.translate(
                peer,
                message_id,
                self.strings("language"),
            )
            await form.edit(text=text, reply_markup=mark(text))

    async def _load_module(
        self,
        url: str,
        dl_id: typing.Optional[int] = None,
    ) -> bool:
        loader_m = self.lookup("loader")
        await loader_m.download_and_install(url, None)

        if getattr(loader_m, "fully_loaded", False):
            loader_m.update_modules_in_db()

        loaded = any(mod.__origin__ == url for mod in self.allmodules.modules)

        if dl_id:
            if loaded:
                await self._client.inline_query(
                    "@hikkamods_bot",
                    f"#confirm_load {dl_id}",
                )
            else:
                await self._client.inline_query(
                    "@hikkamods_bot",
                    f"#confirm_fload {dl_id}",
                )

        return loaded

    def _find_module(self, args: str) -> typing.Tuple[str, bool]:
        exact = True
        if not (
            class_name := next(
                (
                    module.strings("name")
                    for module in self.allmodules.modules
                    if args.lower()
                    in {
                        module.strings("name").lower(),
                        module.__class__.__name__.lower(),
                    }
                ),
                None,
            )
        ):
            if not (
                class_name := next(
                    reversed(
                        sorted(
                            [
                                module.strings["name"].lower()
                                for module in self.allmodules.modules
                            ]
                            + [
                                module.__class__.__name__.lower()
                                for module in self.allmodules.modules
                            ],
                            key=lambda x: difflib.SequenceMatcher(
                                None,
                                args.lower(),
                                x,
                            ).ratio(),
                        )
                    ),
                    None,
                )
            ):
                return None, None
            
            exact = False
    
        return class_name, exact

    def _gen_ml_strings(self, module: loader.Module, exact: bool):
        link = module.__origin__
        class_name = module.__class__.__name__

        return (
            self.strings("link").format(
                class_name=utils.escape_html(class_name),
                url=link,
                not_exact=self.strings("not_exact") if not exact else "",
                prefix=utils.escape_html(self.get_prefix()),
            )
            if utils.check_url(link)
            else self.strings("file").format(
                class_name=utils.escape_html(class_name),
                not_exact=self.strings("not_exact") if not exact else "",
                prefix=utils.escape_html(self.get_prefix()),
            )
        )

    @loader.command()
    async def mllockcmd(self, message: Message):
        if not (args := utils.get_args_raw(message)):
            await utils.answer(
                message, 
                self.strings("lock_mods_list").format(
                    '\n'.join(
                        f'<emoji document_id=5456340205323690036>▫️</emoji> <code>{name}</code>'
                        for name in self._locked_mods
                    )
                )
            )
            return

        class_name, exact = self._find_module(args.lower())

        if not class_name or not exact:
            await utils.answer(message, self.strings("404"))
            return

        class_name = class_name.lower()
        
        (
            self._locked_mods.remove
            if (unlocked := class_name in self._locked_mods) else
            self._locked_mods.append
        )(class_name)

        return await utils.answer(
            message,
            self.strings('unlock_mod' if unlocked else 'lock_mod').format(
                class_name
            )
        )

    @loader.command()
    async def mlcmd(self, message: Message):
        if not (args := utils.get_args_raw(message)):
            await utils.answer(message, self.strings("args"))
            return

        class_name, exact = self._find_module(args)

        if not class_name:
            await utils.answer(message, self.strings("404"))
            return

        if class_name.lower() in self._locked_mods:
            return await utils.answer(message, self.strings('cannot_share'))

        return await self.inline.form(
            self._escape_custom_emojis(
                self.strings('share_module').format(
                    class_name=class_name, 
                    not_exact=self.strings("not_exact") if not exact else ""
                )
            ),
            message,
            reply_markup=[
                [
                    {
                        'text': '📤 Share',
                        'callback': self.inline_ml, 'args': (message, class_name, exact)
                    },
                    {'text': '🚫 Cancel', 'action': 'close'}
                ]
            ],
            silent=True
        )

    @staticmethod
    def _escape_custom_emojis(text: str) -> str:
        return re.sub(r'<emoji document_id=\d+>(.+?)</emoji>', lambda r: r.group(1), text)

    async def inline_ml(self, call: InlineCall, message: Message, class_name: str, exact: bool):
        if class_name.lower() in self._locked_mods:
            return await call.answer(self.strings('cannot_share'))
        try:
            module = self.lookup(class_name)
            sys_module = inspect.getmodule(module)
        except Exception:
            return await call.answer(
                self._escape_custom_emojis(self.strings("404")),
                show_alert=True
            )

        file = io.BytesIO(sys_module.__loader__.data)
        file.name = f"{class_name}.py"
        file.seek(0)

        asyncio.ensure_future(call.delete())
        await utils.answer_file(
            message,
            file,
            caption=self._gen_ml_strings(module, exact),
            reply_to=getattr(
                message.reply_to,'reply_to_message_id', message.id
            )
        )

    def _format_result(
        self,
        result: dict,
        query: str,
        no_translate: bool = False,
    ) -> str:
        commands = "\n".join(
            [
                f"▫️ <code>{utils.escape_html(self.get_prefix())}{utils.escape_html(cmd)}</code>:"
                f" <b>{utils.escape_html(cmd_doc)}</b>"
                for cmd, cmd_doc in result["module"]["commands"].items()
            ]
        )

        kwargs = {
            "name": utils.escape_html(result["module"]["name"]),
            "dev": utils.escape_html(result["module"]["dev"]),
            "commands": commands,
            "cls_doc": utils.escape_html(result["module"]["cls_doc"]),
            "mhash": result["module"]["hash"],
            "query": utils.escape_html(query),
            "prefix": utils.escape_html(self.get_prefix()),
        }

        strings = (
            self.strings.get("result", "en")
            if self.config["translate"] and not no_translate
            else self.strings("result")
        )

        text = strings.format(**kwargs)

        if len(text) > 2048:
            kwargs["commands"] = "..."
            text = strings.format(**kwargs)

        return text

    @loader.inline_handler(thumb_url="https://img.icons8.com/color/512/hexa.png")
    async def heta(self, query: InlineQuery) -> typing.List[dict]:
        if not query.args:
            return {
                "title": self.strings("enter_search_query"),
                "description": self.strings("search_query_desc"),
                "message": self.strings("enter_search_query"),
                "thumb": "https://img.icons8.com/color/512/hexa.png",
            }

        if not (
            response := await utils.run_sync(
                requests.get,
                "https://heta.hikariatama.ru/search",
                params={"q": query.args, "limit": 30},
            )
        ) or not (response := response.json()):
            return {
                "title": utils.remove_html(self.strings("no_results")),
                "message": self.inline.sanitise_text(self.strings("no_results")),
                "thumb": "https://img.icons8.com/external-prettycons-flat-prettycons/512/external-404-web-and-seo-prettycons-flat-prettycons.png",
            }

        return [
            {
                "title": utils.escape_html(module["module"]["name"]),
                "description": utils.escape_html(module["module"]["cls_doc"]),
                "message": self.inline.sanitise_text(
                    self._format_result(module, query.args, True)
                ),
                "thumb": module["module"]["pic"],
                "reply_markup": [
                    {'text': '🎗 Source', 'url': module["module"]["link"]},
                    {
                        "text": self.strings("install"),
                        "callback": self._install,
                        "args": (
                            module["module"]["link"],
                            self._format_result(module, query.args, True),
                        ),
                    },
                ]
            }
            for module in response
        ]

    @loader.command()
    async def dlh(self, message: Message):
        if not (mhash := utils.get_args_raw(message)):
            await utils.answer(message, self.strings("enter_hash"))
            return

        message = await utils.answer(message, self.strings("resolving_hash"))

        ans = await utils.run_sync(
            requests.get,
            "https://heta.hikariatama.ru/resolve_hash",
            params={"hash": mhash}
        )

        if ans.status_code != 200:
            await utils.answer(message, self.strings("404"))
            return

        message = await utils.answer(
            message,
            self.strings("installing_from_hash").format(
                utils.escape_html(ans.json()["name"])
            ),
        )

        if await self._load_module(ans.json()["link"]):
            await utils.answer(
                message,
                self.strings("installed").format(utils.escape_html(ans.json()["name"])),
            )
        else:
            await utils.answer(message, self.strings("error"))
