# bot/handlers/main_menu.py

from aiogram import types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.context import FSMContext
from bot.app import dp
from bot.utils import block_if_active_flow


def build_command_menu_inline():
    # Inline keyboard with callbacks that map to the main commands.
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🚀 بدء البوت", callback_data="cmd:start")],
            [InlineKeyboardButton(text="🧾 ملخص حسابك", callback_data="cmd:account")],
            [InlineKeyboardButton(text="📡 الشبكات", callback_data="cmd:networks"),InlineKeyboardButton(text="👥 خطوط النت", callback_data="cmd:adsls")],
            [InlineKeyboardButton(text="📄 التقارير", callback_data="cmd:reports")],
            [InlineKeyboardButton(text="⚙️ إعدادات", callback_data="cmd:settings")],
            [InlineKeyboardButton(text="ℹ️ حول البوت", callback_data="cmd:about"),InlineKeyboardButton(text="❓ المساعدة", callback_data="cmd:help")],
            
        ]
    )


def build_command_menu_reply():
    # Persistent keyboard with KeyboardButtons (no slashes), labels mirror command descriptions.
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text="🚀 بدء البوت")],
            [KeyboardButton(text="🧾 ملخص حسابك")],
            [KeyboardButton(text="📡 الشبكات"),KeyboardButton(text="👥 خطوط النت")],
            [KeyboardButton(text="📄 التقارير")],
            [KeyboardButton(text="⚙️ إعدادات")],
            [KeyboardButton(text="ℹ️ حول البوت"),KeyboardButton(text="❓ المساعدة")],
        ],
    )


@dp.message(Command("menu"))
@dp.message(Command("start"))
async def show_main_menu(message: types.Message, state: FSMContext):
    if await block_if_active_flow(message, state):
        return
    await message.answer("⬇️ اختر من القائمة:", reply_markup=build_command_menu_reply())

@dp.callback_query(lambda c: c.data == "menu_close")
async def menu_close(call: types.CallbackQuery):
    await call.answer("تم الإغلاق")
    await call.message.delete()


# Generic handler to map command callbacks (cmd:<name>) to sending the corresponding slash command
@dp.callback_query(lambda c: c.data.startswith("cmd:"))
async def menu_command_callback(call: types.CallbackQuery, state: FSMContext):
    if await block_if_active_flow(call, state):
        return
    cmd = call.data.split(":", 1)[-1].strip()
    if not cmd:
        await call.answer()
        return
    try:
        await call.answer()
    except Exception:
        pass

    # Call handlers directly to avoid echoing /command
    try:
        if cmd == "start":
            from bot.handlers.user_handlers import start_handler
            await start_handler(call.message, state)
            return
        if cmd == "networks":
            from bot.handlers.user_handlers import networks_menu
            await networks_menu(call.message, state)
            return
        if cmd == "adsls":
            from bot.handlers.user_handlers import adsls_menu
            await adsls_menu(call.message, state)
            return
        if cmd == "reports":
            from bot.handlers.user_handlers import mysummary_command
            await mysummary_command(call.message, state=state)
            return
        if cmd == "account":
            from bot.handlers.user_handlers import status_command
            await status_command(call.message, state)
            return
        if cmd == "settings":
            from bot.handlers.user_handlers import settings_handler
            await settings_handler(call.message, state)
            return
        if cmd == "about":
            from bot.handlers.user_handlers import about_command
            await about_command(call.message, state)
            return
        if cmd == "help":
            from bot.handlers.help_menu import help_command
            await help_command(call.message, state)
            return
    except Exception:
        pass

    try:
        await call.message.answer(f"/{cmd}")
    except Exception:
        pass


# Map reply-keyboard button text to commands (no slashes in text)
_TEXT_TO_CMD = {
    "🚀 بدء البوت": "start",
    "📡 الشبكات": "networks",
    "👥 خطوط النت": "adsls",
    "📄 التقارير": "reports",
    "🧾 ملخص حسابك": "account",
    "⚙️ إعدادات": "settings",
    "ℹ️ حول البوت": "about",
    "❓ المساعدة": "help",
}


@dp.message(lambda m: m.text in _TEXT_TO_CMD)
async def menu_reply_button_handler(message: types.Message, state: FSMContext):
    if await block_if_active_flow(message, state):
        return
    cmd = _TEXT_TO_CMD.get(message.text)
    if not cmd:
        return
    if cmd == "start":
        # Call the existing start handler directly to avoid echoing /start
        try:
            from bot.handlers.user_handlers import start_handler
            await start_handler(message, state)
            return
        except Exception:
            pass
    if cmd == "networks":
        # Call the existing networks handler directly to avoid echoing /networks
        try:
            from bot.handlers.user_handlers import networks_menu
            await networks_menu(message, state)
            return
        except Exception:
            pass
    if cmd == "adsls":
        # Call the existing adsls handler directly to avoid echoing /adsls
        try:
            from bot.handlers.user_handlers import adsls_menu
            await adsls_menu(message, state)
            return
        except Exception:
            pass
    if cmd == "reports":
        # Call the existing reports handler directly to avoid echoing /reports
        try:
            from bot.handlers.user_handlers import mysummary_command
            await mysummary_command(message, state=state)
            return
        except Exception:
            pass
    if cmd == "account":
        # Call the existing account handler directly to avoid echoing /account
        try:
            from bot.handlers.user_handlers import status_command
            await status_command(message, state)
            return
        except Exception:
            pass
    if cmd == "settings":
        # Call the existing settings handler directly to avoid echoing /settings
        try:
            from bot.handlers.user_handlers import settings_handler
            await settings_handler(message, state)
            return
        except Exception:
            pass
    if cmd == "about":
        # Call the existing about handler directly to avoid echoing /about
        try:
            from bot.handlers.user_handlers import about_command
            await about_command(message, state)
            return
        except Exception:
            pass
    if cmd == "help":
        # Call the existing help handler directly to avoid echoing /help
        try:
            from bot.handlers.help_menu import help_command
            await help_command(message, state)
            return
        except Exception:
            pass
    # Fallback: send the slash command to trigger existing handlers
    await message.answer(f"/{cmd}")
