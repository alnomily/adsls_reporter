from aiogram import types

from bot.app import dp


@dp.callback_query(lambda c: c.data and c.data.startswith("refresh_"))
async def refresh_callback(callback: types.CallbackQuery) -> None:
    username = callback.data.replace("refresh_", "")
    await callback.answer(f"🔄 تحديث {username}...")
    fake_msg = types.Message(
        message_id=callback.message.message_id,
        from_user=callback.from_user,
        chat=callback.message.chat,
        date=callback.message.date,
        text=f"/checklive {username}"
    )
    # Call the checklive handler by dispatching a fake message through the same handler
    # to avoid import-time circular dependencies.
    await dp.feed_update(fake_msg)


@dp.callback_query(lambda c: c.data and c.data.startswith("live_"))
async def live_callback(callback: types.CallbackQuery) -> None:
    username = callback.data.replace("live_", "")
    await callback.answer(f"📡 فحص مباشر لـ {username}")
    fake_msg = types.Message(
        message_id=callback.message.message_id,
        from_user=callback.from_user,
        chat=callback.message.chat,
        date=callback.message.date,
        text=f"/checklive {username}"
    )
    await dp.feed_update(fake_msg)
