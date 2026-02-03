import asyncio
from bot.user_manager import UserManager

async def main():
    try:
        res = await UserManager.get_latest_account_data('00000000-0000-0000-0000-000000000000')
        print('RESULT:', res)
    except Exception as e:
        print('EXCEPTION:', e)

if __name__ == '__main__':
    asyncio.run(main())
