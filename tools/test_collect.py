import asyncio, logging

from bot.report_sender import collect_saved_user_reports

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s:%(name)s:%(message)s')

class FakeUM:
    async def get_latest_account_data(self, user_id):
        # return None for even-indexed users to simulate missing saved data
        if user_id.endswith('0') or user_id.endswith('2') or user_id.endswith('4'):
            return None
        return {'id': user_id, 'usage': '5', 'today_balance': '10'}

async def main():
    users = [
        {'id': f'id{i}', 'username': str(1000 + i), 'adsl_number': '', 'status': 'active'}
        for i in range(6)
    ]
    sem = asyncio.Semaphore(2)
    collected = await collect_saved_user_reports(users, sem, FakeUM())
    print('RESULT LEN', len(collected))
    for u, d in collected:
        print(u, 'notes=', d.get('notes'), 'usage=', d.get('usage'))

if __name__ == '__main__':
    asyncio.run(main())
