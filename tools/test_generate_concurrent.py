import asyncio, os
from bot.report_sender import generate_images

# Prepare sample user_reports with minimal data
sample = [(str(1000+i), {'id': f'id{i}', 'username': str(1000+i), 'adsl_number': '', 'plan': 'Basic', 'today_balance': '10', 'usage': '1'}) for i in range(6)]

async def gen_once(name):
    print('Start gen', name)
    loop = asyncio.get_event_loop()
    images, out_dir = await loop.run_in_executor(None, generate_images, sample)
    print('Done gen', name, images, out_dir)
    return images, out_dir

async def main():
    # run two generators concurrently to simulate overlapping report generation
    t1 = asyncio.create_task(gen_once('A'))
    t2 = asyncio.create_task(gen_once('B'))
    res = await asyncio.gather(t1, t2)
    all_paths = [p for images, out in res for p in images]
    print('All paths exist:', all(os.path.exists(p) for p in all_paths))
    print('Paths:', all_paths)
    # verify out directories exist then cleanup
    out_dirs = [out for images, out in res]
    print('Out dirs exist:', all(os.path.isdir(d) for d in out_dirs))

if __name__ == '__main__':
    asyncio.run(main())
