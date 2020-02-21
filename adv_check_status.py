import json
import aiohttp
import asyncio
import logging
# User modules
from amazon_advertise import Reports
from connectors import redis_pool


async def main():
    amz = Reports()
    pool = await redis_pool()

    redis_list = {
        'SUCCESS': 'targets:ready',
        'IN_PROGRESS': 'targets:requested',
        'FAILURE': 'targets:failure'
    }

    while True:
        with await pool as conn:
            data = await conn.execute('LPOP', 'targets:requested')
            # Check if this is the end of the list (to avoid endless loop)
            if data == b'None':
                logging.info('Get None, RPUSH back and exit')
                await conn.execute('RPUSH', 'targets:requested', data)
                # Close pool
                pool.close()
                await pool.wait_closed()
                break

            data_json = json.loads(data)
            report_id = data_json['reportId']

            async with aiohttp.ClientSession() as session:
                response = await amz.status(session=session, report_id=report_id)
                response_json = await response.json()

            report_status = response_json['status']
            logging.info(f'Report {report_id} has status {report_status}; Push to redis_list{[report_status]}')
            await conn.execute('RPUSH', redis_list[report_status], json.dumps(response_json))


asyncio.run(main())
