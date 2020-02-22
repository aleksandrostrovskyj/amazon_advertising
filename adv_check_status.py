import json
import aiohttp
import asyncio
import logging
# User modules
from amazon_advertise import Reports
from connectors import redis_connections_pool

# TODO - collect data from the queue in a bundle and implement asyncio.gather()?
# TODO - Possible negative impact: high probability of receiving too many requests response, check later


async def main():
    amz = Reports()
    pool = await redis_connections_pool()

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
            report_date = data_json['report_date']
            report_type = data_json['recordType']

            async with aiohttp.ClientSession() as session:
                response = await amz.status(session=session, report_id=report_id)
                response_json = await response.json()

            # Add report date and report type
            response_json.update({'report_date': report_date, 'recordType': report_type})
            report_status = response_json['status']
            logging.info(f'Report {report_id} has status {report_status}; Push to redis_list{[report_status]}')
            # Add to the redis pool (depend on status) serialized json string
            await conn.execute('RPUSH', redis_list[report_status], json.dumps(response_json))


asyncio.run(main())
