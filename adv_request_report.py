import json
import aiohttp
import asyncio
import logging
from settings import metrics
from datetime import datetime, date, timedelta
# User modules
from amazon_advertise import Reports
from connectors import redis_connections_pool


async def sp_targets(amz: type(Reports), pool, report_date: date):
    api_url = '/v2/sp/targets/report'
    request_metrics = metrics['sp']['targets']
    str_report_date = datetime.strftime(report_date, '%Y%m%d')

    body = {
        'metrics': ','.join(request_metrics),
        'reportDate': str_report_date
    }
    asyncio.sleep(0.5)
    logging.info('Request report')
    async with aiohttp.ClientSession() as session:
        # TODO - add check if there were to many requests to the endpoint
        response = await amz.request(session=session, api_url=api_url, body=json.dumps(body))
        # Read response content cause Amazon for this endpoint return incorrect Content-Type
        response_text = await response.read()

    # Convert content to dict and add report_date
    response_data = json.loads(response_text)
    report_id = response_data['reportId']
    response_data.update({'report_date': datetime.strftime(report_date, '%Y-%m-%d')})

    with await pool as conn:
        logging.info(f'Report {report_id} has been added to targets:requested')
        # Add to the redis pool serialized json string
        await conn.execute('LPUSH', 'targets:requested', json.dumps(response_data))


async def main(period: int):
    amz = Reports()
    pool = await redis_connections_pool()
    date_list = [datetime.today()-timedelta(days=i) for i in range(period, 0, -1)]
    tasks = []
    for report_date in date_list:
        tasks.append(asyncio.create_task(sp_targets(amz, pool, report_date)))

    await asyncio.gather(*tasks)
    pool.close()
    await pool.wait_closed()

if __name__ == '__main__':
    asyncio.run(main(60))
