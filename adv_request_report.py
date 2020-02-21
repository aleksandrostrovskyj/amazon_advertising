import json
import aiohttp
import asyncio
import logging
from settings import metrics
from datetime import datetime, date, timedelta
# User modules
from amazon_advertise import Reports
from connectors import redis_pool


async def sp_targets(amz: type(Reports), pool, report_date: date):
    api_url = '/v2/sp/targets/report'
    request_metrics = metrics['sp']['targets']
    body = {
        'metrics': ','.join(request_metrics),
        'reportDate': datetime.strftime(report_date, '%Y%m%d')
    }
    logging.info('Request report')
    async with aiohttp.ClientSession() as session:
        response = await amz.request(session=session, api_url=api_url, body=json.dumps(body))
        logging.info('Response received, await response text')
        response_text = await response.text()

    with await pool as conn:
        await conn.execute('LPUSH', 'targets:requested', response_text)


async def main(period: int):
    amz = Reports()
    pool = await redis_pool()
    date_list = [datetime.today()-timedelta(days=i) for i in range(period, 0, -1)]
    tasks = []
    for report_date in date_list:
        tasks.append(asyncio.create_task(sp_targets(amz, pool, report_date)))

    await asyncio.gather(*tasks)
    pool.close()
    await pool.wait_closed()

if __name__ == '__main__':
    asyncio.run(main(1))
