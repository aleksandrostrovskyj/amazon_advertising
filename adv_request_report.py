import json
import aiohttp
import asyncio
import logging
from settings import metrics
from datetime import datetime, date, timedelta
# User modules
from amazon_advertise import Reports


async def sp_targets(report_date: date):
    amz = Reports()
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
        return


async def main(period: int):
    date_list = [datetime.today()-timedelta(days=i) for i in range(period+1, 0, -1)]
    tasks = []
    for report_date in date_list:
        tasks.append(asyncio.create_task(sp_targets(report_date)))

    reports_data = await asyncio.gather(*tasks)
    print(reports_data)
