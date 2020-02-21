import json
import aiohttp
import logging
from settings import metrics
from datetime import datetime
from amazon_advertise import AmazonAdvertise


async def sp_targets(amz: AmazonAdvertise(), report_date):
    api_url = '/v2/sp/targets/report'
    request_metrics = metrics['sp']['targets']
    body = {
        'metrics': ','.join(request_metrics),
        'reportDate': datetime.strftime(report_date, '%Y%m%d')
    }
    logging.info('Request report')
    async with aiohttp.ClientSession() as session:
        response = await amz.make_request(session=session, method='POST', api_url=api_url, body=json.dumps(body))
        logging.info('Response received, await response text')
        return await response.text()
