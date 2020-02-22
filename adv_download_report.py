import io
import gzip
import json
import aiohttp
import asyncio
import logging
# User modules
from amazon_advertise import Reports
from connectors import redis_connections_pool, db_connections_pool, update_db

# TODO - collect data from the queue in a bundle and implement asyncio.gather()?
# TODO - Possible negative impact: high probability of receiving too many requests response, check later

DB_TABLES = {
    'targets': 'us_advertising_targets_physicans'
}


def unpack_giz(content):
    compressed_file = io.BytesIO(content)
    with gzip.open(compressed_file) as gzip_stream:
        data = gzip_stream.read()
    return json.loads(data)


async def main():
    amz = Reports()
    redis_pool = await redis_connections_pool()
    db_pool = await db_connections_pool()

    while True:
        with await redis_pool as redis_conn:
            data = await redis_conn.execute('LPOP', 'targets:ready')

            if not data:
                logging.info('targets:ready is empty. Exit')
                break

        data_json = json.loads(data)
        report_id = data_json['reportId']
        report_type = data_json['recordType']
        report_date = data_json['report_date']

        async with aiohttp.ClientSession() as session:
            response = await amz.download(session=session, report_id=report_id)
            response_content = await response.read()

        report = unpack_giz(response_content)
        # Add report date to each row in received data
        report = [{**each, **{'report_date': report_date}} for each in report]

        async with db_pool.acquire() as conn:
            logging.info(f'Update database {DB_TABLES[report_type]} for {report_date}')
            await update_db(conn, table=DB_TABLES[report_type], data=report)

        logging.info('Get Error, RPUSH data back')
        await redis_conn.execute('RPUSH', 'targets:requested', data)

    redis_pool.close()
    await redis_pool.wait_closed()

if __name__ == '__main__':
    asyncio.run(main())
