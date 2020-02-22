import logging
import aiomysql
import aioredis
from settings import config


async def redis_connections_pool():
    return await aioredis.create_pool(
        (config['redis']['host'], config['redis']['port'])
    )


async def db_connections_pool():
    return await aiomysql.create_pool(**config['mysql'])


async def update_db(conn, table, data):
    data_to_insert = str([tuple(str(each) for each in row.values()) for row in data]).strip('[]')

    delete_query = f"""
            DELETE FROM {table}
            WHERE report_date = '{data[0]['report_date']}'
        """

    insert_query = f"""
            INSERT INTO {table}
            VALUES {data_to_insert}
        """
    async with conn.cursor() as cursor:
        result = await cursor.execute(delete_query)
        await conn.commit()
        logging.info(f'Table orders: {result} rows have been deleted')
        result = await cursor.execute(insert_query)
        await conn.commit()
        logging.info(f'Table orders: {result} rows have been added')
