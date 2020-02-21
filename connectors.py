import aioredis
from settings import config


async def redis_pool():
    return await aioredis.create_pool(
        (config['redis']['host'], config['redis']['port'])
    )
