import celery
import redis

from devourer import config


class DevourerBaseTask(celery.Task):
    _redis_pool = None

    @property
    def redis(self):
        if not self._redis_pool:
            self._redis_pool = redis.ConnectionPool(
                host=config.REDIS_HOST,
                port=config.REDIS_PORT,
                db=config.REDIS_DB
            )

        return redis.Redis(connection_pool=self._redis_pool)
