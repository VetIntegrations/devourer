import celery
import redis

from devourer import config
from devourer.datasources.hubspot.tasks import WaitGroup


class DevourerBaseTask(celery.Task):
    _redis_pool = None

    def _process_waitgroup_failure(self, kwargs):
        _, current_waitgroup_key = kwargs['waitgroup_keys']
        wg = WaitGroup(current_waitgroup_key, self.redis)
        wg.stop()

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        waitgroup = getattr(self, '__waitgroup__', False)
        if waitgroup:
            self._process_waitgroup_failure(kwargs)

    @property
    def redis(self):
        if not self._redis_pool:
            self._redis_pool = redis.ConnectionPool(
                host=config.REDIS_HOST,
                port=config.REDIS_PORT,
                db=config.REDIS_DB
            )

        return redis.Redis(connection_pool=self._redis_pool)
