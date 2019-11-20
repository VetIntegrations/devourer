from envparse import env


env.read_envfile("environment")

DEBUG = env.bool("DEBUG", default=False)

REDIS_HOST = env.str("REDIS_HOST", default="127.0.0.1")
REDIS_PORT = env.int("REDIS_PORT", default=6379)
REDIS_DB = env.int("REDIS_DB", default=1)
