FROM python:3.8-slim

WORKDIR /devourer

COPY devourer/ devourer/
COPY requirements requirements/

# RUN apk add --no-cache binutils libc-dev libstdc++ postgresql-libs
# RUN apk add --no-cache --virtual .build-deps gcc libffi-dev postgresql-dev build-base
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*
RUN pip install -r requirements/common.txt
RUN apt-get purge -y --auto-remove build-essential
# RUN apk del .build-deps

EXPOSE 80

CMD gunicorn devourer.main:get_application --bind 0.0.0.0:80 --worker-class aiohttp.GunicornWebWorker --timeout 3600
