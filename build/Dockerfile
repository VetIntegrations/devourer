FROM python:3.8-alpine

WORKDIR /devourer

COPY devourer/ devourer/
COPY requirements requirements/

RUN apk add --no-cache binutils libc-dev libstdc++ postgresql-libs
RUN apk add --no-cache --virtual .build-deps gcc libffi-dev postgresql-dev build-base
RUN pip install -r requirements/common.txt
RUN apk del .build-deps

EXPOSE 80

CMD gunicorn devourer.main:get_application --bind 0.0.0.0:80 --worker-class aiohttp.GunicornWebWorker
