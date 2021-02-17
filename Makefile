include *.mk

VERSION := $(shell date +%Y%m%d%H%M)

run:
	gunicorn devourer.main:get_application --bind localhost:8000 --worker-class aiohttp.GunicornWebWorker --timeout 3600 --reload

worker:
	celery -A devourer worker --loglevel=DEBUG -c 2 --beat --purge

test:
	python -m pytest \
		--pylama \
		--bandit \
		--cov=devourer \
		-vv --showlocals \
		--ignore=./tasks \
		devourer/${TEST}

coverage:
	python -m pytest --pylama --cov=. --cov-report term --cov-report html:../tests_artifacts/cov_html --ignore=./tasks .

deps-compile:
	for name in common ci dev; do \
		pip-compile --no-emit-index-url requirements/$$name.in; \
	done

build-image-api:
	docker build \
		-t devourer-api \
		-f build/Dockerfile.api .
	docker tag devourer-api "gcr.io/${GCP_PROJECT_ID}/devourer-api:v0.0.${VERSION}"
	docker tag devourer-api "gcr.io/${GCP_PROJECT_ID}/devourer-api:latest"
	docker push "gcr.io/${GCP_PROJECT_ID}/devourer-api:v0.0.${VERSION}"
	docker push "gcr.io/${GCP_PROJECT_ID}/devourer-api:latest"

build-image-celery:
	docker build \
		-t devourer-celery \
		-f build/Dockerfile.celery .
	docker tag devourer-celery "gcr.io/${GCP_PROJECT_ID}/devourer-celery:v0.0.${VERSION}"
	docker tag devourer-celery "gcr.io/${GCP_PROJECT_ID}/devourer-celery:latest"
	docker push "gcr.io/${GCP_PROJECT_ID}/devourer-celery:v0.0.${VERSION}"
	docker push "gcr.io/${GCP_PROJECT_ID}/devourer-celery:latest"

build-image-celery-beat:
	docker build \
		-t devourer-celery-beat \
		-f build/Dockerfile.celery-beat .
	docker tag devourer-celery-beat "gcr.io/${GCP_PROJECT_ID}/devourer-celery-beat:v0.0.${VERSION}"
	docker tag devourer-celery-beat "gcr.io/${GCP_PROJECT_ID}/devourer-celery-beat:latest"
	docker push "gcr.io/${GCP_PROJECT_ID}/devourer-celery-beat:v0.0.${VERSION}"
	docker push "gcr.io/${GCP_PROJECT_ID}/devourer-celery-beat:latest"


build-images: build-image-api build-image-celery build-image-celery-beat
