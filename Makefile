run:
	gunicorn devourer.main:get_application --bind localhost:8000 --worker-class aiohttp.GunicornWebWorker --timeout 3600 --reload

test:
	python -m pytest --pylama -vv --showlocals --ignore=./tasks .

coverage:
	python -m pytest --pylama --cov=. --cov-report term --cov-report html:../tests_artifacts/cov_html --ignore=./tasks .

deps-compile:
	for name in common ci dev; do \
		pip-compile requirements/$$name.in; \
	done
