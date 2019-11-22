run:
	gunicorn devourer.main:get_application --bind localhost:8000 --worker-class aiohttp.GunicornWebWorker --reload

test:
	python -m pytest --pylama -vv --showlocals .

coverage:
	python -m pytest --pylama --cov=. --cov-report term --cov-report html:../tests_artifacts/cov_html .

deps-compile:
	for name in common ci dev; do \
		pip-compile requirements/$$name.in; \
	done
