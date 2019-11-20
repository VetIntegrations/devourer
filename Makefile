run:
	gunicorn devourer.main:get_application --bind localhost:8000 --worker-class aiohttp.GunicornWebWorker --reload

test:
	python -m pytest --pylama .

deps-compile:
	for name in common dev; do \
		pip-compile requirements/$$name.in; \
	done
