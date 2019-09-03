PYTHON = python3

all: test

test:
	docker build -t wo-test .
	docker run -v ~/.aws:/root/.aws wo-test

wheel:
	$(PYTHON) setup.py bdist_wheel

check:
	twine check dist/*

upload:
	twine upload dist/*

clean: 
	rm -rf .pytest_cache .eggs wo.egg-info build dist 
