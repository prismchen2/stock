clean-pyc:
	find . -name '*.pyc' -delete
test: clean-pyc
	py.test --verbose --color=yes ./tests
