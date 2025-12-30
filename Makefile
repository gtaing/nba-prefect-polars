PYTHON_PATH=.venv/bin/python

.PHONY: tests
tests:
	$(PYTHON_PATH) -m pytest -v tests/


.PHONY: clean-target
clean-target:
	@echo "Cleaning the /target folder..."
	@rm -rf target/*

.PHONY: check
check:
	@echo "Checking with Ruff..."
	$(PYTHON_PATH) -m ruff check

.PHONY: fix-format
fix-format:
	@echo "Fixing format issues with Ruff..."
	$(PYTHON_PATH) -m ruff check --fix

.PHONY: format
format:
	@echo "Formatting with Ruff..."
	$(PYTHON_PATH) -m ruff format .
