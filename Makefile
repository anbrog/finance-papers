.PHONY: install clean

install: clean
	pip install -e .
	@$(MAKE) clean

clean:
	@rm -rf *.egg-info build dist __pycache__ .pytest_cache 2>/dev/null || true
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
