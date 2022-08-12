env:
	poetry install

format:
	poetry run black .
	poetry run isort .
