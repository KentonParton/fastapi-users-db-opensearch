isort:
	isort ./fastapi_users_db_opensearch ./tests

format: isort
	black .

test:
	pytest --cov=fastapi_users_db_opensearch/ --fail-under=100

bumpversion-major:
	bumpversion major

bumpversion-minor:
	bumpversion minor

bumpversion-patch:
	bumpversion patch
