OPENSEARCH_CONTAINER_NAME := fastapi-users-db-openserchdb-test-opensearch

isort:
	isort ./fastapi_users_db_opensearch ./tests

format: isort
	black .

test:
	docker stop $(OPENSEARCH_CONTAINER_NAME) || true
	docker run -d --rm --name $(OPENSEARCH_CONTAINER_NAME) -p 9200:9200 -p 9600:9600 -e "discovery.type=single-node" opensearchproject/opensearch:2.3.0
	pytest --cov=fastapi_users_db_opensearch/
	docker stop $(OPENSEARCH_CONTAINER_NAME)

bumpversion-major:
	bumpversion major

bumpversion-minor:
	bumpversion minor

bumpversion-patch:
	bumpversion patch
