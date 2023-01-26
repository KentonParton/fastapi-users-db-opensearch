import time
from typing import AsyncGenerator

import pytest
from opensearchpy import AsyncOpenSearch

from fastapi_users_db_opensearch import OpenSearchUserDatabase
from tests.conftest import UserDB, UserDBOAuth


@pytest.fixture(scope="module")
async def opensearchdb_client():
    client = AsyncOpenSearch(
        hosts=[{"host": "localhost", "port": "9200"}],
        http_auth=("admin", "admin"),
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
    )
    wait_period = time.time() + 60

    while time.time() < wait_period:
        try:
            await client.cluster.health(wait_for_status="yellow", request_timeout=10)
            yield client
        except Exception:
            continue


async def create_indices(opensearchdb_client: AsyncOpenSearch):
    user_index = "user"

    if not await opensearchdb_client.indices.exists(user_index):
        await opensearchdb_client.indices.create(
            index=user_index,
            body={"mappings": {"properties": {"oauth_accounts": {"type": "nested"}}}},
        )


async def delete_indices(opensearchdb_client: AsyncOpenSearch):
    user_index = "user"

    if await opensearchdb_client.indices.exists(user_index):
        await opensearchdb_client.indices.delete(user_index)


@pytest.fixture
async def opensearch_user_db(
    opensearchdb_client: AsyncOpenSearch,
) -> AsyncGenerator[OpenSearchUserDatabase, None]:
    await create_indices(opensearchdb_client)
    yield OpenSearchUserDatabase(UserDB, opensearchdb_client)
    await delete_indices(opensearchdb_client)


@pytest.fixture
async def opensearch_user_db_oauth(
    opensearchdb_client: AsyncOpenSearch,
) -> AsyncGenerator[OpenSearchUserDatabase, None]:
    await create_indices(opensearchdb_client)
    yield OpenSearchUserDatabase(UserDBOAuth, opensearchdb_client)
    await delete_indices(opensearchdb_client)
    await opensearchdb_client.close()


@pytest.mark.asyncio
@pytest.mark.db
async def test_queries(opensearch_user_db: OpenSearchUserDatabase[UserDB]):
    user = UserDB(
        email="lancelot@camelot.bt",
        hashed_password="guinevere",
    )

    # Create
    user_db = await opensearch_user_db.create(user)
    assert user_db.id is not None
    assert user_db.is_active is True
    assert user_db.is_superuser is False
    assert user_db.email == user.email

    # Update
    user_db.is_superuser = True
    await opensearch_user_db.update(user_db)

    # Get by id
    id_user = await opensearch_user_db.get(user.id)
    assert id_user is not None
    assert id_user.id == user_db.id
    assert id_user.is_superuser is True

    # Get by email
    email_user = await opensearch_user_db.get_by_email(str(user.email))
    assert email_user is not None
    assert email_user.id == user_db.id

    # Get by uppercased email
    email_user = await opensearch_user_db.get_by_email("Lancelot@camelot.bt")
    assert email_user is not None
    assert email_user.id == user_db.id

    # Exception when inserting existing email
    with pytest.raises(Exception):
        await opensearch_user_db.create(user)

    # Exception when inserting non-nullable fields
    with pytest.raises(Exception):
        # Use construct to bypass Pydantic validation
        wrong_user = UserDB.construct(hashed_password="aaa")
        await opensearch_user_db.create(wrong_user)

    # Unknown user
    unknown_user = await opensearch_user_db.get_by_email("galahad@camelot.bt")
    assert unknown_user is None

    # Delete user
    await opensearch_user_db.delete(user)
    deleted_user = await opensearch_user_db.get(user.id)
    assert deleted_user is None


@pytest.mark.asyncio
@pytest.mark.db
async def test_queries_custom_fields(
    opensearch_user_db: OpenSearchUserDatabase[UserDB],
):
    """It should output custom fields in query result."""
    user = UserDB(
        email="lancelot@camelot.bt",
        hashed_password="guinevere",
        first_name="Lancelot",
    )
    await opensearch_user_db.create(user)

    id_user = await opensearch_user_db.get(user.id)
    assert id_user is not None
    assert id_user.id == user.id
    assert id_user.first_name == user.first_name


@pytest.mark.asyncio
@pytest.mark.db
async def test_queries_oauth(
    opensearch_user_db_oauth: OpenSearchUserDatabase[UserDBOAuth],
    oauth_account1,
    oauth_account2,
):
    user = UserDBOAuth(
        email="lancelot@camelot.bt",
        hashed_password="guinevere",
        oauth_accounts=[oauth_account1, oauth_account2],
    )

    # Create
    user_db = await opensearch_user_db_oauth.create(user)
    assert user_db.id is not None
    assert hasattr(user_db, "oauth_accounts")
    assert len(user_db.oauth_accounts) == 2

    # Update
    user_db.oauth_accounts[0].access_token = "NEW_TOKEN"
    await opensearch_user_db_oauth.update(user_db)

    # Get by id
    id_user = await opensearch_user_db_oauth.get(user.id)
    assert id_user is not None
    assert id_user.id == user_db.id
    assert id_user.oauth_accounts[0].access_token == "NEW_TOKEN"

    # Get by email
    email_user = await opensearch_user_db_oauth.get_by_email(str(user.email))
    assert email_user is not None
    assert email_user.id == user_db.id
    assert len(email_user.oauth_accounts) == 2

    # Get by OAuth account
    oauth_user = await opensearch_user_db_oauth.get_by_oauth_account(
        oauth_account1.oauth_name, oauth_account1.account_id
    )
    assert oauth_user is not None
    assert oauth_user.id == user.id

    # Unknown OAuth account
    unknown_oauth_user = await opensearch_user_db_oauth.get_by_oauth_account(
        "foo", "bar"
    )
    assert unknown_oauth_user is None
