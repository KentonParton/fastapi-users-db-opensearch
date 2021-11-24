"""FastAPI Users database adapter for OpenSearch."""
from typing import Optional, Type, Mapping

import opensearchpy.exceptions
from opensearchpy import AsyncOpenSearch
from opensearchpy.helpers import async_bulk
from pydantic import UUID4

from fastapi_users.db.base import BaseUserDatabase
from fastapi_users.models import UD


__version__ = "0.0.0"


class OpenSearchUserDatabase(BaseUserDatabase[UD]):
    """
    Database adapter for OpenSearch.

    :param user_db_model: Pydantic model of a DB representation of a user.
    """

    def __init__(
        self,
        user_db_model: Type[UD],
        client: AsyncOpenSearch,
    ):
        super().__init__(user_db_model)
        self.client = client
        self.user_index = "user"
        self.oauth_account_index = "oauth_account"

    async def get(self, id: UUID4) -> Optional[UD]:
        """Get a single user by id."""
        try:
            response = await self.client.get(index=self.user_index, id=id)
        except opensearchpy.exceptions.NotFoundError:
            return None
        user = response.get("_source")
        user["id"] = id
        return await self._make_user(user)

    async def get_by_email(self, email: str) -> Optional[UD]:
        """Get a single user by email."""
        response = await self.client.search(
            index=self.user_index,
            body={"query": {"match": {"email.keyword": email.lower()}}}
        )
        hits = response["hits"]["hits"]
        if not hits:
            return None
        user = hits[0]["_source"]
        user["id"] = hits[0]["_id"]
        return await self._make_user(user)

    async def get_by_oauth_account(self, oauth: str, account_id: str) -> Optional[UD]:
        """Get a single user by OAuth account id."""
        response = await self.client.search(
            index=self.oauth_account_index,
            body={
                "query": {
                    "bool": {
                        "must": [
                            {"match": {'oauth_name.keyword': oauth}},
                            {"match": {'account_id.keyword': account_id}},
                        ],
                    }
                }
            }
        )
        hits = response["hits"]["hits"]
        if not hits:
            return None
        user_id = hits[0]["_source"]["user_id"]
        return await self.get(user_id)

    async def create(self, user: UD) -> UD:
        """Create a user."""
        user_dict = user.dict()

        oauth_accounts_values = None

        if "oauth_accounts" in user_dict:
            oauth_accounts_values = []

            oauth_accounts = user_dict.pop("oauth_accounts")
            for oauth_account in oauth_accounts:
                oauth_account_id = str(oauth_account.pop("id"))
                oauth_accounts_values.append({
                    "_id": oauth_account_id,
                    "_index": self.oauth_account_index,
                    "user_id": user.id,
                    **oauth_account
                })

        if await self.get_by_email(user.email.lower()):
            raise Exception

        user_dict["email"] = user_dict["email"].lower()
        user_id = user_dict.pop("id")
        await self.client.index(
            index=self.user_index,
            id=user_id,
            body=user_dict,
            refresh="wait_for",
        )

        if oauth_accounts_values is not None:
            await async_bulk(
                self.client,
                oauth_accounts_values,
                index=self.oauth_account_index,
                refresh="wait_for",
            )

        return user

    async def update(self, user: UD) -> UD:
        """Update a user."""
        user_dict = user.dict()

        if "oauth_accounts" in user_dict:
            await self.client.delete_by_query(
                index=self.oauth_account_index,
                body={"query": {"match": {"user_id.keyword": user.id}}}
            )

            oauth_accounts_values = []
            oauth_accounts = user_dict.pop("oauth_accounts")
            for oauth_account in oauth_accounts:
                oauth_account_id = str(oauth_account.pop("id"))
                oauth_accounts_values.append({
                    "_id": oauth_account_id,
                    "_index": self.oauth_account_index,
                    "user_id": user.id,
                    **oauth_account
                })

            await async_bulk(
                self.client,
                oauth_accounts_values,
                index=self.oauth_account_index,
                refresh="wait_for",
            )

        user_id = user_dict.pop("id")
        await self.client.update(
            index=self.user_index,
            id=user_id,
            body={"doc": user_dict}
        )
        return user

    async def delete(self, user: UD) -> None:
        """Delete a user."""
        await self.client.delete(index=self.user_index, id=user.id)

    async def _make_user(self, user: Mapping) -> UD:
        user_dict = {**user}

        response = await self.client.search(
            index=self.oauth_account_index,
            body={
                "query": {
                    "match": {'user_id.keyword': user["id"]}
                },
            }
        )
        oauth_accounts = response["hits"]["hits"]

        if oauth_accounts:
            user_dict["oauth_accounts"] = [{"id": a["_id"], **a["_source"]} for a in oauth_accounts]

        return self.user_db_model(**user_dict)

