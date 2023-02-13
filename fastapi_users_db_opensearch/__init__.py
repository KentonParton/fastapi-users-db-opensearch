"""FastAPI Users database adapter for OpenSearch."""
from typing import Optional, Type

import opensearchpy.exceptions
from fastapi_users.db.base import BaseUserDatabase
from fastapi_users.models import UD
from opensearchpy import AsyncOpenSearch
from pydantic import UUID4


__version__ = "0.0.4"


class OpenSearchUserDatabase(BaseUserDatabase[UD]):
    """
    Database adapter for OpenSearch.

    :param user_db_model: Pydantic model of a DB representation of a user.
    """

    def __init__(
        self,
        user_db_model: Type[UD],
        client: AsyncOpenSearch,
        user_index: str = "user",
    ):
        super().__init__(user_db_model)
        self.client = client
        self.user_index = user_index

    async def get(self, id: UUID4) -> Optional[UD]:
        """Get a single user by id."""
        try:
            response = await self.client.get(index=self.user_index, id=id)
        except opensearchpy.exceptions.NotFoundError:
            return None
        user = response.get("_source")
        user["id"] = id
        return self.user_db_model(**user)

    async def get_by_email(self, email: str) -> Optional[UD]:
        """Get a single user by email."""
        response = await self.client.search(
            index=self.user_index,
            body={"query": {"match": {"email.keyword": email.lower()}}},
        )
        hits = response["hits"]["hits"]
        if not hits:
            return None
        user = hits[0]["_source"]
        user["id"] = hits[0]["_id"]
        return self.user_db_model(**user)

    async def get_by_oauth_account(self, oauth: str, account_id: str) -> Optional[UD]:
        """Get a single user by OAuth account id."""
        response = await self.client.search(
            index=self.user_index,
            body={
                "query": {
                    "nested": {
                        "path": "oauth_accounts",
                        "query": {
                            "bool": {
                                "must": [
                                    {
                                        "match": {
                                            "oauth_accounts.oauth_name.keyword": oauth
                                        }
                                    },
                                    {
                                        "match": {
                                            "oauth_accounts.account_id.keyword": account_id
                                        }
                                    },
                                ]
                            }
                        },
                    }
                }
            },
        )
        hits = response["hits"]["hits"]
        if not hits:
            return None
        user = hits[0]["_source"]
        user["id"] = hits[0]["_id"]
        return self.user_db_model(**user)

    async def create(self, user: UD) -> UD:
        """Create a user."""
        user_dict = user.dict()

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

        return user

    async def update(self, user: UD) -> UD:
        """Update a user."""
        user_dict = user.dict()

        user_id = user_dict.pop("id")
        await self.client.update(
            index=self.user_index,
            id=user_id,
            body={"doc": user_dict},
            refresh="wait_for",
        )
        return user

    async def delete(self, user: UD) -> None:
        """Delete a user."""
        await self.client.delete(index=self.user_index, id=user.id, refresh="wait_for")
