from sqlalchemy.ext.asyncio import AsyncAttributes
from sqlalchemy.orm import DeclarativeBase

class Base(AsyncAttributes, DeclarativeBase):
    pass

# TODO: User to insert schema definitions here
