from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase

MODEL_SCHEMAS = (
    "core",
    "iam",
    "authz",
    "moderation",
    "content",
    "scoring",
    "social",
    "community",
    "multiplayer",
    "service",
    "events",
    "audit",
    "system",
)

NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_name)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class DbBase(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)
