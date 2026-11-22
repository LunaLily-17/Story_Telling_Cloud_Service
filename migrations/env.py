from alembic import context
from sqlalchemy import text

from app.db import engine
from app.models import Base


def migrate(connection):
    context.configure(connection=connection, target_metadata=Base.metadata)
    with context.begin_transaction():
        # Serialize migration runners, including concurrent first installs.
        connection.execute(text("SELECT pg_advisory_xact_lock(7219)"))
        context.run_migrations()


if context.is_offline_mode():
    raise RuntimeError("Migrations require a connection to inspect legacy tables")
elif context.config.attributes.get("connection") is not None:
    migrate(context.config.attributes["connection"])
else:
    with engine.connect() as connection:
        migrate(connection)
