"""Enforce uniqueness and add worker claim metadata.

Revision ID: 0002_fenced_jobs
"""
from alembic import op
import sqlalchemy as sa

revision = "0002_fenced_jobs"
down_revision = "0001_legacy_baseline"


def ensure_unique(table, columns, name):
    inspector = sa.inspect(op.get_bind())
    existing_columns = {column["name"]: column for column in inspector.get_columns(table)}
    if any(existing_columns[column]["nullable"] for column in columns):
        raise RuntimeError(f"{table} has nullable uniqueness columns; inspect the legacy schema")
    constraints = inspector.get_unique_constraints(table)
    if not any(set(item["column_names"]) == set(columns) for item in constraints):
        # PostgreSQL rejects duplicates and rolls back; existing records stay intact.
        op.create_unique_constraint(name, table, columns)


def upgrade():
    ensure_unique("chapters", ["story_id", "version"], "chapters_story_id_version_key")
    ensure_unique(
        "generation_jobs", ["story_id", "idempotency_key"],
        "generation_jobs_story_id_idempotency_key_key",
    )
    # Historical update and chapter creation times are unknown.
    for table, column in (("stories", "updated_at"), ("chapters", "created_at")):
        op.add_column(table, sa.Column(column, sa.DateTime(timezone=True)))
        op.alter_column(table, column, server_default=sa.func.now())
    op.add_column("generation_jobs", sa.Column("claim_token", sa.String(36)))
    op.add_column("generation_jobs", sa.Column("base_version", sa.Integer()))
    op.add_column("generation_jobs", sa.Column("available_at", sa.DateTime(timezone=True)))
    op.add_column("generation_jobs", sa.Column("updated_at", sa.DateTime(timezone=True)))
    op.alter_column("generation_jobs", "updated_at", server_default=sa.func.now())
    op.create_index(
        "ix_generation_jobs_claim", "generation_jobs",
        ["status", "available_at", "locked_until", "created_at"],
    )


def downgrade():
    raise RuntimeError("Downgrading would discard job ownership and retry metadata")
