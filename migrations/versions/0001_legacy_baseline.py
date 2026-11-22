"""Create missing legacy tables and verify the existing schema.

Revision ID: 0001_legacy_baseline
"""
from alembic import op
import sqlalchemy as sa

revision = "0001_legacy_baseline"
down_revision = None

TABLES = {
    "stories": {"id", "title", "genre", "premise", "version", "state", "created_at"},
    "chapters": {"id", "story_id", "version", "text", "contributions"},
    "generation_jobs": {
        "id", "story_id", "idempotency_key", "instruction", "expected_version",
        "status", "attempts", "locked_until", "result", "error", "created_at",
    },
}


def upgrade():
    inspector = sa.inspect(op.get_bind())
    existing = set(inspector.get_table_names())
    for table, required in TABLES.items():
        if table in existing:
            columns = {column["name"] for column in inspector.get_columns(table)}
            if missing := required - columns:
                raise RuntimeError(f"{table} is missing legacy columns: {sorted(missing)}")
            if inspector.get_pk_constraint(table)["constrained_columns"] != ["id"]:
                raise RuntimeError(f"{table} has an unexpected primary key")

    if "stories" not in existing:
        op.create_table(
            "stories",
            sa.Column("id", sa.String(36), primary_key=True),
            sa.Column("title", sa.String(200), nullable=False),
            sa.Column("genre", sa.String(80), nullable=False),
            sa.Column("premise", sa.String(1000), nullable=False),
            sa.Column("version", sa.Integer(), nullable=False),
            sa.Column("state", sa.JSON(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        )
    if "chapters" not in existing:
        op.create_table(
            "chapters",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("story_id", sa.String(36), sa.ForeignKey("stories.id"), nullable=False),
            sa.Column("version", sa.Integer(), nullable=False),
            sa.Column("text", sa.String(20000), nullable=False),
            sa.Column("contributions", sa.JSON(), nullable=False),
        )
    if "generation_jobs" not in existing:
        op.create_table(
            "generation_jobs",
            sa.Column("id", sa.String(36), primary_key=True),
            sa.Column("story_id", sa.String(36), sa.ForeignKey("stories.id"), nullable=False),
            sa.Column("idempotency_key", sa.String(120), nullable=False),
            sa.Column("instruction", sa.String(1000), nullable=False),
            sa.Column("expected_version", sa.Integer()),
            sa.Column("status", sa.String(20), nullable=False),
            sa.Column("attempts", sa.Integer(), nullable=False),
            sa.Column("locked_until", sa.DateTime(timezone=True)),
            sa.Column("result", sa.JSON()),
            sa.Column("error", sa.String(500)),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        )
        op.create_index("ix_generation_jobs_story_id", "generation_jobs", ["story_id"])
        op.create_index("ix_generation_jobs_status", "generation_jobs", ["status"])


def downgrade():
    raise RuntimeError("Downgrading the baseline can discard existing story data")
