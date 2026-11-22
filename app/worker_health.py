"""Worker event-loop liveness and database readiness probes."""
import asyncio
import os
import sys
from pathlib import Path
from time import monotonic

INTERVAL = 3
MAX_AGE = 15


async def publish_liveness(path: Path) -> None:
    temporary = path.with_suffix(".tmp")
    try:
        while True:
            temporary.write_text(str(monotonic()))
            temporary.replace(path)
            await asyncio.sleep(INTERVAL)
    finally:
        path.unlink(missing_ok=True)
        temporary.unlink(missing_ok=True)


def probe(mode: str) -> bool:
    try:
        stamp = float(Path(os.environ["WORKER_HEALTH_FILE"]).read_text())
    except (KeyError, ValueError, OSError):
        return False
    if not 0 <= monotonic() - stamp <= MAX_AGE:
        return False
    if mode == "ready":
        from sqlalchemy import text
        from sqlalchemy.exc import SQLAlchemyError
        from app.db import engine

        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT claim_token FROM generation_jobs LIMIT 0"))
        except SQLAlchemyError:
            return False
    return True


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in {"live", "ready"}:
        sys.exit("usage: python -m app.worker_health {live|ready}")
    sys.exit(0 if probe(sys.argv[1]) else 1)
