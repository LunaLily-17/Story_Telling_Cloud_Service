from sqlalchemy import Column, String, Integer, JSON, DateTime, ForeignKey, UniqueConstraint, Index, func
from sqlalchemy.orm import declarative_base

CHAPTER_VERSION_CONSTRAINT = "chapters_story_id_version_key"
JOB_IDEMPOTENCY_CONSTRAINT = "generation_jobs_story_id_idempotency_key_key"

Base = declarative_base()

class Story(Base):
    __tablename__ = 'stories'
    id = Column(String(36), primary_key=True)
    title = Column(String(200), nullable=False)
    genre = Column(String(80), nullable=False)
    premise = Column(String(1000), nullable=False)
    version = Column(Integer, nullable=False, default=0)
    state = Column(JSON, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now())

class Chapter(Base):
    __tablename__ = 'chapters'
    id = Column(Integer, primary_key=True, autoincrement=True)
    story_id = Column(String(36), ForeignKey('stories.id'), nullable=False)
    version = Column(Integer, nullable=False)
    text = Column(String(20000), nullable=False)
    contributions = Column(JSON, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    __table_args__ = (UniqueConstraint('story_id', 'version', name=CHAPTER_VERSION_CONSTRAINT),)


class GenerationJob(Base):
    __tablename__ = 'generation_jobs'
    id = Column(String(36), primary_key=True)
    story_id = Column(String(36), ForeignKey('stories.id'), nullable=False, index=True)
    idempotency_key = Column(String(120), nullable=False)
    instruction = Column(String(1000), nullable=False)
    expected_version = Column(Integer, nullable=True)
    status = Column(String(20), nullable=False, default='queued', index=True)
    attempts = Column(Integer, nullable=False, default=0)
    claim_token = Column(String(36), nullable=True)
    base_version = Column(Integer, nullable=True)
    available_at = Column(DateTime(timezone=True), nullable=True)
    locked_until = Column(DateTime(timezone=True), nullable=True)
    result = Column(JSON, nullable=True)
    error = Column(String(500), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now())
    __table_args__ = (
        UniqueConstraint('story_id', 'idempotency_key', name=JOB_IDEMPOTENCY_CONSTRAINT),
        Index('ix_generation_jobs_claim', 'status', 'available_at', 'locked_until', 'created_at'),
    )
