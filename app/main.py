from pathlib import Path
from uuid import uuid4
from fastapi import FastAPI, Depends, HTTPException, Header, Response
from fastapi.responses import FileResponse
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from app.db import SessionLocal
from app.models import Story, Chapter, GenerationJob
from app.schemas import NewStory, ContinueStory

app = FastAPI(title='Distributed Storytelling API')


def db_session():
    with SessionLocal() as session:
        yield session

@app.get('/', include_in_schema=False)
def index():
    return FileResponse(Path(__file__).with_name('story.html'))

@app.get('/health')
def health(): return {'status':'ok'}

@app.get('/ready')
def ready(db: Session = Depends(db_session)):
    db.execute(select(Story.id).limit(1)).all()
    db.execute(select(GenerationJob.claim_token).limit(1)).all()
    return {'status':'ready'}

@app.post('/stories', status_code=201)
def create_story(body: NewStory, db: Session = Depends(db_session)):
    story_id = str(uuid4())
    state = {'hero':'Alex','setting':body.premise[:100],'chapter_count':0,
             'latest_fact':None,'facts':[],'last_chapter':None}
    db.add(Story(id=story_id,title=body.title,genre=body.genre,
                 premise=body.premise,state=state,version=0))
    db.commit()
    return {'story_id':story_id,'version':0,'state':state}

@app.get('/stories/{story_id}')
def get_story(story_id: str, db: Session = Depends(db_session)):
    story = db.get(Story,story_id)
    if story is None: raise HTTPException(404,'Story not found')
    chapters = db.execute(select(Chapter).where(Chapter.story_id==story_id,Chapter.version<=story.version)
                          .order_by(Chapter.version)).scalars().all()
    return {'story_id':story.id,'title':story.title,'genre':story.genre,
            'premise':story.premise,'version':story.version,'state':story.state,
            'chapters':[{'version':c.version,'text':c.text} for c in chapters]}

@app.post('/stories/{story_id}/continue', status_code=202)
def advance(story_id: str, body: ContinueStory, db: Session=Depends(db_session),
            idempotency_key: str|None=Header(default=None,alias='Idempotency-Key')):
    story = db.get(Story, story_id)
    if story is None:
        raise HTTPException(404, 'Story not found')
    key = str(uuid4()) if idempotency_key is None else idempotency_key
    if not key.strip() or len(key) > 120 or '\x00' in key:
        raise HTTPException(422, 'Idempotency-Key must be nonblank and at most 120 characters')
    base_version = body.expected_version if body.expected_version is not None else story.version
    created_id = db.scalar(insert(GenerationJob).values(
        id=str(uuid4()), story_id=story_id, idempotency_key=key,
        instruction=body.instruction, expected_version=body.expected_version,
        base_version=base_version, status='queued',
    ).on_conflict_do_nothing(
        index_elements=[GenerationJob.story_id, GenerationJob.idempotency_key],
    ).returning(GenerationJob.id))
    if created_id is not None:
        db.commit()
        return {'job_id': created_id, 'status': 'queued'}
    previous = db.execute(select(GenerationJob).where(
        GenerationJob.story_id == story_id, GenerationJob.idempotency_key == key,
    )).scalar_one()
    if previous.instruction != body.instruction or previous.expected_version != body.expected_version:
        raise HTTPException(409, 'Idempotency key reused with different request')
    return {'job_id': previous.id, 'status': previous.status}


@app.get('/jobs/{job_id}')
def get_job(job_id: str, response: Response, db: Session = Depends(db_session)):
    job=db.get(GenerationJob,job_id)
    if job is None: raise HTTPException(404,'Job not found')
    response.headers['Cache-Control'] = 'no-store'
    return {'job_id':job.id,'story_id':job.story_id,'status':job.status,
            'attempts':job.attempts,'error':job.error,'result':job.result}
