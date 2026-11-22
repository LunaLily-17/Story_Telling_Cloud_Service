import asyncio
import os
import httpx
from fastapi import HTTPException
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from app.models import Story, Chapter
from app.schemas import AgentRequest, AgentResponse

AGENTS = {role: os.getenv(f'{role.upper()}_URL', f'http://{role}:8000')
          for role in ('plot', 'character', 'dialogue', 'refinement')}

async def ask_agent(client, role, request):
    try:
        response = await client.post(f'{AGENTS[role]}/contribute', json=request.model_dump())
        response.raise_for_status()
        result = AgentResponse.model_validate(response.json())
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(503, f'{role} agent unavailable or invalid response') from exc
    if result.agent != role or result.story_id != request.story_id or result.base_version != request.version:
        raise HTTPException(502, f'{role} agent returned inconsistent metadata')
    return result.contribution

async def continue_story(session, story_id, instruction, expected_version):
    """Generate against a snapshot and commit only if its version is still current."""
    story = session.get(Story, story_id)
    if story is None:
        raise HTTPException(404, 'Story not found')
    if expected_version is not None and expected_version != story.version:
        raise HTTPException(409, {'error': 'version_conflict', 'current_version': story.version})
    req = AgentRequest(story_id=story.id, version=story.version, state=story.state,
                       instruction=instruction)
    session.rollback()
    async with httpx.AsyncClient(timeout=15.0) as client:
        # Plot and character work in parallel; dialogue follows their shared context.
        plot, character = await asyncio.gather(ask_agent(client, 'plot', req),
                                                ask_agent(client, 'character', req))
        contributions = {'plot': plot, 'character': character}
        dialogue = await ask_agent(client, 'dialogue', req.model_copy(update={'contributions': contributions}))
        contributions['dialogue'] = dialogue
        refined = await ask_agent(client, 'refinement', req.model_copy(update={'contributions': contributions}))
        contributions['refinement'] = refined
    checks = refined.get('checks', {})
    if not checks or not all(checks.values()) or not refined.get('text', '').strip():
        raise HTTPException(422, 'Narrative coherence validation failed; nothing committed')
    next_version = req.version + 1
    state = dict(req.state)
    state['chapter_count'] = next_version
    state['latest_fact'] = plot['new_fact']
    state['facts'] = [*state.get('facts', []), plot['new_fact']]
    state['last_chapter'] = refined['text']
    updated = session.execute(
        update(Story)
        .where(Story.id == story_id, Story.version == req.version)
        .values(version=next_version, state=state)
        .returning(Story.version)
    ).scalar_one_or_none()
    if updated is None:
        session.rollback()
        current = session.get(Story, story_id)
        raise HTTPException(409, {'error': 'version_conflict', 'current_version': current.version})
    chapter = Chapter(story_id=story_id, version=next_version,
                      text=refined['text'], contributions=contributions)
    session.add(chapter)
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        raise HTTPException(409, 'Concurrent version conflict')
    return {'story_id': story_id, 'version': next_version,
            'chapter': chapter.text, 'state': state, 'contributions': contributions}
