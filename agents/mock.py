"""Deterministic offline storytelling rules."""
import re

from app.narrative import fact_is_new
from app.schemas import AgentRequest

PATHS = {
    'tunnel': ('Footprints in the tunnel lead to a hidden drain.',
               'Alex marks a return route before going below.', 'the tunnel footprints'),
    'boathouse': ('A dry oar in the boathouse points towards a concealed landing.',
                  'Alex asks the boatkeeper for help and offers to share the discovery.', 'the dry oar'),
    'archive': ('An old flood diagram reveals a blue lantern symbol.',
                'Alex checks the diagram against a second record before trusting it.', 'the blue lantern symbol'),
}

def generate(role: str, req: AgentRequest) -> dict:
    state = req.state
    chapter = req.version + 1
    hero = state.get('hero', 'Alex')
    facts = state.get('facts', [])

    if role == 'plot':
        requested_paths = re.findall(r'\b(?:archive|tunnel|boathouse)\b', req.instruction.lower())
        available = [path for path, details in PATHS.items() if fact_is_new(details[0], facts)]
        path = next((name for name in requested_paths if name in available), None)
        path = path or next(iter(available), 'resolve')
        if path == 'resolve':
            learned = facts[0] if facts else 'The search revealed a route worth marking.'
            fact = f'The group recorded its findings after chapter {chapter}.'
            beat = f'{hero} returns to the village and connects an earlier discovery to the map. {learned} {fact}'
            clue = 'the route marked on the map'
        else:
            fact, _, clue = PATHS[path]
            beat = f'{hero} follows the map towards the {path}. {fact}'
        return {'beat': beat, 'new_fact': fact, 'path': path, 'clue': clue, 'chapter_number': chapter}

    if role == 'character':
        cautious = 'careful' in req.instruction.lower() or 'quiet' in req.instruction.lower()
        caution = f'{hero} puts safety first.' if cautious else f'{hero} feels ready to act.'
        motivation = f'{hero} wants to protect the people who helped recover the map.'
        return {'development': f'{caution} {motivation}', 'hero': hero, 'cautious': cautious}

    plot = req.contributions.get('plot', {})
    character = req.contributions.get('character', {})
    if role == 'dialogue':
        clue = plot.get('clue', 'the map')
        reply = 'We should check it together before acting.' if character.get('cautious') else 'I will help you follow it.'
        return {'lines': [f'{hero}: Did you see {clue}?', f'Companion: {reply}']}

    path = plot.get('path')
    development = character.get('development', '')
    if path in PATHS:
        development += ' ' + PATHS[path][1].replace('Alex', hero)
    lines = req.contributions.get('dialogue', {}).get('lines', [])
    return {
        'text': '\n\n'.join([plot.get('beat', ''), development.strip(), '\n'.join(lines)]),
        'checks': {
            'chapter_number_matches': plot.get('chapter_number') == chapter,
            'hero_preserved': character.get('hero') == hero,
            'clue_is_new': bool(plot.get('new_fact')) and plot['new_fact'] not in facts,
            'dialogue_uses_current_clue': len(lines) >= 2 and bool(plot.get('clue'))
            and any(plot['clue'] in line for line in lines),
        },
    }
