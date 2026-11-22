from app.schemas import AgentRequest
from agents.mock import generate

def test_agent_chain_and_coherence():
    req = AgentRequest(story_id='s', version=0, state={'hero':'Alex', 'setting':'Leicester'}, instruction='continue')
    plot = generate('plot', req)
    character = generate('character', req)
    dialogue = generate('dialogue', req.model_copy(update={'contributions': {
        'plot': plot, 'character': character}}))
    refined = generate('refinement', req.model_copy(update={'contributions': {
        'plot': plot, 'character': character, 'dialogue': dialogue}}))
    assert all(refined['checks'].values())
    assert 'Alex' in refined['text']
    assert plot['chapter_number'] == 1

def test_refinement_rejects_inconsistent_chapter():
    req = AgentRequest(story_id='s', version=4, state={'hero':'Alex'}, instruction='continue',
                       contributions={'plot': {'beat':'bad', 'chapter_number':2},
                                      'character': {'hero':'Alex'}, 'dialogue': {'lines':['a','b']}})
    assert not generate('refinement', req)['checks']['chapter_number_matches']

def test_plot_uses_instruction_and_does_not_repeat_a_clue():
    req = AgentRequest(story_id='s', version=0, state={'hero': 'Alex'}, instruction='Search the tunnel carefully')
    first = generate('plot', req)
    assert first['path'] == 'tunnel'
    req = req.model_copy(update={'version': 1, 'state': {'facts': [first['new_fact']]}})
    assert generate('plot', req)['path'] != 'tunnel'
