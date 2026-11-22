from typing import Annotated

from pydantic import BaseModel, Field, RootModel, StringConstraints
from typing_extensions import NotRequired, TypedDict

NonBlank = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=2000)]

class NewStory(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    genre: str = Field(default='adventure', max_length=80)
    premise: str = Field(min_length=3, max_length=1000)

class ContinueStory(BaseModel):
    instruction: str = Field(default='Continue the narrative coherently', max_length=1000)
    expected_version: int | None = Field(default=None, ge=0, le=2_147_483_646)

class AgentRequest(BaseModel):
    story_id: str = Field(min_length=1, max_length=36, pattern=r"^[A-Za-z0-9_-]+$")
    version: int = Field(ge=0, le=2_147_483_646)
    state: dict
    instruction: str = Field(max_length=1000)
    contributions: dict = Field(default_factory=dict)

class AgentResponse(BaseModel):
    agent: str
    story_id: str
    base_version: int
    contribution: dict


class PlotContribution(TypedDict):
    beat: NonBlank
    new_fact: NonBlank
    path: NonBlank
    clue: NonBlank
    chapter_number: Annotated[int, Field(ge=1)]


class CharacterContribution(TypedDict):
    development: NonBlank
    hero: NonBlank
    cautious: bool


class DialogueContribution(TypedDict):
    lines: Annotated[list[NonBlank], Field(min_length=2, max_length=24)]


class RefinementChecks(TypedDict):
    chapter_number_matches: bool
    hero_preserved: bool
    clue_is_new: bool
    dialogue_uses_current_clue: bool
    semantic_coherent: NotRequired[bool]


class RefinementContribution(TypedDict):
    text: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=20_000)]
    checks: RefinementChecks


CONTRIBUTION_MODELS = {
    "plot": RootModel[PlotContribution],
    "character": RootModel[CharacterContribution],
    "dialogue": RootModel[DialogueContribution],
    "refinement": RootModel[RefinementContribution],
}
