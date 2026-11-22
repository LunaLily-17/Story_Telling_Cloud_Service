"""Deterministic checks shared by the proposal boundary and the workflow."""
import re


def normalize(text: str) -> str:
    return " ".join(text.casefold().split())


def fact_is_new(fact: str, facts: object) -> bool:
    return isinstance(facts, list) and all(
        isinstance(existing, str) and normalize(existing) != normalize(fact)
        for existing in facts
    )


def chapter_contains_hero(chapter: str, hero: object) -> bool:
    return isinstance(hero, str) and bool(hero) and bool(
        re.search(rf"(?<!\w){re.escape(hero)}(?!\w)", chapter)
    )
