from dataclasses import dataclass, field
from typing import List


@dataclass
class Publisher:
    publisher_type: str = None
    publisher_string: str = None


@dataclass
class Editor:
    initials: List[str] = field(default_factory=list)
    firstnames: List[str] = field(default_factory=list)
    editor_type: str = None
    editor_string: str = None
    surnames: str = None


@dataclass
class Author:
    initials: List[str] = field(default_factory=list)
    firstnames: List[str] = field(default_factory=list)
    author_type: str = None
    author_string: str = None
    surnames: str = None,


@dataclass
class Reference:
    publishers: List[Publisher] = field(default_factory=list)
    editors: List[Editor] = field(default_factory=list)
    authors: List[Author] = field(default_factory=list)
    reference: str = None
    title: str = None
    year: int = None
    place: str = None
    start: int = None
    end: int = None
    source: str = None
    volume: str = None
    issue: str = None
    type: str = None
    doi: str = None
