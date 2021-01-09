import typing

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class Resource():
    """Resource model"""
    name: str
    sha1sum: typing.Optional[str] = None
    size: typing.Optional[int] = None
    error: typing.Optional[str] = None
    created_at: datetime = datetime.utcnow()
    file: typing.Optional[Path] = None
    metadata_id: typing.Optional[int] = None
