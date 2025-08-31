"""SQLAlchemy model for an episode."""
from sqlalchemy import String, Integer, Text, Date
from sqlalchemy.orm import mapped_column, Mapped
from sqlalchemy.dialects.mysql import JSON
from datetime import date

from tvbingefriend_episode_service.models.base import Base


class Episode(Base):
    """SQLAlchemy model for an episode."""
    __tablename__ = "episodes"

    # Attributes
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    show_id: Mapped[int] = mapped_column(Integer, nullable=False)
    url: Mapped[str] = mapped_column(Text)
    name: Mapped[str | None] = mapped_column(Text)
    season: Mapped[int | None] = mapped_column(Integer)
    number: Mapped[int | None] = mapped_column(Integer)
    type: Mapped[str | None] = mapped_column(String(255))
    airdate: Mapped[date | None] = mapped_column(Date)
    airtime: Mapped[str | None] = mapped_column(String(255))
    airstamp: Mapped[str | None] = mapped_column(String(255))
    runtime: Mapped[int | None] = mapped_column(Integer)
    rating: Mapped[dict | None] = mapped_column(JSON)
    image: Mapped[dict | None] = mapped_column(JSON)
    summary: Mapped[str | None] = mapped_column(Text)
    _links: Mapped[dict | None] = mapped_column(JSON)
