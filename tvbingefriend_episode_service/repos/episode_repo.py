"""Repository for episodes"""
import logging
from typing import Any

from sqlalchemy import inspect
from sqlalchemy.dialects.mysql import Insert, insert as mysql_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, Mapper, ColumnProperty

from tvbingefriend_episode_service.models.episode import Episode


# noinspection PyMethodMayBeStatic
class EpisodeRepository:
    """Repository for episodes"""
    def upsert_episode(self, episode: dict[str, Any], show_id: int, db: Session) -> None:
        """Upsert an episode in the database

        Args:
            episode (dict[str, Any]): Episode to upsert
            show_id (int): ID of the show this episode belongs to
            db (Session): Database session
        """
        episode_id: int | None = episode.get("id")  # get episode_id from episode
        logging.debug(f"EpisodeRepository.upsert_episode: episode_id: {episode_id}")

        if not episode_id:  # if episode_id is missing, log error and return
            logging.error("episode_repository.upsert_episode: Error upserting episode: Episode must have an episode_id")
            return

        mapper: Mapper = inspect(Episode)  # get episode mapper
        episode_columns: set[str] = {  # get episode columns
            prop.key for prop in mapper.attrs.values() if isinstance(prop, ColumnProperty)
        }

        insert_values: dict[str, Any] = {  # create insert values
            key: value for key, value in episode.items() if key in episode_columns
        }
        insert_values["id"] = episode_id  # add id value to insert values
        insert_values["show_id"] = show_id  # add show_id value to insert values

        update_values: dict[str, Any] = {  # create update values
            key: value for key, value in insert_values.items() if key != "id"
        }

        try:

            # noinspection PyTypeHints
            stmt: Insert = mysql_insert(Episode).values(insert_values)  # create insert statement
            stmt = stmt.on_duplicate_key_update(**update_values)  # add duplicate key update statement

            db.execute(stmt)  # execute insert statement
            db.flush()  # flush changes

        except SQLAlchemyError as e:  # catch any SQLAchemy errors and log them
            logging.error(
                f"episode_repository.upsert_episode: Database error during upsert of episode_id {episode_id}: {e}"
            )
        except Exception as e:  # catch any other errors and log them
            logging.error(
                f"episode_repository.upsert_episode: Unexpected error during upsert of episode episode_id {episode_id}: {e}"
            )