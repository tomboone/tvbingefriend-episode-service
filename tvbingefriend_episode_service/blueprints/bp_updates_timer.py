"""Update episodes from TV Maze"""
import logging

import azure.functions as func

from tvbingefriend_episode_service.config import UPDATES_NCRON
from tvbingefriend_episode_service.services.episode_service import EpisodeService

bp: func.Blueprint = func.Blueprint()


# noinspection PyUnusedLocal
@bp.function_name(name="get_updates_timer")
@bp.timer_trigger(
    arg_name="updateepisodes",
    schedule=UPDATES_NCRON,
    run_on_startup=False
)
def get_updates_timer(updateepisodes: func.TimerRequest) -> None:
    """Update episodes from TV Maze"""
    try:
        episode_service: EpisodeService = EpisodeService()  # create episode service
        episode_service.get_updates()  # get updates
    except Exception as e:   # catch errors and log them
        logging.error(
            f"get_updates_timer: Unhandled exception. Error: {e}",
            exc_info=True
        )
        raise