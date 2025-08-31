"""Get one show's episodes"""
import logging

import azure.functions as func

from tvbingefriend_episode_service.config import EPISODES_QUEUE, STORAGE_CONNECTION_SETTING_NAME
from tvbingefriend_episode_service.services.episode_service import EpisodeService

bp: func.Blueprint = func.Blueprint()


@bp.function_name(name="get_show_episodes")
@bp.queue_trigger(
    arg_name="episodemsg",
    queue_name=EPISODES_QUEUE,
    connection=STORAGE_CONNECTION_SETTING_NAME
)
def get_show_episodes(episodemsg: func.QueueMessage) -> None:
    """Get all episodes for one show

    Args:
        episodemsg (func.QueueMessage): Show ID message
    """
    try:
        logging.info("=== PROCESSING SHOW EPISODES MESSAGE ===")
        logging.info(f"Message ID: {episodemsg.id}")
        logging.info(f"Message content: {episodemsg.get_body().decode()}")
        logging.info(f"Dequeue count: {episodemsg.dequeue_count}")
        logging.info(f"Pop receipt: {episodemsg.pop_receipt}")
        
        # Try to parse message content
        try:
            msg_data = episodemsg.get_json()
            logging.info(f"Parsed message data: {msg_data}")
        except Exception as parse_e:
            logging.error(f"Failed to parse message JSON: {parse_e}")
            raise
        
        logging.info("Initializing EpisodeService...")
        episode_service: EpisodeService = EpisodeService()  # initialize episode service
        
        logging.info("Calling episode_service.get_show_episodes...")
        episode_service.get_show_episodes(episodemsg)   # get and process show episodes
        
        logging.info(f"=== SUCCESSFULLY PROCESSED MESSAGE ID: {episodemsg.id} ===")
    except Exception as e:  # catch any exceptions, log them, and re-raise them
        logging.error(
            f"=== ERROR PROCESSING MESSAGE ID {episodemsg.id} ===",
            exc_info=True
        )
        logging.error(f"Exception type: {type(e).__name__}")
        logging.error(f"Exception message: {str(e)}")
        raise