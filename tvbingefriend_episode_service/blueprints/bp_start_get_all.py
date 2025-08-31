"""Start get all episodes from TV Maze"""
import azure.functions as func

from tvbingefriend_episode_service.services.episode_service import EpisodeService

bp: func.Blueprint = func.Blueprint()


# noinspection PyUnusedLocal
@bp.function_name(name="start_get_all")
@bp.route(route="start_get_episodes", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def start_get_all(req: func.HttpRequest) -> func.HttpResponse:
    """Start get all episodes from TV Maze

    Gets all show IDs from the SHOW_IDS_TABLE and queues each show for episode processing.

    Args:
        req (func.HttpRequest): Request object

    Returns:
        func.HttpResponse: Response object
    """

    episode_service: EpisodeService = EpisodeService()  # initialize episode service

    import_id = episode_service.start_get_all_shows_episodes()  # initiate retrieval of all episodes

    response_text = f"Getting all episodes from TV Maze for all shows. Import ID: {import_id}"  # set response text
    response = func.HttpResponse(response_text, status_code=202)  # set http response

    return response