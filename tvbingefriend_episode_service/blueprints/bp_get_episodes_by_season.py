"""Get episodes for a specific season by show ID and season number"""
import json
import logging

import azure.functions as func

from tvbingefriend_episode_service.services.episode_service import EpisodeService

bp: func.Blueprint = func.Blueprint()


@bp.function_name(name="get_episodes_by_season")
@bp.route(route="shows/{show_id:int}/seasons/{season_number:int}/episodes", methods=["GET"])
def get_episodes_by_season(req: func.HttpRequest) -> func.HttpResponse:
    """Get all episodes for a specific season by show ID and season number

    Args:
        req (func.HttpRequest): HTTP request

    Returns:
        func.HttpResponse: HTTP response with episodes data
    """
    try:
        show_id = req.route_params.get('show_id')
        season_number = req.route_params.get('season_number')

        if not show_id or not season_number:
            return func.HttpResponse(
                body="Show ID and season number are required",
                status_code=400
            )

        show_id_int = int(show_id)
        season_number_int = int(season_number)

        episode_service = EpisodeService()
        episodes = episode_service.get_episodes_by_season(show_id_int, season_number_int)

        return func.HttpResponse(
            body=json.dumps(episodes),
            status_code=200,
            headers={
                "Content-Type": "application/json",
                "Cache-Control": "public, max-age=3600",  # Cache for 1 hour
                "ETag": f'"{show_id_int}-{season_number_int}-{len(episodes)}"'  # Simple ETag based on content
            }
        )

    except ValueError:
        return func.HttpResponse(
            body="Invalid show ID or season number format",
            status_code=400
        )
    except Exception as e:
        logging.error(f"get_episodes_by_season: Unhandled exception: {e}", exc_info=True)
        return func.HttpResponse(
            body="Internal server error",
            status_code=500
        )