"""Get episode by ID"""
import json
import logging
import hashlib

import azure.functions as func

from tvbingefriend_episode_service.services.episode_service import EpisodeService

bp: func.Blueprint = func.Blueprint()


@bp.function_name(name="get_episode_by_id")
@bp.route(route="episodes/{episode_id:int}", methods=["GET"])
def get_episode_by_id(req: func.HttpRequest) -> func.HttpResponse:
    """Get an episode by its ID

    Args:
        req (func.HttpRequest): HTTP request

    Returns:
        func.HttpResponse: HTTP response with episode data
    """
    try:
        episode_id = req.route_params.get('episode_id')
        if not episode_id:
            return func.HttpResponse(
                body="Episode ID is required",
                status_code=400
            )

        episode_id_int = int(episode_id)
        episode_service = EpisodeService()
        episode = episode_service.get_episode_by_id(episode_id_int)

        if not episode:
            return func.HttpResponse(
                body="Episode not found",
                status_code=404
            )

        # Generate ETag for caching
        etag = hashlib.md5(json.dumps(episode, sort_keys=True).encode(), usedforsecurity=False).hexdigest()

        # Check if client has current version
        if_none_match = req.headers.get('If-None-Match')
        if if_none_match == etag:
            return func.HttpResponse(status_code=304)

        return func.HttpResponse(
            body=json.dumps(episode),
            status_code=200,
            headers={
                "Content-Type": "application/json",
                "Cache-Control": "public, max-age=3600",  # Cache for 1 hour
                "ETag": etag
            }
        )

    except ValueError:
        return func.HttpResponse(
            body="Invalid episode ID format",
            status_code=400
        )
    except Exception as e:
        logging.error(f"get_episode_by_id: Unhandled exception: {e}", exc_info=True)
        return func.HttpResponse(
            body="Internal server error",
            status_code=500
        )