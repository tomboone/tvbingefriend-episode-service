"""Service for TV episode-related operations."""
import logging
from datetime import datetime, UTC
from typing import Any
import uuid

import azure.functions as func
from tvbingefriend_azure_storage_service import StorageService  # type: ignore
from tvbingefriend_tvmaze_client import TVMazeAPI  # type: ignore

from tvbingefriend_episode_service.config import (
    STORAGE_CONNECTION_STRING,
    EPISODES_QUEUE,
    SHOW_IDS_TABLE
)
from tvbingefriend_episode_service.repos.episode_repo import EpisodeRepository
from tvbingefriend_episode_service.utils import db_session_manager
from tvbingefriend_episode_service.services.monitoring_service import MonitoringService, ImportStatus
from tvbingefriend_episode_service.services.retry_service import RetryService


# noinspection PyMethodMayBeStatic
class EpisodeService:
    """Service for TV episode-related operations."""
    def __init__(self, 
                 episode_repository: EpisodeRepository | None = None,
                 monitoring_service: MonitoringService | None = None,
                 retry_service: RetryService | None = None) -> None:
        self.episode_repository = episode_repository or EpisodeRepository()
        self.storage_service = StorageService(STORAGE_CONNECTION_STRING)
        
        # Use TVMaze client
        self.tvmaze_api = TVMazeAPI()
        
        # Initialize monitoring services
        self.monitoring_service = monitoring_service or MonitoringService()
        self.retry_service = retry_service or RetryService()
        
        # Current bulk import ID for tracking
        self.current_import_id: str | None = None

    def start_get_all_shows_episodes(self) -> str:
        """Start getting all episodes for all shows from the SHOW_IDS_TABLE using batched processing.
        
        Returns:
            Import ID for tracking progress
        """
        # Generate unique import ID
        import_id = f"episodes_import_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
        self.current_import_id = import_id
        
        logging.info(
            f"EpisodeService.start_get_all_shows_episodes: Starting batched episodes retrieval with import ID: {import_id}"
        )

        try:
            # Start tracking this bulk import (estimated count will be updated as we process)
            self.monitoring_service.start_show_episodes_import_tracking(
                import_id=import_id,
                show_id=-1,  # Placeholder for bulk operation
                estimated_episodes=-1  # Will be updated as batches are processed
            )
            
            # Start the first batch
            return self._process_shows_batch(import_id=import_id, batch_number=0)
            
        except Exception as e:
            logging.error(f"Failed to start episode import: {e}")
            self.monitoring_service.complete_show_episodes_import(import_id, ImportStatus.FAILED)
            raise

    def _process_shows_batch(self, import_id: str, batch_number: int, batch_size: int = 1000) -> str:
        """Process a batch of shows for episode retrieval.
        
        Args:
            import_id: Import operation identifier
            batch_number: Current batch number (0-based)
            batch_size: Number of shows to process in this batch
            
        Returns:
            Import ID for tracking progress
        """
        logging.info(f"Processing batch {batch_number} with batch_size {batch_size} for import {import_id}")
        
        try:
            # Get table service client for pagination
            table_client = self.storage_service.get_table_service_client().get_table_client(SHOW_IDS_TABLE)
            
            # Calculate skip amount for this batch
            skip_count = batch_number * batch_size
            
            # Query entities with pagination
            filter_query = "PartitionKey eq 'show'"
            entities_iter = table_client.query_entities(
                query_filter=filter_query,
                results_per_page=batch_size
            )
            
            # Skip to the correct batch
            current_count = 0
            batch_entities: list[dict[str, Any]] = []
            total_processed = 0
            
            for entity in entities_iter:
                # Skip entities until we reach our batch
                if current_count < skip_count:
                    current_count += 1
                    continue
                    
                # Collect entities for this batch
                if len(batch_entities) < batch_size:
                    batch_entities.append(entity)
                    total_processed += 1
                else:
                    # We have enough for this batch, break to process
                    break
                    
                current_count += 1
            
            if not batch_entities:
                logging.info(f"No more entities found in batch {batch_number}. Completing import {import_id}")
                self.monitoring_service.complete_show_episodes_import(import_id, ImportStatus.COMPLETED)
                return import_id
            
            logging.info(f"Found {len(batch_entities)} shows in batch {batch_number}")
            
            # Queue each show ID in this batch for episode processing
            queued_count = 0
            for show_entity in batch_entities:
                row_key = show_entity.get("RowKey")
                if row_key is None:
                    logging.warning(f"Entity missing RowKey, skipping: {show_entity}")
                    continue
                    
                show_id = int(row_key)
                show_message: dict[str, Any] = {
                    "show_id": show_id,
                    "import_id": import_id
                }
                
                self.storage_service.upload_queue_message(
                    queue_name=EPISODES_QUEUE,
                    message=show_message
                )
                queued_count += 1
            
            logging.info(f"Queued {queued_count} shows from batch {batch_number}")
            
            # Check if there might be more batches by trying to get one more entity
            has_more = len(batch_entities) == batch_size
            
            if has_more:
                # Queue the next batch for processing
                next_batch_message = {
                    "import_id": import_id,
                    "batch_number": batch_number + 1,
                    "batch_size": batch_size,
                    "action": "process_batch"
                }
                
                self.storage_service.upload_queue_message(
                    queue_name=EPISODES_QUEUE,
                    message=next_batch_message
                )
                
                logging.info(f"Queued next batch {batch_number + 1} for processing")
            else:
                logging.info(f"Batch {batch_number} was the final batch. Import {import_id} batching complete")
            
            return import_id
            
        except Exception as e:
            logging.error(f"Failed to process batch {batch_number} for import {import_id}: {e}")
            self.monitoring_service.complete_show_episodes_import(import_id, ImportStatus.FAILED)
            raise

    def get_show_episodes(self, episode_msg: func.QueueMessage) -> None:
        """Get all episodes for a specific show from TV Maze, or process batch messages.

        Args:
            episode_msg (func.QueueMessage): Show ID message or batch processing message
        """
        logging.info("=== EpisodeService.get_show_episodes ENTRY ===")
        
        # Handle message with retry logic
        def handle_show_episodes(message: func.QueueMessage) -> None:
            """Handle show episodes message or batch processing message."""
            logging.info("=== handle_show_episodes ENTRY ===")
            try:
                msg_data = message.get_json()
                logging.info(f"Message data in handle_show_episodes: {msg_data}")
                
                # Check if this is a batch processing message
                action = msg_data.get("action")
                if action == "process_batch":
                    batch_import_id = msg_data.get("import_id")
                    batch_number = msg_data.get("batch_number")
                    batch_size = msg_data.get("batch_size", 1000)
                    
                    logging.info(f"Processing batch message for import {batch_import_id}, batch {batch_number}")
                    self._process_shows_batch(
                        import_id=batch_import_id, batch_number=batch_number, batch_size=batch_size
                    )
                    return
                
                # Handle regular show episode message
                show_id: int | None = msg_data.get("show_id")
                import_id: str | None = msg_data.get("import_id")
                logging.info(f"Extracted show_id: {show_id}, import_id: {import_id}")

                if show_id is None:
                    logging.error("Queue message is missing 'show_id' number.")
                    return

                logging.info(f"EpisodeService.get_show_episodes: Getting episodes from TV Maze for show_id: {show_id}")
            except Exception as err:
                logging.error(f"Error in handle_show_episodes setup: {err}", exc_info=True)
                raise

            try:
                logging.info(f"Calling TVMaze API for show {show_id} episodes...")
                # TVMaze API now has built-in rate limiting and retry logic
                episodes: list[dict[str, Any]] | None = self.tvmaze_api.get_episodes(show_id)
                logging.info(f"TVMaze API returned {len(episodes) if episodes else 0} episodes for show {show_id}")

                if episodes:
                    # Process episodes with database retry logic
                    success_count = 0
                    for episode in episodes:
                        if not episode or not isinstance(episode, dict):
                            logging.error("EpisodeService.upsert_episode: Episode not found.")
                            continue
                        
                        @self.retry_service.with_retry('database_write', max_attempts=3)
                        def upsert_with_retry():
                            """Upsert episode into database."""
                            with db_session_manager() as db:
                                # TVMaze episodes API returns episodes with show information embedded
                                # When calling get_episodes(show_id), we already have the show_id from the parameter
                                episode_show_id = episode.get('show', {}).get('id')
                                if not episode_show_id:
                                    # Try to get it from the _links as fallback
                                    show_link = episode.get('_links', {}).get('show', {}).get('href')
                                    if show_link and '/shows/' in show_link:
                                        # Extract show_id from href like "/shows/123"
                                        try:
                                            episode_show_id = int(show_link.split('/shows/')[-1])
                                        except (ValueError, IndexError):
                                            logging.warning(f"Could not extract show_id from link: {show_link}")
                                
                                episode_show_id = episode_show_id or show_id

                                if not episode_show_id:
                                    logging.error(f"Episode {episode.get('id')} missing show_id, skipping")
                                    return
                                
                                self.episode_repository.upsert_episode(episode, episode_show_id, db)
                        
                        try:
                            upsert_with_retry()
                            success_count += 1
                            
                            # Update progress tracking for each episode
                            if import_id:
                                episode_id = episode.get('id')
                                if episode_id:
                                    self.monitoring_service.update_episode_import_progress(import_id, episode_id)
                            
                        except Exception as err:
                            logging.error(
                                f"Failed to upsert episode {episode.get('id', 'unknown')} for show {show_id} "
                                f"after retries: {err}")
                            if import_id:
                                episode_id = episode.get('id')
                                if episode_id:
                                    self.monitoring_service.update_episode_import_progress(
                                        import_id, episode_id, success=False
                                    )

                    logging.info(f"Successfully processed {success_count}/{len(episodes)} episodes for show {show_id}")
                else:
                    logging.info(f"No episodes returned for show {show_id}")

            except Exception as err:
                logging.error(f"Failed to get episodes for show {show_id}: {err}")
                raise

        # Process with retry logic
        logging.info("=== Calling retry_service.handle_queue_message_with_retry ===")
        try:
            self.retry_service.handle_queue_message_with_retry(
                message=episode_msg,
                handler_func=handle_show_episodes,
                operation_type="show_episodes"
            )
            logging.info("=== retry_service.handle_queue_message_with_retry COMPLETED ===")
        except Exception as e:
            logging.error(f"=== ERROR in retry_service.handle_queue_message_with_retry: {e} ===", exc_info=True)
            raise

    def get_updates(self, since: str = "day"):
        """Get updates with rate limiting and monitoring.

        Args:
            since (str): Since parameter for TV Maze API. Defaults to "day".
        """
        logging.info(f"EpisodeService.get_updates: Getting updates since {since}")
        
        try:
            # TVMaze API now has built-in rate limiting and retry logic
            updates: dict[str, Any] = self.tvmaze_api.get_show_updates(period=since)
            
            if not updates:
                logging.info("No updates found")
                return
            
            logging.info(f"Found {len(updates)} show updates")
            
            # Process updates - directly queue shows for episode processing
            success_count = 0
            for show_id, last_updated in updates.items():
                try:
                    # Queue show ID directly for episode processing
                    episodes_queue_msg = {
                        "show_id": int(show_id),
                    }
                    
                    # Queue show ID to episodes queue for episode processing
                    self.storage_service.upload_queue_message(
                        queue_name=EPISODES_QUEUE,
                        message=episodes_queue_msg
                    )
                    
                    success_count += 1
                    
                except Exception as e:
                    logging.error(f"Failed to process update for show {show_id}: {e}")
            
            logging.info(f"Successfully queued {success_count}/{len(updates)} show updates for episode processing")
            
            # Update data health metrics
            self.monitoring_service.update_data_health(
                metric_name="updates_processed",
                value=success_count,
                threshold=len(updates) * 0.95  # Alert if less than 95% success rate
            )
            
        except Exception as e:
            logging.error(f"Failed to get show updates: {e}")
            self.monitoring_service.update_data_health(
                metric_name="updates_failed",
                value=1
            )
            raise

    def get_import_status(self, import_id: str) -> dict[str, Any]:
        """Get the status of an episode import operation.
        
        Args:
            import_id: Import operation identifier
            
        Returns:
            Dictionary with import status information
        """
        return self.monitoring_service.get_import_status(import_id)
    
    def get_system_health(self) -> dict[str, Any]:
        """Get overall system health status.
        
        Returns:
            Dictionary with system health information
        """
        health_summary = self.monitoring_service.get_health_summary()
        
        # TVMaze API status (basic connectivity assumed)
        health_summary['tvmaze_api_healthy'] = True  # Assume healthy for standard client
        
        # Add data freshness check
        freshness_status = self.monitoring_service.check_data_freshness()
        health_summary['data_freshness'] = freshness_status
        
        return health_summary
    
    def retry_failed_operations(self, operation_type: str, max_age_hours: int = 24) -> dict[str, Any]:
        """Retry failed operations of a specific type.
        
        Args:
            operation_type: Type of operations to retry
            max_age_hours: Only retry failures within this many hours
            
        Returns:
            Summary of retry attempts
        """
        failed_operations = self.monitoring_service.get_failed_operations(operation_type, max_age_hours)
        
        retry_summary: dict[str, Any] = {
            'operation_type': operation_type,
            'found_failed_operations': len(failed_operations),
            'successful_retries': 0,
            'failed_retries': 0,
            'retry_attempts': []
        }
        
        for operation in failed_operations:
            try:
                success = self.retry_service.retry_failed_operation(operation_type, operation)
                if success:
                    retry_summary['successful_retries'] += 1
                else:
                    retry_summary['failed_retries'] += 1
                
                retry_summary['retry_attempts'].append({
                    'operation': operation,
                    'success': success
                })
                
            except Exception as e:
                logging.error(f"Failed to retry operation {operation}: {e}")
                retry_summary['failed_retries'] += 1
                retry_summary['retry_attempts'].append({
                    'operation': operation,
                    'success': False,
                    'error': str(e)
                })
        
        return retry_summary