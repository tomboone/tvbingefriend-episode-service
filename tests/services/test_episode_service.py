import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch, call
from types import ModuleType

import azure.functions as func

# Set required env vars for module import
os.environ['SQLALCHEMY_CONNECTION_STRING'] = 'sqlite:///:memory:'

# Create a mock TVMaze module to avoid import errors
mock_tvmaze_module = ModuleType('tvbingefriend_tvmaze_client')
mock_tvmaze_module.TVMazeAPI = MagicMock
sys.modules['tvbingefriend_tvmaze_client'] = mock_tvmaze_module

from tvbingefriend_episode_service.services.episode_service import EpisodeService
from tvbingefriend_episode_service.config import EPISODES_QUEUE, SHOW_IDS_TABLE


class TestEpisodeService(unittest.TestCase):

    def setUp(self):
        """Set up test environment for each test."""
        self.mock_episode_repo = MagicMock()
        with patch('tvbingefriend_episode_service.services.episode_service.db_session_manager'), \
             patch('tvbingefriend_episode_service.services.episode_service.TVMazeAPI') as mock_tvmaze:
            mock_tvmaze_instance = MagicMock()
            mock_tvmaze.return_value = mock_tvmaze_instance
            self.service = EpisodeService(episode_repository=self.mock_episode_repo)
        self.service.storage_service = MagicMock()
        self.service.tvmaze_api = MagicMock()
        self.service.monitoring_service = MagicMock()
        
        # Mock retry_service but make it actually execute the handler function
        self.service.retry_service = MagicMock()
        def mock_handle_retry(message, handler_func, operation_type):
            # Actually call the handler function for testing
            return handler_func(message)
        self.service.retry_service.handle_queue_message_with_retry.side_effect = mock_handle_retry
        
        # Mock the retry decorator to actually execute functions
        def mock_with_retry(operation_type, max_attempts=None):
            def decorator(func):
                return func  # Just return the function unchanged for testing
            return decorator
        self.service.retry_service.with_retry = mock_with_retry

    def test_start_get_all_shows_episodes(self):
        """Test starting the process of getting all shows' episodes."""
        # Mock show entities returned from table client
        mock_show_entities = [
            {"RowKey": "1", "PartitionKey": "show"},
            {"RowKey": "2", "PartitionKey": "show"},
            {"RowKey": "3", "PartitionKey": "show"}
        ]
        
        # Mock the table service client chain
        mock_table_service_client = MagicMock()
        mock_table_client = MagicMock()
        mock_table_service_client.get_table_client.return_value = mock_table_client
        mock_table_client.query_entities.return_value = iter(mock_show_entities)
        self.service.storage_service.get_table_service_client.return_value = mock_table_service_client
        
        import_id = self.service.start_get_all_shows_episodes()
        
        # Verify import tracking was started
        self.service.monitoring_service.start_show_episodes_import_tracking.assert_called_once()
        call_args = self.service.monitoring_service.start_show_episodes_import_tracking.call_args[1]
        self.assertEqual(call_args['show_id'], -1)  # Placeholder for bulk operation
        self.assertEqual(call_args['estimated_episodes'], -1)  # Updated for batched processing
        
        # Verify all shows were queued (3 shows + potentially 1 batch message, but since < batch_size, no next batch)
        self.assertEqual(self.service.storage_service.upload_queue_message.call_count, 3)
        expected_calls = [
            call(queue_name=EPISODES_QUEUE, message={"show_id": 1, "import_id": import_id}),
            call(queue_name=EPISODES_QUEUE, message={"show_id": 2, "import_id": import_id}),
            call(queue_name=EPISODES_QUEUE, message={"show_id": 3, "import_id": import_id})
        ]
        self.service.storage_service.upload_queue_message.assert_has_calls(expected_calls, any_order=True)
        
        # Verify table client was called
        mock_table_client.query_entities.assert_called_once_with(
            query_filter="PartitionKey eq 'show'",
            results_per_page=1000
        )

    def test_start_get_all_shows_episodes_no_shows(self):
        """Test starting episodes import when no shows exist."""
        # Mock empty result from table client
        mock_table_service_client = MagicMock()
        mock_table_client = MagicMock()
        mock_table_service_client.get_table_client.return_value = mock_table_client
        mock_table_client.query_entities.return_value = iter([])  # Empty iterator
        self.service.storage_service.get_table_service_client.return_value = mock_table_service_client
        
        import_id = self.service.start_get_all_shows_episodes()
        
        # Should still return import ID but not queue anything
        self.assertIsNotNone(import_id)
        self.service.storage_service.upload_queue_message.assert_not_called()
        
        # Should complete import as completed
        self.service.monitoring_service.complete_show_episodes_import.assert_called_once()

    def test_start_get_all_shows_episodes_exception(self):
        """Test exception handling in start_get_all_shows_episodes."""
        # Mock table service client to raise exception
        mock_table_service_client = MagicMock()
        mock_table_client = MagicMock()
        mock_table_service_client.get_table_client.return_value = mock_table_client
        mock_table_client.query_entities.side_effect = Exception("Storage error")
        self.service.storage_service.get_table_service_client.return_value = mock_table_service_client
        
        with self.assertRaises(Exception):
            self.service.start_get_all_shows_episodes()
        
        # Should complete import with failed status
        self.service.monitoring_service.complete_show_episodes_import.assert_called()

    def test_get_show_episodes_success(self):
        """Test processing episodes for a show successfully."""
        # Mock queue message
        mock_message = MagicMock()
        mock_message.get_json.return_value = {"show_id": 123, "import_id": "test_import_id"}
        
        # Mock episodes returned from TVMaze API
        mock_episodes = [
            {"id": 1, "name": "Episode 1", "season": 1, "number": 1},
            {"id": 2, "name": "Episode 2", "season": 1, "number": 2}
        ]
        self.service.tvmaze_api.get_episodes.return_value = mock_episodes
        
        # Mock database session manager
        with patch('tvbingefriend_episode_service.services.episode_service.db_session_manager') as mock_db_mgr:
            mock_db = MagicMock()
            mock_db_mgr.return_value.__enter__ = MagicMock(return_value=mock_db)
            mock_db_mgr.return_value.__exit__ = MagicMock(return_value=None)
            
            self.service.get_show_episodes(mock_message)
        
        # Verify TVMaze API was called
        self.service.tvmaze_api.get_episodes.assert_called_once_with(123)
        
        # Verify episodes were upserted
        self.assertEqual(self.mock_episode_repo.upsert_episode.call_count, 2)
        expected_upsert_calls = [
            call(mock_episodes[0], 123, mock_db),
            call(mock_episodes[1], 123, mock_db)
        ]
        self.mock_episode_repo.upsert_episode.assert_has_calls(expected_upsert_calls)
        
        # Verify progress tracking
        progress_calls = self.service.monitoring_service.update_episode_import_progress.call_args_list
        self.assertEqual(len(progress_calls), 2)

    def test_get_show_episodes_no_episodes(self):
        """Test processing when show has no episodes."""
        mock_message = MagicMock()
        mock_message.get_json.return_value = {"show_id": 123, "import_id": "test_import_id"}
        
        self.service.tvmaze_api.get_episodes.return_value = None
        
        self.service.get_show_episodes(mock_message)
        
        # Should not attempt to upsert anything
        self.mock_episode_repo.upsert_episode.assert_not_called()

    def test_get_show_episodes_missing_show_id(self):
        """Test processing with missing show_id in message."""
        mock_message = MagicMock()
        mock_message.get_json.return_value = {"import_id": "test_import_id"}  # Missing show_id
        
        self.service.get_show_episodes(mock_message)
        
        # Should not call TVMaze API
        self.service.tvmaze_api.get_episodes.assert_not_called()

    def test_get_show_episodes_upsert_failure(self):
        """Test handling of upsert failures."""
        mock_message = MagicMock()
        mock_message.get_json.return_value = {"show_id": 123, "import_id": "test_import_id"}
        
        mock_episodes = [{"id": 1, "name": "Episode 1", "season": 1, "number": 1}]
        self.service.tvmaze_api.get_episodes.return_value = mock_episodes
        
        # Mock upsert to fail
        with patch('tvbingefriend_episode_service.services.episode_service.db_session_manager'):
            # Make the retry decorator fail
            self.service.retry_service.with_retry.return_value = MagicMock(side_effect=Exception("Upsert failed"))
            
            self.service.get_show_episodes(mock_message)
        
        # Should track failed episode
        # Check that update_episode_import_progress was called
        self.service.monitoring_service.update_episode_import_progress.assert_called()

    def test_get_show_episodes_batch_message(self):
        """Test processing batch messages."""
        mock_message = MagicMock()
        mock_message.get_json.return_value = {
            "action": "process_batch",
            "import_id": "test_import_id", 
            "batch_number": 1,
            "batch_size": 500
        }
        
        # Mock the _process_shows_batch method
        with patch.object(self.service, '_process_shows_batch') as mock_process_batch:
            self.service.get_show_episodes(mock_message)
            
            # Verify batch processing was called with correct parameters
            mock_process_batch.assert_called_once_with(
                import_id="test_import_id",
                batch_number=1,
                batch_size=500
            )

    def test_process_shows_batch_success(self):
        """Test successful batch processing."""
        import_id = "test_import_123"
        batch_number = 0
        batch_size = 2
        
        # Mock show entities
        mock_show_entities = [
            {"RowKey": "1", "PartitionKey": "show"},
            {"RowKey": "2", "PartitionKey": "show"}
        ]
        
        # Mock the table service client chain
        mock_table_service_client = MagicMock()
        mock_table_client = MagicMock()
        mock_table_service_client.get_table_client.return_value = mock_table_client
        mock_table_client.query_entities.return_value = iter(mock_show_entities)
        self.service.storage_service.get_table_service_client.return_value = mock_table_service_client
        
        result = self.service._process_shows_batch(import_id, batch_number, batch_size)
        
        self.assertEqual(result, import_id)
        
        # Verify shows were queued
        self.assertEqual(self.service.storage_service.upload_queue_message.call_count, 3)
        expected_calls = [
            call(queue_name=EPISODES_QUEUE, message={"show_id": 1, "import_id": import_id}),
            call(queue_name=EPISODES_QUEUE, message={"show_id": 2, "import_id": import_id})
        ]
        self.service.storage_service.upload_queue_message.assert_has_calls(expected_calls, any_order=True)

    def test_process_shows_batch_with_next_batch(self):
        """Test batch processing that queues next batch."""
        import_id = "test_import_123"
        batch_number = 0
        batch_size = 1
        
        # Mock show entities - exactly batch_size, indicating more might exist
        mock_show_entities = [
            {"RowKey": "1", "PartitionKey": "show"}
        ]
        
        # Mock the table service client chain
        mock_table_service_client = MagicMock()
        mock_table_client = MagicMock()
        mock_table_service_client.get_table_client.return_value = mock_table_client
        mock_table_client.query_entities.return_value = iter(mock_show_entities)
        self.service.storage_service.get_table_service_client.return_value = mock_table_service_client
        
        result = self.service._process_shows_batch(import_id, batch_number, batch_size)
        
        # Should queue show and next batch message
        self.assertEqual(self.service.storage_service.upload_queue_message.call_count, 2)

    def test_get_updates_success(self):
        """Test getting updates successfully."""
        mock_updates = {
            "1": 1640995200,  # timestamp
            "2": 1640995300,
            "3": 1640995400
        }
        self.service.tvmaze_api.get_show_updates.return_value = mock_updates
        
        self.service.get_updates("day")
        
        # Verify TVMaze API was called
        self.service.tvmaze_api.get_show_updates.assert_called_once_with(period="day")
        
        # Verify shows were queued for episode processing
        self.assertEqual(self.service.storage_service.upload_queue_message.call_count, 3)
        expected_calls = [
            call(queue_name=EPISODES_QUEUE, message={"show_id": 1}),
            call(queue_name=EPISODES_QUEUE, message={"show_id": 2}),
            call(queue_name=EPISODES_QUEUE, message={"show_id": 3})
        ]
        self.service.storage_service.upload_queue_message.assert_has_calls(expected_calls, any_order=True)
        
        # Verify health metrics were updated
        self.service.monitoring_service.update_data_health.assert_called_once_with(
            metric_name="updates_processed",
            value=3,
            threshold=3 * 0.95  # 95% success rate threshold
        )

    def test_get_updates_no_updates(self):
        """Test get_updates when no updates are found."""
        self.service.tvmaze_api.get_show_updates.return_value = None
        
        self.service.get_updates("day")
        
        # Should not queue anything or update health metrics
        self.service.storage_service.upload_queue_message.assert_not_called()
        self.service.monitoring_service.update_data_health.assert_not_called()

    def test_get_updates_exception(self):
        """Test exception handling in get_updates."""
        self.service.tvmaze_api.get_show_updates.side_effect = Exception("API error")
        
        with self.assertRaises(Exception):
            self.service.get_updates("day")
        
        # Should update health metrics with failure
        self.service.monitoring_service.update_data_health.assert_called_once_with(
            metric_name="updates_failed",
            value=1
        )

    def test_get_updates_queue_failure(self):
        """Test handling of queue upload failures in get_updates."""
        mock_updates = {"1": 1640995200}
        self.service.tvmaze_api.get_show_updates.return_value = mock_updates
        self.service.storage_service.upload_queue_message.side_effect = Exception("Queue error")
        
        self.service.get_updates("day")
        
        # Should still update health metrics showing 0 successful
        self.service.monitoring_service.update_data_health.assert_called_once_with(
            metric_name="updates_processed",
            value=0,
            threshold=1 * 0.95
        )

    def test_get_import_status(self):
        """Test getting import status."""
        expected_status = {"status": "in_progress", "completed": 5, "failed": 1}
        self.service.monitoring_service.get_import_status.return_value = expected_status
        
        result = self.service.get_import_status("test_import_id")
        
        self.assertEqual(result, expected_status)
        self.service.monitoring_service.get_import_status.assert_called_once_with("test_import_id")

    def test_get_system_health(self):
        """Test getting system health status."""
        mock_health = {"overall_health": "healthy", "active_imports": 0}
        mock_freshness = {"is_fresh": True, "stale_count": 0}
        
        self.service.monitoring_service.get_health_summary.return_value = mock_health
        self.service.monitoring_service.check_data_freshness.return_value = mock_freshness
        
        result = self.service.get_system_health()
        
        self.assertEqual(result["overall_health"], "healthy")
        self.assertTrue(result["tvmaze_api_healthy"])
        self.assertEqual(result["data_freshness"], mock_freshness)

    def test_retry_failed_operations_success(self):
        """Test retrying failed operations successfully."""
        mock_failed_ops = [
            {"operation_id": "op1", "data": {"show_id": 1}},
            {"operation_id": "op2", "data": {"show_id": 2}}
        ]
        self.service.monitoring_service.get_failed_operations.return_value = mock_failed_ops
        self.service.retry_service.retry_failed_operation.return_value = True
        
        result = self.service.retry_failed_operations("show_episodes", 24)
        
        self.assertEqual(result["operation_type"], "show_episodes")
        self.assertEqual(result["found_failed_operations"], 2)
        self.assertEqual(result["successful_retries"], 2)
        self.assertEqual(result["failed_retries"], 0)
        
        # Verify retry service was called for each operation
        self.assertEqual(self.service.retry_service.retry_failed_operation.call_count, 2)

    def test_retry_failed_operations_with_failures(self):
        """Test retrying failed operations with some failures."""
        mock_failed_ops = [
            {"operation_id": "op1", "data": {"show_id": 1}},
            {"operation_id": "op2", "data": {"show_id": 2}}
        ]
        self.service.monitoring_service.get_failed_operations.return_value = mock_failed_ops
        # First succeeds, second fails
        self.service.retry_service.retry_failed_operation.side_effect = [True, False]
        
        result = self.service.retry_failed_operations("show_episodes", 24)
        
        self.assertEqual(result["successful_retries"], 1)
        self.assertEqual(result["failed_retries"], 1)

    def test_retry_failed_operations_with_exception(self):
        """Test retry operations when retry service raises exception."""
        mock_failed_ops = [{"operation_id": "op1", "data": {"show_id": 1}}]
        self.service.monitoring_service.get_failed_operations.return_value = mock_failed_ops
        self.service.retry_service.retry_failed_operation.side_effect = Exception("Retry error")
        
        result = self.service.retry_failed_operations("show_episodes", 24)
        
        self.assertEqual(result["successful_retries"], 0)
        self.assertEqual(result["failed_retries"], 1)
        self.assertEqual(len(result["retry_attempts"]), 1)
        self.assertIn("error", result["retry_attempts"][0])

    def test_get_show_episodes_with_show_in_episode_data(self):
        """Test processing episodes where show data is embedded in episode."""
        mock_message = MagicMock()
        mock_message.get_json.return_value = {"show_id": 123, "import_id": "test_import_id"}
        
        # Mock episodes with embedded show data
        mock_episodes = [
            {"id": 1, "name": "Episode 1", "show": {"id": 456}},  # Different show_id in episode
            {"id": 2, "name": "Episode 2", "_links": {"show": {"href": "/shows/789"}}}  # Show from links
        ]
        self.service.tvmaze_api.get_episodes.return_value = mock_episodes
        
        # Mock database session manager
        with patch('tvbingefriend_episode_service.services.episode_service.db_session_manager') as mock_db_mgr:
            mock_db = MagicMock()
            mock_db_mgr.return_value.__enter__ = MagicMock(return_value=mock_db)
            mock_db_mgr.return_value.__exit__ = MagicMock(return_value=None)
            
            self.service.get_show_episodes(mock_message)
        
        # Verify episodes were upserted with correct show_ids
        expected_upsert_calls = [
            call(mock_episodes[0], 456, mock_db),  # Uses show_id from episode.show.id
            call(mock_episodes[1], 789, mock_db)   # Uses show_id from _links
        ]
        self.mock_episode_repo.upsert_episode.assert_has_calls(expected_upsert_calls)

    def test_get_show_episodes_invalid_episode_data(self):
        """Test handling of invalid episode data."""
        mock_message = MagicMock()
        mock_message.get_json.return_value = {"show_id": 123, "import_id": "test_import_id"}
        
        # Mock episodes with invalid data
        mock_episodes = [
            None,  # Invalid episode
            {},    # Empty episode
            {"id": 1, "name": "Valid Episode"}  # Valid episode
        ]
        self.service.tvmaze_api.get_episodes.return_value = mock_episodes
        
        # Mock database session manager
        with patch('tvbingefriend_episode_service.services.episode_service.db_session_manager') as mock_db_mgr:
            mock_db = MagicMock()
            mock_db_mgr.return_value.__enter__ = MagicMock(return_value=mock_db)
            mock_db_mgr.return_value.__exit__ = MagicMock(return_value=None)
            
            self.service.get_show_episodes(mock_message)
        
        # Should only process the valid episode
        self.assertEqual(self.mock_episode_repo.upsert_episode.call_count, 1)


if __name__ == '__main__':
    unittest.main()