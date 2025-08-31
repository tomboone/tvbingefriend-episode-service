import os
import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, UTC

# Set required env vars for module import
os.environ['SQLALCHEMY_CONNECTION_STRING'] = 'sqlite:///:memory:'

from tvbingefriend_episode_service.services.monitoring_service import MonitoringService, ImportStatus


class TestMonitoringService(unittest.TestCase):

    def setUp(self):
        self.mock_storage_service = MagicMock()
        self.service = MonitoringService(storage_service=self.mock_storage_service)

    def test_start_show_episodes_import_tracking(self):
        """Test starting episode import tracking."""
        import_id = "test_import_123"
        show_id = 456
        estimated_episodes = 10
        
        self.service.start_show_episodes_import_tracking(import_id, show_id, estimated_episodes)
        
        # Verify entity was upserted to tracking table
        self.mock_storage_service.upsert_entity.assert_called_once()
        call_args = self.mock_storage_service.upsert_entity.call_args
        
        self.assertEqual(call_args[1]['table_name'], "episodeimporttracking")
        entity = call_args[1]['entity']
        self.assertEqual(entity['PartitionKey'], "show_episodes_import")
        self.assertEqual(entity['RowKey'], import_id)
        self.assertEqual(entity['Status'], ImportStatus.IN_PROGRESS.value)
        self.assertEqual(entity['ShowId'], show_id)
        self.assertEqual(entity['EstimatedEpisodes'], estimated_episodes)
        self.assertEqual(entity['CompletedEpisodes'], 0)
        self.assertEqual(entity['FailedEpisodes'], 0)

    def test_start_season_episodes_import_tracking(self):
        """Test starting season's episodes import tracking."""
        import_id = "test_import_123"
        season_id = 456
        estimated_episodes = 10
        
        self.service.start_season_episodes_import_tracking(import_id, season_id, estimated_episodes)
        
        # Verify entity was upserted to tracking table
        self.mock_storage_service.upsert_entity.assert_called_once()
        call_args = self.mock_storage_service.upsert_entity.call_args
        
        self.assertEqual(call_args[1]['table_name'], "episodeimporttracking")
        entity = call_args[1]['entity']
        self.assertEqual(entity['PartitionKey'], "season_episodes_import")
        self.assertEqual(entity['RowKey'], import_id)
        self.assertEqual(entity['Status'], ImportStatus.IN_PROGRESS.value)
        self.assertEqual(entity['SeasonId'], season_id)
        self.assertEqual(entity['EstimatedEpisodes'], estimated_episodes)
        self.assertEqual(entity['CompletedEpisodes'], 0)
        self.assertEqual(entity['FailedEpisodes'], 0)

    def test_update_episode_import_progress_success(self):
        """Test updating episode import progress successfully."""
        import_id = "test_import_123"
        episode_id = 789
        
        # Mock existing entity
        existing_entity = {
            'PartitionKey': 'show_episodes_import',
            'RowKey': import_id,
            'CompletedEpisodes': 5,
            'FailedEpisodes': 1
        }
        self.mock_storage_service.get_entities.return_value = [existing_entity]
        
        self.service.update_episode_import_progress(import_id, episode_id, success=True)
        
        # Verify entity was fetched and updated
        self.mock_storage_service.get_entities.assert_called_once_with(
            table_name="episodeimporttracking",
            filter_query=f"PartitionKey eq 'show_episodes_import' and RowKey eq '{import_id}'"
        )
        
        # Verify upsert was called with updated entity
        self.mock_storage_service.upsert_entity.assert_called_once()
        updated_entity = self.mock_storage_service.upsert_entity.call_args[1]['entity']
        self.assertEqual(updated_entity['CompletedEpisodes'], 6)  # Incremented
        self.assertEqual(updated_entity['FailedEpisodes'], 1)  # Unchanged
        self.assertEqual(updated_entity['LastProcessedEpisodeId'], episode_id)

    def test_update_episode_import_progress_failure(self):
        """Test updating episode import progress with failure."""
        import_id = "test_import_123"
        episode_id = 789
        
        # Mock existing entity
        existing_entity = {
            'PartitionKey': 'season_episodes_import',
            'RowKey': import_id,
            'CompletedEpisodes': 5,
            'FailedEpisodes': 1
        }
        self.mock_storage_service.get_entities.return_value = [existing_entity]
        
        self.service.update_episode_import_progress(import_id, episode_id, success=False)
        
        # Verify failed episodes was incremented
        updated_entity = self.mock_storage_service.upsert_entity.call_args[1]['entity']
        self.assertEqual(updated_entity['CompletedEpisodes'], 5)  # Unchanged
        self.assertEqual(updated_entity['FailedEpisodes'], 2)  # Incremented

    def test_update_episode_import_progress_entity_not_found(self):
        """Test updating progress when tracking entity doesn't exist."""
        import_id = "test_import_123"
        episode_id = 789
        
        self.mock_storage_service.get_entities.return_value = []
        
        with patch('tvbingefriend_episode_service.services.monitoring_service.logging') as mock_logging:
            self.service.update_episode_import_progress(import_id, episode_id)
        
        # Should log error and not attempt upsert
        mock_logging.error.assert_called_once()
        self.mock_storage_service.upsert_entity.assert_not_called()

    def test_complete_season_episodes_import(self):
        """Test completing episode import tracking."""
        import_id = "test_import_123"
        final_status = ImportStatus.COMPLETED
        
        # Mock existing entity
        existing_entity = {
            'PartitionKey': 'season_episodes_import',
            'RowKey': import_id,
            'Status': ImportStatus.IN_PROGRESS.value
        }
        self.mock_storage_service.get_entities.return_value = [existing_entity]
        
        self.service.complete_season_episodes_import(import_id, final_status)
        
        # Verify entity was updated with completion status
        updated_entity = self.mock_storage_service.upsert_entity.call_args[1]['entity']
        self.assertEqual(updated_entity['Status'], ImportStatus.COMPLETED.value)
        self.assertIn('EndTime', updated_entity)
        self.assertIn('LastActivityTime', updated_entity)

    def test_complete_show_episodes_import(self):
        """Test completing show episodes import tracking."""
        import_id = "test_import_123"
        final_status = ImportStatus.COMPLETED
        
        # Mock existing entity for show episodes import
        existing_entity = {
            'PartitionKey': 'show_episodes_import',
            'RowKey': import_id,
            'Status': ImportStatus.IN_PROGRESS.value
        }
        self.mock_storage_service.get_entities.return_value = [existing_entity]
        
        self.service.complete_show_episodes_import(import_id, final_status)
        
        # Verify entity was updated with completion status
        updated_entity = self.mock_storage_service.upsert_entity.call_args[1]['entity']
        self.assertEqual(updated_entity['Status'], ImportStatus.COMPLETED.value)
        self.assertIn('EndTime', updated_entity)
        self.assertIn('LastActivityTime', updated_entity)

    def test_get_import_status_success(self):
        """Test getting import status successfully."""
        import_id = "test_import_123"
        expected_entity = {
            'PartitionKey': 'season_episodes_import',
            'RowKey': import_id,
            'Status': ImportStatus.IN_PROGRESS.value,
            'CompletedEpisodes': 8,
            'FailedEpisodes': 2
        }
        self.mock_storage_service.get_entities.return_value = [expected_entity]
        
        result = self.service.get_import_status(import_id)
        
        self.assertEqual(result, expected_entity)

    def test_get_import_status_not_found(self):
        """Test getting import status when not found."""
        import_id = "test_import_123"
        self.mock_storage_service.get_entities.return_value = []
        
        result = self.service.get_import_status(import_id)
        
        self.assertEqual(result, {})

    def test_track_retry_attempt(self):
        """Test tracking retry attempts."""
        operation_type = "episode_details"
        identifier = "episode_123"
        attempt = 2
        max_attempts = 3
        error = "Network timeout"
        
        self.service.track_retry_attempt(operation_type, identifier, attempt, max_attempts, error)
        
        # Verify retry entity was stored
        self.mock_storage_service.upsert_entity.assert_called_once()
        call_args = self.mock_storage_service.upsert_entity.call_args
        
        self.assertEqual(call_args[1]['table_name'], "episoderetrytracking")
        entity = call_args[1]['entity']
        self.assertEqual(entity['PartitionKey'], operation_type)
        self.assertEqual(entity['RowKey'], f"{identifier}_{attempt}")
        self.assertEqual(entity['Identifier'], identifier)
        self.assertEqual(entity['AttemptNumber'], attempt)
        self.assertEqual(entity['MaxAttempts'], max_attempts)
        self.assertEqual(entity['ErrorMessage'], error)
        self.assertIn('AttemptTime', entity)
        self.assertIn('NextRetryTime', entity)

    def test_get_failed_operations(self):
        """Test getting failed operations."""
        operation_type = "episode_details"
        max_age_hours = 24
        
        with patch('tvbingefriend_episode_service.services.monitoring_service.logging') as mock_logging:
            result = self.service.get_failed_operations(operation_type, max_age_hours)
        
        # Currently returns empty list as placeholder
        self.assertEqual(result, [])
        mock_logging.info.assert_called_once()

    def test_update_data_health(self):
        """Test updating data health metrics."""
        metric_name = "episodes_processed"
        value = 150
        threshold = 200
        
        self.service.update_data_health(metric_name, value, threshold)
        
        # Verify health entity was stored
        self.mock_storage_service.upsert_entity.assert_called_once()
        call_args = self.mock_storage_service.upsert_entity.call_args
        
        self.assertEqual(call_args[1]['table_name'], "episodedatahealth")
        entity = call_args[1]['entity']
        self.assertEqual(entity['PartitionKey'], "health")
        self.assertEqual(entity['RowKey'], metric_name)
        self.assertEqual(entity['Value'], str(value))
        self.assertEqual(entity['Threshold'], str(threshold))
        self.assertTrue(entity['IsHealthy'])  # 150 <= 200

    def test_update_data_health_unhealthy(self):
        """Test updating data health with unhealthy values."""
        metric_name = "error_rate"
        value = 15
        threshold = 10
        
        self.service.update_data_health(metric_name, value, threshold)
        
        entity = self.mock_storage_service.upsert_entity.call_args[1]['entity']
        self.assertFalse(entity['IsHealthy'])  # 15 > 10

    def test_check_data_freshness(self):
        """Test checking data freshness."""
        max_age_days = 7
        
        result = self.service.check_data_freshness(max_age_days)
        
        # Verify basic structure of response
        self.assertIn('last_check', result)
        self.assertIn('max_age_days', result)
        self.assertEqual(result['max_age_days'], max_age_days)
        self.assertIn('is_fresh', result)
        self.assertIn('total_episodes', result)
        
        # Verify data health was updated
        self.mock_storage_service.upsert_entity.assert_called_once()

    def test_get_health_summary(self):
        """Test getting health summary."""
        result = self.service.get_health_summary()
        
        # Verify basic structure of summary
        self.assertIn('last_check', result)
        self.assertIn('active_imports', result)
        self.assertIn('failed_operations', result)
        self.assertIn('data_freshness', result)
        self.assertIn('overall_health', result)
        self.assertEqual(result['overall_health'], "healthy")

    def test_exception_handling_in_update_progress(self):
        """Test exception handling in update_episode_import_progress."""
        import_id = "test_import_123"
        episode_id = 789
        
        self.mock_storage_service.get_entities.side_effect = Exception("Storage error")
        
        with patch('tvbingefriend_episode_service.services.monitoring_service.logging') as mock_logging:
            self.service.update_episode_import_progress(import_id, episode_id)
        
        # Should log error but not raise exception
        mock_logging.error.assert_called()

    def test_exception_handling_in_complete_season_episodes_import(self):
        """Test exception handling in complete_season_episodes_import."""
        import_id = "test_import_123"
        final_status = ImportStatus.COMPLETED
        
        self.mock_storage_service.get_entities.side_effect = Exception("Storage error")
        
        with patch('tvbingefriend_episode_service.services.monitoring_service.logging') as mock_logging:
            self.service.complete_season_episodes_import(import_id, final_status)
        
        # Should log error but not raise exception
        mock_logging.error.assert_called()

    def test_exception_handling_in_complete_show_episodes_import(self):
        """Test exception handling in complete_show_episodes_import."""
        import_id = "test_import_123"
        final_status = ImportStatus.COMPLETED
        
        self.mock_storage_service.get_entities.side_effect = Exception("Storage error")
        
        with patch('tvbingefriend_episode_service.services.monitoring_service.logging') as mock_logging:
            self.service.complete_show_episodes_import(import_id, final_status)
        
        # Should log error but not raise exception
        mock_logging.error.assert_called()

    def test_table_names_configuration(self):
        """Test that table names are correctly configured for episode service."""
        self.assertEqual(self.service.import_tracking_table, "episodeimporttracking")
        self.assertEqual(self.service.retry_tracking_table, "episoderetrytracking")  
        self.assertEqual(self.service.data_health_table, "episodedatahealth")

    def test_start_show_episodes_import_tracking_with_defaults(self):
        """Test starting episode import tracking with default values."""
        import_id = "test_import_123"
        show_id = 456
        
        # Call without estimated_episodes
        self.service.start_show_episodes_import_tracking(import_id, show_id)
        
        # Verify entity was upserted with default estimated value
        entity = self.mock_storage_service.upsert_entity.call_args[1]['entity']
        self.assertEqual(entity['EstimatedEpisodes'], -1)

    def test_get_import_status_exception_handling(self):
        """Test exception handling in get_import_status."""
        import_id = "test_import_123"
        
        self.mock_storage_service.get_entities.side_effect = Exception("Storage error")
        
        with patch('tvbingefriend_episode_service.services.monitoring_service.logging') as mock_logging:
            result = self.service.get_import_status(import_id)
        
        # Should return empty dict and log error
        self.assertEqual(result, {})
        mock_logging.error.assert_called()

    def test_check_data_freshness_exception_handling(self):
        """Test exception handling in check_data_freshness."""
        max_age_days = 7
        
        # Mock datetime.now to raise exception
        with patch('tvbingefriend_episode_service.services.monitoring_service.datetime') as mock_datetime:
            mock_datetime.now.side_effect = Exception("Time error")
            
            result = self.service.check_data_freshness(max_age_days)
        
        self.assertIn('error', result)

    def test_get_health_summary_exception_handling(self):
        """Test exception handling in get_health_summary."""
        with patch('tvbingefriend_episode_service.services.monitoring_service.datetime') as mock_datetime:
            mock_datetime.now.side_effect = Exception("Time error")
            
            result = self.service.get_health_summary()
        
        self.assertIn('error', result)


if __name__ == '__main__':
    unittest.main()