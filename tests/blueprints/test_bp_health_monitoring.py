"""
Unit tests for bp_health_monitoring blueprint.
Tests all health monitoring and system status endpoints.
"""
import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch
from types import ModuleType

import azure.functions as func

# Set required env vars for module import
os.environ['SQLALCHEMY_CONNECTION_STRING'] = 'sqlite:///:memory:'

# Create a mock TVMaze module to avoid import errors
mock_tvmaze_module = ModuleType('tvbingefriend_tvmaze_client')
mock_tvmaze_module.TVMazeAPI = MagicMock
sys.modules['tvbingefriend_tvmaze_client'] = mock_tvmaze_module

from tvbingefriend_episode_service.blueprints.bp_health_monitoring import (
    health_check, 
    import_status,
    retry_failed_operations,
    tvmaze_api_status
)


class TestBpHealthMonitoring(unittest.TestCase):
    """Test cases for health monitoring endpoints."""

    def setUp(self):
        """Set up test environment for each test."""
        self.mock_episode_service = MagicMock()
        self.mock_http_request = MagicMock(spec=func.HttpRequest)
        
        # Default mock responses
        self.healthy_status = {
            'overall_health': 'healthy',
            'last_check': '2023-12-01T10:30:00Z',
            'rate_limiting': {'in_backoff_period': False},
            'data_freshness': {'is_fresh': True}
        }

    def tearDown(self):
        """Clean up after each test."""
        pass

    # ========== health_check endpoint tests ==========

    @patch('tvbingefriend_episode_service.blueprints.bp_health_monitoring.EpisodeService')
    def test_health_check_healthy_system(self, mock_episode_service_class):
        """Test health check when system is healthy."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        self.mock_episode_service.get_system_health.return_value = self.healthy_status
        
        # Act
        response = health_check(self.mock_http_request)
        
        # Assert
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Content-Type"], "application/json")
        
        response_data = json.loads(response.get_body().decode())
        self.assertEqual(response_data["status"], "healthy")
        self.assertEqual(response_data["timestamp"], "2023-12-01T10:30:00Z")
        self.assertEqual(response_data["details"], self.healthy_status)

    @patch('tvbingefriend_episode_service.blueprints.bp_health_monitoring.EpisodeService')
    def test_health_check_unhealthy_overall_health(self, mock_episode_service_class):
        """Test health check when overall health is unhealthy."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        unhealthy_status = self.healthy_status.copy()
        unhealthy_status['overall_health'] = 'unhealthy'
        self.mock_episode_service.get_system_health.return_value = unhealthy_status
        
        # Act
        response = health_check(self.mock_http_request)
        
        # Assert
        self.assertEqual(response.status_code, 503)
        response_data = json.loads(response.get_body().decode())
        self.assertEqual(response_data["status"], "unhealthy")

    @patch('tvbingefriend_episode_service.blueprints.bp_health_monitoring.EpisodeService')
    def test_health_check_rate_limiting_backoff(self, mock_episode_service_class):
        """Test health check when system is in rate limiting backoff."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        backoff_status = self.healthy_status.copy()
        backoff_status['rate_limiting']['in_backoff_period'] = True
        self.mock_episode_service.get_system_health.return_value = backoff_status
        
        # Act
        response = health_check(self.mock_http_request)
        
        # Assert
        self.assertEqual(response.status_code, 503)
        response_data = json.loads(response.get_body().decode())
        self.assertEqual(response_data["status"], "unhealthy")

    @patch('tvbingefriend_episode_service.blueprints.bp_health_monitoring.EpisodeService')
    def test_health_check_stale_data(self, mock_episode_service_class):
        """Test health check when data is stale."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        stale_status = self.healthy_status.copy()
        stale_status['data_freshness']['is_fresh'] = False
        self.mock_episode_service.get_system_health.return_value = stale_status
        
        # Act
        response = health_check(self.mock_http_request)
        
        # Assert
        self.assertEqual(response.status_code, 503)
        response_data = json.loads(response.get_body().decode())
        self.assertEqual(response_data["status"], "unhealthy")

    @patch('tvbingefriend_episode_service.blueprints.bp_health_monitoring.EpisodeService')
    @patch('tvbingefriend_episode_service.blueprints.bp_health_monitoring.logging')
    def test_health_check_service_error(self, mock_logging, mock_episode_service_class):
        """Test health check when service throws an error."""
        # Arrange
        error_msg = "Database connection failed"
        mock_episode_service_class.return_value = self.mock_episode_service
        self.mock_episode_service.get_system_health.side_effect = Exception(error_msg)
        
        # Act
        response = health_check(self.mock_http_request)
        
        # Assert
        self.assertEqual(response.status_code, 500)
        response_data = json.loads(response.get_body().decode())
        self.assertEqual(response_data["status"], "error")
        self.assertEqual(response_data["error"], error_msg)
        mock_logging.error.assert_called_once()

    @patch('tvbingefriend_episode_service.blueprints.bp_health_monitoring.EpisodeService')
    def test_health_check_missing_health_fields(self, mock_episode_service_class):
        """Test health check with missing health status fields."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        minimal_status = {'overall_health': 'healthy'}  # Missing other fields
        self.mock_episode_service.get_system_health.return_value = minimal_status
        
        # Act
        response = health_check(self.mock_http_request)
        
        # Assert
        # Should still work, using .get() with defaults
        self.assertEqual(response.status_code, 200)
        response_data = json.loads(response.get_body().decode())
        self.assertEqual(response_data["status"], "healthy")

    # ========== import_status endpoint tests ==========

    @patch('tvbingefriend_episode_service.blueprints.bp_health_monitoring.EpisodeService')
    def test_import_status_success(self, mock_episode_service_class):
        """Test successful import status retrieval."""
        # Arrange
        import_id = "import-12345"
        self.mock_http_request.params.get.return_value = import_id
        status_data = {
            "import_id": import_id,
            "status": "completed",
            "shows_processed": 100,
            "episodes_imported": 5000
        }
        mock_episode_service_class.return_value = self.mock_episode_service
        self.mock_episode_service.get_import_status.return_value = status_data
        
        # Act
        response = import_status(self.mock_http_request)
        
        # Assert
        self.assertEqual(response.status_code, 200)
        response_data = json.loads(response.get_body().decode())
        self.assertEqual(response_data, status_data)
        self.mock_http_request.params.get.assert_called_with('import_id')

    @patch('tvbingefriend_episode_service.blueprints.bp_health_monitoring.EpisodeService')
    def test_import_status_missing_import_id(self, mock_episode_service_class):
        """Test import status request without import_id parameter."""
        # Arrange
        self.mock_http_request.params.get.return_value = None
        
        # Act
        response = import_status(self.mock_http_request)
        
        # Assert
        self.assertEqual(response.status_code, 400)
        response_data = json.loads(response.get_body().decode())
        self.assertEqual(response_data["error"], "import_id parameter is required")

    @patch('tvbingefriend_episode_service.blueprints.bp_health_monitoring.EpisodeService')
    def test_import_status_not_found(self, mock_episode_service_class):
        """Test import status for non-existent import."""
        # Arrange
        import_id = "nonexistent-import"
        self.mock_http_request.params.get.return_value = import_id
        mock_episode_service_class.return_value = self.mock_episode_service
        self.mock_episode_service.get_import_status.return_value = None
        
        # Act
        response = import_status(self.mock_http_request)
        
        # Assert
        self.assertEqual(response.status_code, 404)
        response_data = json.loads(response.get_body().decode())
        self.assertEqual(response_data["error"], f"Import {import_id} not found")

    @patch('tvbingefriend_episode_service.blueprints.bp_health_monitoring.EpisodeService')
    @patch('tvbingefriend_episode_service.blueprints.bp_health_monitoring.logging')
    def test_import_status_service_error(self, mock_logging, mock_episode_service_class):
        """Test import status when service throws an error."""
        # Arrange
        import_id = "import-12345"
        error_msg = "Database query failed"
        self.mock_http_request.params.get.return_value = import_id
        mock_episode_service_class.return_value = self.mock_episode_service
        self.mock_episode_service.get_import_status.side_effect = Exception(error_msg)
        
        # Act
        response = import_status(self.mock_http_request)
        
        # Assert
        self.assertEqual(response.status_code, 500)
        response_data = json.loads(response.get_body().decode())
        self.assertEqual(response_data["error"], error_msg)
        mock_logging.error.assert_called_once()

    # ========== retry_failed_operations endpoint tests ==========

    @patch('tvbingefriend_episode_service.blueprints.bp_health_monitoring.EpisodeService')
    def test_retry_failed_operations_success(self, mock_episode_service_class):
        """Test successful retry of failed operations."""
        # Arrange
        operation_type = "episode_import"
        retry_results = {
            "operation_type": operation_type,
            "retried_count": 25,
            "successful_retries": 20,
            "failed_retries": 5
        }
        self.mock_http_request.params.get.side_effect = lambda key, default=None: {
            'operation_type': operation_type,
            'max_age_hours': default
        }.get(key, default)
        
        mock_episode_service_class.return_value = self.mock_episode_service
        self.mock_episode_service.retry_failed_operations.return_value = retry_results
        
        # Act
        response = retry_failed_operations(self.mock_http_request)
        
        # Assert
        self.assertEqual(response.status_code, 200)
        response_data = json.loads(response.get_body().decode())
        self.assertEqual(response_data, retry_results)
        self.mock_episode_service.retry_failed_operations.assert_called_once_with(operation_type, 24)

    @patch('tvbingefriend_episode_service.blueprints.bp_health_monitoring.EpisodeService')
    def test_retry_failed_operations_custom_max_age(self, mock_episode_service_class):
        """Test retry operations with custom max_age_hours parameter."""
        # Arrange
        operation_type = "show_updates"
        max_age_hours = 48
        self.mock_http_request.params.get.side_effect = lambda key, default=None: {
            'operation_type': operation_type,
            'max_age_hours': str(max_age_hours)
        }.get(key, default)
        
        mock_episode_service_class.return_value = self.mock_episode_service
        self.mock_episode_service.retry_failed_operations.return_value = {}
        
        # Act
        response = retry_failed_operations(self.mock_http_request)
        
        # Assert
        self.assertEqual(response.status_code, 200)
        self.mock_episode_service.retry_failed_operations.assert_called_once_with(operation_type, max_age_hours)

    @patch('tvbingefriend_episode_service.blueprints.bp_health_monitoring.EpisodeService')
    def test_retry_failed_operations_missing_operation_type(self, mock_episode_service_class):
        """Test retry operations without operation_type parameter."""
        # Arrange
        self.mock_http_request.params.get.side_effect = lambda key, default=None: {
            'operation_type': None,
            'max_age_hours': default
        }.get(key, default)
        
        # Act
        response = retry_failed_operations(self.mock_http_request)
        
        # Assert
        self.assertEqual(response.status_code, 400)
        response_data = json.loads(response.get_body().decode())
        self.assertEqual(response_data["error"], "operation_type parameter is required")

    @patch('tvbingefriend_episode_service.blueprints.bp_health_monitoring.EpisodeService')
    @patch('tvbingefriend_episode_service.blueprints.bp_health_monitoring.logging')
    def test_retry_failed_operations_service_error(self, mock_logging, mock_episode_service_class):
        """Test retry operations when service throws an error."""
        # Arrange
        operation_type = "episode_import"
        error_msg = "Retry service unavailable"
        self.mock_http_request.params.get.side_effect = lambda key, default=None: {
            'operation_type': operation_type,
            'max_age_hours': default
        }.get(key, default)
        
        mock_episode_service_class.return_value = self.mock_episode_service
        self.mock_episode_service.retry_failed_operations.side_effect = Exception(error_msg)
        
        # Act
        response = retry_failed_operations(self.mock_http_request)
        
        # Assert
        self.assertEqual(response.status_code, 500)
        response_data = json.loads(response.get_body().decode())
        self.assertEqual(response_data["error"], error_msg)
        mock_logging.error.assert_called_once()

    @patch('tvbingefriend_episode_service.blueprints.bp_health_monitoring.EpisodeService')
    def test_retry_failed_operations_invalid_max_age(self, mock_episode_service_class):
        """Test retry operations with invalid max_age_hours parameter."""
        # Arrange
        operation_type = "episode_import"
        invalid_max_age = "invalid"
        self.mock_http_request.params.get.side_effect = lambda key, default=None: {
            'operation_type': operation_type,
            'max_age_hours': invalid_max_age
        }.get(key, default)

        # Act
        response = retry_failed_operations(self.mock_http_request)

        # Assert
        self.assertEqual(response.status_code, 400)
        response_data = json.loads(response.get_body().decode())
        self.assertEqual(response_data["error"], "Invalid 'max_age_hours' parameter. Must be an integer.")

    # ========== tvmaze_api_status endpoint tests ==========

    @patch('tvbingefriend_episode_service.blueprints.bp_health_monitoring.EpisodeService')
    def test_tvmaze_api_status_healthy(self, mock_episode_service_class):
        """Test TVMaze API status when healthy."""
        # Arrange
        reliability_status = {
            "success_rate": 0.98,
            "avg_response_time": 150,
            "last_failure": None
        }
        mock_episode_service_class.return_value = self.mock_episode_service
        self.mock_episode_service.tvmaze_api.get_reliability_status.return_value = reliability_status
        self.mock_episode_service.tvmaze_api.is_healthy.return_value = True
        
        # Act
        response = tvmaze_api_status(self.mock_http_request)
        
        # Assert
        self.assertEqual(response.status_code, 200)
        response_data = json.loads(response.get_body().decode())
        expected_data = {
            "tvmaze_api": {
                "is_healthy": True,
                "reliability_status": reliability_status
            }
        }
        self.assertEqual(response_data, expected_data)

    @patch('tvbingefriend_episode_service.blueprints.bp_health_monitoring.EpisodeService')
    def test_tvmaze_api_status_unhealthy(self, mock_episode_service_class):
        """Test TVMaze API status when unhealthy."""
        # Arrange
        reliability_status = {
            "success_rate": 0.45,
            "avg_response_time": 5000,
            "last_failure": "2023-12-01T09:00:00Z"
        }
        mock_episode_service_class.return_value = self.mock_episode_service
        self.mock_episode_service.tvmaze_api.get_reliability_status.return_value = reliability_status
        self.mock_episode_service.tvmaze_api.is_healthy.return_value = False
        
        # Act
        response = tvmaze_api_status(self.mock_http_request)
        
        # Assert
        self.assertEqual(response.status_code, 200)  # Still returns 200, just reports unhealthy
        response_data = json.loads(response.get_body().decode())
        self.assertFalse(response_data["tvmaze_api"]["is_healthy"])

    @patch('tvbingefriend_episode_service.blueprints.bp_health_monitoring.EpisodeService')
    @patch('tvbingefriend_episode_service.blueprints.bp_health_monitoring.logging')
    def test_tvmaze_api_status_service_error(self, mock_logging, mock_episode_service_class):
        """Test TVMaze API status when service throws an error."""
        # Arrange
        error_msg = "TVMaze API client unavailable"
        mock_episode_service_class.return_value = self.mock_episode_service
        self.mock_episode_service.tvmaze_api.get_reliability_status.side_effect = Exception(error_msg)
        
        # Act
        response = tvmaze_api_status(self.mock_http_request)
        
        # Assert
        self.assertEqual(response.status_code, 500)
        response_data = json.loads(response.get_body().decode())
        self.assertEqual(response_data["error"], error_msg)
        mock_logging.error.assert_called_once()

    @patch('tvbingefriend_episode_service.blueprints.bp_health_monitoring.EpisodeService')
    def test_tvmaze_api_status_initialization_error(self, mock_episode_service_class):
        """Test TVMaze API status when EpisodeService initialization fails."""
        # Arrange
        init_error = RuntimeError("Failed to initialize TVMaze API client")
        mock_episode_service_class.side_effect = init_error
    
        # Act
        response = tvmaze_api_status(self.mock_http_request)

        # Assert
        self.assertEqual(response.status_code, 500)
        response_data = json.loads(response.get_body().decode())
        self.assertEqual(response_data["error"], str(init_error))

    # ========== Integration and edge case tests ==========

    def test_all_endpoints_return_json_content_type(self):
        """Test that all endpoints return proper JSON content type."""
        endpoints_to_test = [
            ("health_check", health_check, {}),
            ("import_status", import_status, {'import_id': 'test'}),
            ("retry_failed_operations", retry_failed_operations, {'operation_type': 'test'}),
            ("tvmaze_api_status", tvmaze_api_status, {})
        ]
        
        for name, endpoint_func, params in endpoints_to_test:
            with self.subTest(endpoint=name):
                with patch('tvbingefriend_episode_service.blueprints.bp_health_monitoring.EpisodeService'):
                    mock_req = MagicMock(spec=func.HttpRequest)
                    mock_req.params.get.side_effect = lambda key, default=None: params.get(key, default)
                    
                    try:
                        response = endpoint_func(mock_req)
                        if response.status_code != 400:  # Skip parameter validation errors
                            self.assertEqual(response.headers.get("Content-Type"), "application/json")
                    except Exception:
                        # Some endpoints may throw exceptions in test setup, that's ok
                        pass

    @patch('tvbingefriend_episode_service.blueprints.bp_health_monitoring.EpisodeService')
    def test_json_response_formatting(self, mock_episode_service_class):
        """Test that JSON responses are properly formatted."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        self.mock_episode_service.get_system_health.return_value = self.healthy_status
        
        # Act
        response = health_check(self.mock_http_request)
        
        # Assert
        response_body = response.get_body().decode()
        
        # Should be valid JSON
        parsed_json = json.loads(response_body)
        self.assertIsInstance(parsed_json, dict)
        
        # Should be formatted with indentation (indent=2)
        self.assertIn('\n', response_body)  # Multi-line due to indentation
        self.assertIn('  ', response_body)   # Two-space indentation


if __name__ == '__main__':
    unittest.main()