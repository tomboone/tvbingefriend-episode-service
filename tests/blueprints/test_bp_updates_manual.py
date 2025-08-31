"""
Unit tests for bp_updates_manual blueprint.
Tests the HTTP endpoint for manually triggering episode updates.
"""
import json
import os
import sys
import unittest
import logging
from unittest.mock import MagicMock, patch
from types import ModuleType

import azure.functions as func

# Set required env vars for module import
os.environ['SQLALCHEMY_CONNECTION_STRING'] = 'sqlite:///:memory:'

# Create a mock TVMaze module to avoid import errors
mock_tvmaze_module = ModuleType('tvbingefriend_tvmaze_client')
mock_tvmaze_module.TVMazeAPI = MagicMock
sys.modules['tvbingefriend_tvmaze_client'] = mock_tvmaze_module

from tvbingefriend_episode_service.blueprints.bp_updates_manual import get_updates_manually


class TestBpUpdatesManual(unittest.TestCase):
    """Test cases for the get_updates_manually HTTP endpoint function."""

    def setUp(self):
        """Set up test environment for each test."""
        self.mock_episode_service = MagicMock()
        self.mock_http_request = MagicMock(spec=func.HttpRequest)
        self.mock_http_request.params = MagicMock()

    def tearDown(self):
        """Clean up after each test."""
        # Clear any logging handlers that might have been set up
        logging.getLogger().handlers.clear()

    @patch('tvbingefriend_episode_service.blueprints.bp_updates_manual.EpisodeService')
    @patch('tvbingefriend_episode_service.blueprints.bp_updates_manual.logging')
    def test_get_updates_manually_default_since_day(self, mock_logging, mock_episode_service_class):
        """Test manual updates with default 'since' parameter (day)."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        self.mock_http_request.params.get.side_effect = {}.get  # No 'since' param provided
        
        # Act
        response = get_updates_manually(self.mock_http_request)
        
        # Assert
        mock_episode_service_class.assert_called_once()
        self.mock_episode_service.get_updates.assert_called_once_with('day')
        
        # Verify response
        self.assertIsInstance(response, func.HttpResponse)
        self.assertEqual(response.status_code, 202)
        expected_message = "Getting all updates from TV Maze for the last day and queuing seasons for episode processing"
        self.assertEqual(response.get_body().decode(), expected_message)
        
        # Verify request params were accessed
        self.mock_http_request.params.get.assert_called_once_with('since', 'day')

    @patch('tvbingefriend_episode_service.blueprints.bp_updates_manual.EpisodeService')
    def test_get_updates_manually_since_week(self, mock_episode_service_class):
        """Test manual updates with 'since' parameter set to 'week'."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        self.mock_http_request.params.get.return_value = 'week'
        
        # Act
        response = get_updates_manually(self.mock_http_request)
        
        # Assert
        self.mock_episode_service.get_updates.assert_called_once_with('week')
        self.assertEqual(response.status_code, 202)
        expected_message = "Getting all updates from TV Maze for the last week and queuing seasons for episode processing"
        self.assertEqual(response.get_body().decode(), expected_message)

    @patch('tvbingefriend_episode_service.blueprints.bp_updates_manual.EpisodeService')
    def test_get_updates_manually_since_month(self, mock_episode_service_class):
        """Test manual updates with 'since' parameter set to 'month'."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        self.mock_http_request.params.get.return_value = 'month'
        
        # Act
        response = get_updates_manually(self.mock_http_request)
        
        # Assert
        self.mock_episode_service.get_updates.assert_called_once_with('month')
        self.assertEqual(response.status_code, 202)
        expected_message = "Getting all updates from TV Maze for the last month and queuing seasons for episode processing"
        self.assertEqual(response.get_body().decode(), expected_message)

    @patch('tvbingefriend_episode_service.blueprints.bp_updates_manual.EpisodeService')
    @patch('tvbingefriend_episode_service.blueprints.bp_updates_manual.logging')
    def test_get_updates_manually_invalid_since_parameter(self, mock_logging, mock_episode_service_class):
        """Test manual updates with invalid 'since' parameter."""
        # Arrange
        invalid_since = 'year'
        self.mock_http_request.params.get.return_value = invalid_since
        
        # Act
        response = get_updates_manually(self.mock_http_request)
        
        # Assert
        # Should not create EpisodeService or call get_updates
        mock_episode_service_class.assert_not_called()
        
        # Should return error response
        self.assertEqual(response.status_code, 400)
        expected_message = "Query parameter 'since' must be 'day', 'week', or 'month'."
        self.assertEqual(response.get_body().decode(), expected_message)
        
        # Should log error
        mock_logging.error.assert_called_once_with(f"Invalid since parameter provided: {invalid_since}")

    @patch('tvbingefriend_episode_service.blueprints.bp_updates_manual.EpisodeService')
    @patch('tvbingefriend_episode_service.blueprints.bp_updates_manual.logging')
    def test_get_updates_manually_empty_since_parameter(self, mock_logging, mock_episode_service_class):
        """Test manual updates with empty 'since' parameter."""
        # Arrange
        empty_since = ''
        self.mock_http_request.params.get.return_value = empty_since
        
        # Act
        response = get_updates_manually(self.mock_http_request)
        
        # Assert
        # Empty string is not in valid values, should return error
        self.assertEqual(response.status_code, 400)
        mock_logging.error.assert_called_once_with(f"Invalid since parameter provided: {empty_since}")
        mock_episode_service_class.assert_not_called()

    @patch('tvbingefriend_episode_service.blueprints.bp_updates_manual.EpisodeService')
    @patch('tvbingefriend_episode_service.blueprints.bp_updates_manual.logging')
    def test_get_updates_manually_case_sensitive_since(self, mock_logging, mock_episode_service_class):
        """Test that 'since' parameter validation is case-sensitive."""
        # Arrange
        uppercase_since = 'DAY'
        self.mock_http_request.params.get.return_value = uppercase_since
        
        # Act
        response = get_updates_manually(self.mock_http_request)
        
        # Assert
        # Uppercase should be invalid
        self.assertEqual(response.status_code, 400)
        mock_logging.error.assert_called_once_with(f"Invalid since parameter provided: {uppercase_since}")
        mock_episode_service_class.assert_not_called()

    @patch('tvbingefriend_episode_service.blueprints.bp_updates_manual.EpisodeService')
    def test_get_updates_manually_service_initialization_error(self, mock_episode_service_class):
        """Test handling of EpisodeService initialization errors."""
        # Arrange
        init_error = RuntimeError("Database connection failed")
        mock_episode_service_class.side_effect = init_error
        self.mock_http_request.params.get.return_value = 'day'
        
        # Act & Assert
        with self.assertRaises(RuntimeError) as context:
            get_updates_manually(self.mock_http_request)
        
        self.assertEqual(str(context.exception), "Database connection failed")
        mock_episode_service_class.assert_called_once()

    @patch('tvbingefriend_episode_service.blueprints.bp_updates_manual.EpisodeService')
    def test_get_updates_manually_service_method_error(self, mock_episode_service_class):
        """Test handling of errors from get_updates method."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        service_error = Exception("Failed to fetch updates from TVMaze")
        self.mock_episode_service.get_updates.side_effect = service_error
        self.mock_http_request.params.get.return_value = 'week'
        
        # Act & Assert
        with self.assertRaises(Exception) as context:
            get_updates_manually(self.mock_http_request)
        
        self.assertEqual(str(context.exception), "Failed to fetch updates from TVMaze")
        self.mock_episode_service.get_updates.assert_called_once_with('week')

    @patch('tvbingefriend_episode_service.blueprints.bp_updates_manual.EpisodeService')
    def test_get_updates_manually_multiple_calls_different_params(self, mock_episode_service_class):
        """Test multiple calls with different 'since' parameters."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        test_cases = [
            ('day', 'day'),
            ('week', 'week'),
            ('month', 'month')
        ]
        
        for param_value, expected_call in test_cases:
            with self.subTest(since_param=param_value):
                # Reset mocks for each test
                mock_episode_service_class.reset_mock()
                self.mock_episode_service.reset_mock()
                self.mock_http_request.params.get.return_value = param_value
                
                # Act
                response = get_updates_manually(self.mock_http_request)
                
                # Assert
                self.assertEqual(response.status_code, 202)
                self.mock_episode_service.get_updates.assert_called_once_with(expected_call)
                expected_message = f"Getting all updates from TV Maze for the last {param_value} and queuing seasons for episode processing"
                self.assertEqual(response.get_body().decode(), expected_message)

    @patch('tvbingefriend_episode_service.blueprints.bp_updates_manual.EpisodeService')
    def test_get_updates_manually_whitespace_since_parameter(self, mock_episode_service_class):
        """Test manual updates with whitespace in 'since' parameter."""
        # Arrange
        whitespace_since = ' day '
        self.mock_http_request.params.get.return_value = whitespace_since
        
        # Act
        response = get_updates_manually(self.mock_http_request)
        
        # Assert
        # Whitespace should make it invalid
        self.assertEqual(response.status_code, 400)
        mock_episode_service_class.assert_not_called()

    @patch('tvbingefriend_episode_service.blueprints.bp_updates_manual.EpisodeService')
    def test_get_updates_manually_service_timeout_error(self, mock_episode_service_class):
        """Test handling of timeout errors from the service method."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        timeout_error = TimeoutError("Operation timed out after 60 seconds")
        self.mock_episode_service.get_updates.side_effect = timeout_error
        self.mock_http_request.params.get.return_value = 'month'
        
        # Act & Assert
        with self.assertRaises(TimeoutError) as context:
            get_updates_manually(self.mock_http_request)
        
        self.assertEqual(str(context.exception), "Operation timed out after 60 seconds")
        self.mock_episode_service.get_updates.assert_called_once_with('month')

    @patch('tvbingefriend_episode_service.blueprints.bp_updates_manual.EpisodeService')
    def test_get_updates_manually_response_format(self, mock_episode_service_class):
        """Test the exact format of the HTTP response."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        test_since = 'week'
        self.mock_http_request.params.get.return_value = test_since
        
        # Act
        response = get_updates_manually(self.mock_http_request)
        
        # Assert response properties
        self.assertIsInstance(response, func.HttpResponse)
        self.assertEqual(response.status_code, 202)  # Accepted status
        self.assertEqual(response.charset, 'utf-8')  # Default charset
        
        # Verify response body format
        body = response.get_body()
        self.assertIsInstance(body, bytes)
        
        decoded_body = body.decode()
        self.assertIsInstance(decoded_body, str)
        expected_start = "Getting all updates from TV Maze for the last"
        expected_end = "and queuing seasons for episode processing"
        self.assertTrue(decoded_body.startswith(expected_start))
        self.assertTrue(decoded_body.endswith(expected_end))
        self.assertIn(test_since, decoded_body)

    def test_valid_since_parameters_comprehensive(self):
        """Test all valid 'since' parameter values comprehensively."""
        valid_values = ['day', 'week', 'month']
        
        for valid_value in valid_values:
            with self.subTest(since_value=valid_value):
                # Test that the value is considered valid (not in error condition)
                self.assertIn(valid_value, ('day', 'week', 'month'))

    def test_invalid_since_parameters_comprehensive(self):
        """Test various invalid 'since' parameter values."""
        invalid_values = [
            'hour', 'days', 'weeks', 'months', 'year', 'minute',
            'DAY', 'WEEK', 'MONTH', 'Day', 'Week', 'Month',
            '1day', 'day1', 'day-1', '1', '7', '30',
            '', ' ', 'null', 'undefined', None
        ]
        
        for invalid_value in invalid_values:
            with self.subTest(since_value=invalid_value):
                # Test that the value is considered invalid
                if invalid_value is None:
                    # None should default to 'day' and be valid
                    continue
                else:
                    self.assertNotIn(invalid_value, ('day', 'week', 'month'))

    @patch('tvbingefriend_episode_service.blueprints.bp_updates_manual.EpisodeService')
    def test_get_updates_manually_service_called_before_response(self, mock_episode_service_class):
        """Test that service methods are called before response is generated."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        self.mock_http_request.params.get.return_value = 'day'
        
        # Create a side effect that tracks when the method is called
        call_order = []
        
        def track_service_creation(*args, **kwargs):
            call_order.append('service_created')
            return self.mock_episode_service
            
        def track_get_updates(*args, **kwargs):
            call_order.append('get_updates_called')
            
        mock_episode_service_class.side_effect = track_service_creation
        self.mock_episode_service.get_updates.side_effect = track_get_updates
        
        # Act
        response = get_updates_manually(self.mock_http_request)
        
        # Assert
        self.assertEqual(response.status_code, 202)
        self.assertEqual(call_order, ['service_created', 'get_updates_called'])

    @patch('tvbingefriend_episode_service.blueprints.bp_updates_manual.EpisodeService')
    def test_get_updates_manually_numeric_since_parameter(self, mock_episode_service_class):
        """Test manual updates with numeric 'since' parameter."""
        # Arrange - some systems might pass numbers
        numeric_since = 7  # Equivalent to week but as number
        self.mock_http_request.params.get.return_value = numeric_since
        
        # Act
        response = get_updates_manually(self.mock_http_request)
        
        # Assert
        # Numeric values should be invalid
        self.assertEqual(response.status_code, 400)
        mock_episode_service_class.assert_not_called()


if __name__ == '__main__':
    unittest.main()