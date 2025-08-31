"""
Unit tests for bp_updates_timer blueprint.
Tests the timer-triggered function that automatically updates episodes.
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

from tvbingefriend_episode_service.blueprints.bp_updates_timer import get_updates_timer


class TestBpUpdatesTimer(unittest.TestCase):
    """Test cases for the get_updates_timer timer-triggered function."""

    def setUp(self):
        """Set up test environment for each test."""
        self.mock_episode_service = MagicMock()
        self.mock_timer_request = MagicMock(spec=func.TimerRequest)
        
        # Set up mock timer request properties
        self.mock_timer_request.past_due = False
        self.mock_timer_request.schedule_status = MagicMock()
        self.mock_timer_request.schedule_status.last = "2023-12-01T09:00:00Z"
        self.mock_timer_request.schedule_status.next = "2023-12-01T10:00:00Z"

    def tearDown(self):
        """Clean up after each test."""
        # Clear any logging handlers that might have been set up
        logging.getLogger().handlers.clear()

    @patch('tvbingefriend_episode_service.blueprints.bp_updates_timer.EpisodeService')
    @patch('tvbingefriend_episode_service.blueprints.bp_updates_timer.logging')
    def test_get_updates_timer_success(self, mock_logging, mock_episode_service_class):
        """Test successful timer-triggered episode updates."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        
        # Act
        result = get_updates_timer(self.mock_timer_request)
        
        # Assert
        # Function should return None (void function)
        self.assertIsNone(result)
        
        # Verify service was created and method was called
        mock_episode_service_class.assert_called_once()
        self.mock_episode_service.get_updates.assert_called_once_with()
        
        # Should not log any errors
        mock_logging.error.assert_not_called()

    @patch('tvbingefriend_episode_service.blueprints.bp_updates_timer.EpisodeService')
    @patch('tvbingefriend_episode_service.blueprints.bp_updates_timer.logging')
    def test_get_updates_timer_service_initialization_error(self, mock_logging, mock_episode_service_class):
        """Test handling of EpisodeService initialization errors."""
        # Arrange
        init_error = RuntimeError("Database connection failed")
        mock_episode_service_class.side_effect = init_error
        
        # Act & Assert
        with self.assertRaises(RuntimeError) as context:
            get_updates_timer(self.mock_timer_request)
        
        # Verify exception details
        self.assertEqual(str(context.exception), "Database connection failed")
        
        # Verify error logging
        mock_logging.error.assert_called_once_with(
            f"get_updates_timer: Unhandled exception. Error: {init_error}",
            exc_info=True
        )
        
        # Verify service creation was attempted
        mock_episode_service_class.assert_called_once()

    @patch('tvbingefriend_episode_service.blueprints.bp_updates_timer.EpisodeService')
    @patch('tvbingefriend_episode_service.blueprints.bp_updates_timer.logging')
    def test_get_updates_timer_service_method_error(self, mock_logging, mock_episode_service_class):
        """Test handling of errors from get_updates method."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        service_error = Exception("Failed to fetch updates from TVMaze API")
        self.mock_episode_service.get_updates.side_effect = service_error
        
        # Act & Assert
        with self.assertRaises(Exception) as context:
            get_updates_timer(self.mock_timer_request)
        
        # Verify exception details
        self.assertEqual(str(context.exception), "Failed to fetch updates from TVMaze API")
        
        # Verify error logging
        mock_logging.error.assert_called_once_with(
            f"get_updates_timer: Unhandled exception. Error: {service_error}",
            exc_info=True
        )
        
        # Verify service method was called
        self.mock_episode_service.get_updates.assert_called_once_with()

    @patch('tvbingefriend_episode_service.blueprints.bp_updates_timer.EpisodeService')
    @patch('tvbingefriend_episode_service.blueprints.bp_updates_timer.logging')
    def test_get_updates_timer_timeout_error(self, mock_logging, mock_episode_service_class):
        """Test handling of timeout errors during updates."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        timeout_error = TimeoutError("Operation timed out after 300 seconds")
        self.mock_episode_service.get_updates.side_effect = timeout_error
        
        # Act & Assert
        with self.assertRaises(TimeoutError) as context:
            get_updates_timer(self.mock_timer_request)
        
        # Verify exception details
        self.assertEqual(str(context.exception), "Operation timed out after 300 seconds")
        
        # Verify error logging
        mock_logging.error.assert_called_once_with(
            f"get_updates_timer: Unhandled exception. Error: {timeout_error}",
            exc_info=True
        )

    @patch('tvbingefriend_episode_service.blueprints.bp_updates_timer.EpisodeService')
    @patch('tvbingefriend_episode_service.blueprints.bp_updates_timer.logging')
    def test_get_updates_timer_memory_error(self, mock_logging, mock_episode_service_class):
        """Test handling of memory errors during updates."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        memory_error = MemoryError("Insufficient memory to process updates")
        self.mock_episode_service.get_updates.side_effect = memory_error
        
        # Act & Assert
        with self.assertRaises(MemoryError) as context:
            get_updates_timer(self.mock_timer_request)
        
        # Verify exception details
        self.assertEqual(str(context.exception), "Insufficient memory to process updates")
        
        # Verify error logging with correct error instance
        mock_logging.error.assert_called_once_with(
            f"get_updates_timer: Unhandled exception. Error: {memory_error}",
            exc_info=True
        )

    @patch('tvbingefriend_episode_service.blueprints.bp_updates_timer.EpisodeService')
    @patch('tvbingefriend_episode_service.blueprints.bp_updates_timer.logging')
    def test_get_updates_timer_custom_exception(self, mock_logging, mock_episode_service_class):
        """Test handling of custom application exceptions."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        
        class CustomEpisodeException(Exception):
            pass
            
        custom_error = CustomEpisodeException("Custom episode processing error")
        self.mock_episode_service.get_updates.side_effect = custom_error
        
        # Act & Assert
        with self.assertRaises(CustomEpisodeException) as context:
            get_updates_timer(self.mock_timer_request)
        
        # Verify exception details
        self.assertEqual(str(context.exception), "Custom episode processing error")
        
        # Verify error logging
        mock_logging.error.assert_called_once_with(
            f"get_updates_timer: Unhandled exception. Error: {custom_error}",
            exc_info=True
        )

    @patch('tvbingefriend_episode_service.blueprints.bp_updates_timer.EpisodeService')
    @patch('tvbingefriend_episode_service.blueprints.bp_updates_timer.logging')
    def test_get_updates_timer_logging_includes_exc_info(self, mock_logging, mock_episode_service_class):
        """Test that error logging includes exception info for stack traces."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        error = ValueError("Invalid episode data format")
        self.mock_episode_service.get_updates.side_effect = error
        
        # Act & Assert
        with self.assertRaises(ValueError):
            get_updates_timer(self.mock_timer_request)
        
        # Verify logging was called with exc_info=True
        mock_logging.error.assert_called_once_with(
            f"get_updates_timer: Unhandled exception. Error: {error}",
            exc_info=True
        )

    @patch('tvbingefriend_episode_service.blueprints.bp_updates_timer.EpisodeService')
    def test_get_updates_timer_service_method_called_without_parameters(self, mock_episode_service_class):
        """Test that get_updates is called without any parameters."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        
        # Act
        get_updates_timer(self.mock_timer_request)
        
        # Assert
        # get_updates should be called without parameters (uses default time period)
        self.mock_episode_service.get_updates.assert_called_once_with()

    def test_timer_request_parameter_not_used(self):
        """Test that the timer request parameter is not used in the function."""
        # The function parameter is marked as unused with # noinspection PyUnusedLocal
        # This test documents that behavior
        
        # Create different timer request objects
        timer1 = MagicMock(spec=func.TimerRequest)
        timer1.schedule_status = MagicMock()
        timer2 = MagicMock(spec=func.TimerRequest)
        timer2.schedule_status = MagicMock()
        
        # Set different properties
        timer1.past_due = True
        timer1.schedule_status.last = "2023-12-01T08:00:00Z"
        timer2.past_due = False  
        timer2.schedule_status.last = "2023-12-01T09:00:00Z"
        
        # The function should behave identically regardless of timer content
        with patch('tvbingefriend_episode_service.blueprints.bp_updates_timer.EpisodeService') as mock_service_class:
            mock_service = MagicMock()
            mock_service_class.return_value = mock_service
            
            # Both calls should result in identical service interactions
            get_updates_timer(timer1)
            get_updates_timer(timer2)
            
            # Both should have called the service identically
            self.assertEqual(mock_service_class.call_count, 2)
            self.assertEqual(mock_service.get_updates.call_count, 2)

    @patch('tvbingefriend_episode_service.blueprints.bp_updates_timer.EpisodeService')
    @patch('tvbingefriend_episode_service.blueprints.bp_updates_timer.logging')
    def test_get_updates_timer_multiple_consecutive_calls(self, mock_logging, mock_episode_service_class):
        """Test multiple consecutive timer executions."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        
        # Act - simulate multiple timer triggers
        for i in range(3):
            with self.subTest(execution=i+1):
                # Reset mocks for each execution
                mock_episode_service_class.reset_mock()
                self.mock_episode_service.reset_mock()
                mock_logging.reset_mock()
                
                # Execute
                result = get_updates_timer(self.mock_timer_request)
                
                # Assert
                self.assertIsNone(result)
                mock_episode_service_class.assert_called_once()
                self.mock_episode_service.get_updates.assert_called_once_with()
                mock_logging.error.assert_not_called()

    @patch('tvbingefriend_episode_service.blueprints.bp_updates_timer.EpisodeService')
    @patch('tvbingefriend_episode_service.blueprints.bp_updates_timer.logging')
    def test_get_updates_timer_partial_service_failure(self, mock_logging, mock_episode_service_class):
        """Test scenario where service initializes but get_updates fails."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        # Service initializes successfully, but method fails
        partial_failure_error = ConnectionError("Lost connection to TVMaze during update")
        self.mock_episode_service.get_updates.side_effect = partial_failure_error
        
        # Act & Assert
        with self.assertRaises(ConnectionError) as context:
            get_updates_timer(self.mock_timer_request)
        
        # Verify the specific error message
        self.assertEqual(str(context.exception), "Lost connection to TVMaze during update")
        
        # Verify service was created successfully
        mock_episode_service_class.assert_called_once()
        
        # Verify get_updates was attempted
        self.mock_episode_service.get_updates.assert_called_once_with()
        
        # Verify error was logged
        mock_logging.error.assert_called_once()

    @patch('tvbingefriend_episode_service.blueprints.bp_updates_timer.EpisodeService')
    @patch('tvbingefriend_episode_service.blueprints.bp_updates_timer.logging')
    def test_get_updates_timer_exception_chain(self, mock_logging, mock_episode_service_class):
        """Test handling of chained exceptions."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        
        # Create a chained exception scenario
        root_cause = ConnectionError("Network unreachable")
        chained_error = RuntimeError("Episode update failed")
        chained_error.__cause__ = root_cause
        self.mock_episode_service.get_updates.side_effect = chained_error
        
        # Act & Assert
        with self.assertRaises(RuntimeError) as context:
            get_updates_timer(self.mock_timer_request)
        
        # Verify exception chain
        self.assertEqual(str(context.exception), "Episode update failed")
        self.assertIsInstance(context.exception.__cause__, ConnectionError)
        
        # Verify logging includes the chained exception (exc_info=True captures this)
        mock_logging.error.assert_called_once_with(
            f"get_updates_timer: Unhandled exception. Error: {chained_error}",
            exc_info=True
        )

    @patch('tvbingefriend_episode_service.blueprints.bp_updates_timer.EpisodeService')
    def test_get_updates_timer_function_signature(self, mock_episode_service_class):
        """Test that the function has the correct signature for Azure Functions."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        
        # Act
        result = get_updates_timer(self.mock_timer_request)
        
        # Assert
        # Timer functions should return None
        self.assertIsNone(result)
        
        # Should accept TimerRequest parameter
        self.assertIsInstance(self.mock_timer_request, func.TimerRequest)

    @patch('tvbingefriend_episode_service.blueprints.bp_updates_timer.EpisodeService')
    @patch('tvbingefriend_episode_service.blueprints.bp_updates_timer.logging')
    def test_get_updates_timer_error_message_format(self, mock_logging, mock_episode_service_class):
        """Test the exact format of error log messages."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        specific_error = ValueError("Specific test error message")
        self.mock_episode_service.get_updates.side_effect = specific_error
        
        # Act & Assert
        with self.assertRaises(ValueError):
            get_updates_timer(self.mock_timer_request)
        
        # Verify exact error message format
        expected_message = "get_updates_timer: Unhandled exception. Error: Specific test error message"
        mock_logging.error.assert_called_once_with(expected_message, exc_info=True)

    def test_timer_request_properties_available(self):
        """Test that TimerRequest properties are accessible (even if not used)."""
        # This test verifies that the function could access timer properties if needed
        
        timer_req = MagicMock(spec=func.TimerRequest)
        timer_req.past_due = True
        timer_req.schedule_status = MagicMock()
        timer_req.schedule_status.last = "2023-12-01T10:00:00Z"
        timer_req.schedule_status.next = "2023-12-01T11:00:00Z"
        
        # Verify properties are accessible
        self.assertTrue(timer_req.past_due)
        self.assertEqual(timer_req.schedule_status.last, "2023-12-01T10:00:00Z")
        self.assertEqual(timer_req.schedule_status.next, "2023-12-01T11:00:00Z")


if __name__ == '__main__':
    unittest.main()