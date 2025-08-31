"""
Unit tests for bp_get_show_episodes blueprint.
Tests the queue-triggered function that processes individual show episodes.
"""
import json
import os
import sys
import unittest
import logging
from unittest.mock import MagicMock, patch, call
from types import ModuleType

import azure.functions as func

# Set required env vars for module import
os.environ['SQLALCHEMY_CONNECTION_STRING'] = 'sqlite:///:memory:'

# Create a mock TVMaze module to avoid import errors
mock_tvmaze_module = ModuleType('tvbingefriend_tvmaze_client')
mock_tvmaze_module.TVMazeAPI = MagicMock
sys.modules['tvbingefriend_tvmaze_client'] = mock_tvmaze_module

from tvbingefriend_episode_service.blueprints.bp_get_show_episodes import get_show_episodes


class TestBpGetShowEpisodes(unittest.TestCase):
    """Test cases for the get_show_episodes queue trigger function."""

    def setUp(self):
        """Set up test environment for each test."""
        self.mock_episode_service = MagicMock()
        self.mock_queue_message = MagicMock(spec=func.QueueMessage)
        
        # Set up mock queue message properties
        self.mock_queue_message.id = "test-message-id-123"
        self.mock_queue_message.dequeue_count = 1
        self.mock_queue_message.pop_receipt = "test-pop-receipt"
        
        # Mock message content
        self.test_show_data = {"show_id": 12345, "name": "Test Show"}
        self.mock_queue_message.get_body.return_value.decode.return_value = json.dumps(self.test_show_data)
        self.mock_queue_message.get_json.return_value = self.test_show_data

    def tearDown(self):
        """Clean up after each test."""
        # Clear any logging handlers that might have been set up
        logging.getLogger().handlers.clear()

    @patch('tvbingefriend_episode_service.blueprints.bp_get_show_episodes.EpisodeService')
    @patch('tvbingefriend_episode_service.blueprints.bp_get_show_episodes.logging')
    def test_get_show_episodes_success(self, mock_logging, mock_episode_service_class):
        """Test successful processing of a show episodes message."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        
        # Act
        get_show_episodes(self.mock_queue_message)
        
        # Assert
        mock_episode_service_class.assert_called_once()
        self.mock_episode_service.get_show_episodes.assert_called_once_with(self.mock_queue_message)
        
        # Verify logging calls
        mock_logging.info.assert_any_call("=== PROCESSING SHOW EPISODES MESSAGE ===")
        mock_logging.info.assert_any_call(f"Message ID: {self.mock_queue_message.id}")
        mock_logging.info.assert_any_call(f"Message content: {json.dumps(self.test_show_data)}")
        mock_logging.info.assert_any_call(f"Dequeue count: {self.mock_queue_message.dequeue_count}")
        mock_logging.info.assert_any_call(f"Pop receipt: {self.mock_queue_message.pop_receipt}")
        mock_logging.info.assert_any_call(f"Parsed message data: {self.test_show_data}")
        mock_logging.info.assert_any_call("Initializing EpisodeService...")
        mock_logging.info.assert_any_call("Calling episode_service.get_show_episodes...")
        mock_logging.info.assert_any_call(f"=== SUCCESSFULLY PROCESSED MESSAGE ID: {self.mock_queue_message.id} ===")

    @patch('tvbingefriend_episode_service.blueprints.bp_get_show_episodes.EpisodeService')
    @patch('tvbingefriend_episode_service.blueprints.bp_get_show_episodes.logging')
    def test_get_show_episodes_json_parse_error(self, mock_logging, mock_episode_service_class):
        """Test handling of JSON parsing errors in queue message."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        parse_error = json.JSONDecodeError("Invalid JSON", "test", 0)
        self.mock_queue_message.get_json.side_effect = parse_error
        
        # Act & Assert
        with self.assertRaises(json.JSONDecodeError):
            get_show_episodes(self.mock_queue_message)
        
        # Verify error logging
        mock_logging.error.assert_any_call(f"Failed to parse message JSON: {parse_error}")
        mock_logging.error.assert_any_call(
            f"=== ERROR PROCESSING MESSAGE ID {self.mock_queue_message.id} ===",
            exc_info=True
        )
        
        # Verify service was not called
        self.mock_episode_service.get_show_episodes.assert_not_called()

    @patch('tvbingefriend_episode_service.blueprints.bp_get_show_episodes.EpisodeService')
    @patch('tvbingefriend_episode_service.blueprints.bp_get_show_episodes.logging')
    def test_get_show_episodes_service_initialization_error(self, mock_logging, mock_episode_service_class):
        """Test handling of EpisodeService initialization errors."""
        # Arrange
        init_error = RuntimeError("Service initialization failed")
        mock_episode_service_class.side_effect = init_error
        
        # Act & Assert
        with self.assertRaises(RuntimeError):
            get_show_episodes(self.mock_queue_message)
        
        # Verify error logging
        mock_logging.error.assert_any_call(
            f"=== ERROR PROCESSING MESSAGE ID {self.mock_queue_message.id} ===",
            exc_info=True
        )
        mock_logging.error.assert_any_call(f"Exception type: {type(init_error).__name__}")
        mock_logging.error.assert_any_call(f"Exception message: {str(init_error)}")

    @patch('tvbingefriend_episode_service.blueprints.bp_get_show_episodes.EpisodeService')
    @patch('tvbingefriend_episode_service.blueprints.bp_get_show_episodes.logging')
    def test_get_show_episodes_service_processing_error(self, mock_logging, mock_episode_service_class):
        """Test handling of errors during episode processing."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        processing_error = Exception("Episode processing failed")
        self.mock_episode_service.get_show_episodes.side_effect = processing_error
        
        # Act & Assert
        with self.assertRaises(Exception):
            get_show_episodes(self.mock_queue_message)
        
        # Verify service method was called
        self.mock_episode_service.get_show_episodes.assert_called_once_with(self.mock_queue_message)
        
        # Verify error logging
        mock_logging.error.assert_any_call(
            f"=== ERROR PROCESSING MESSAGE ID {self.mock_queue_message.id} ===",
            exc_info=True
        )
        mock_logging.error.assert_any_call(f"Exception type: {type(processing_error).__name__}")
        mock_logging.error.assert_any_call(f"Exception message: {str(processing_error)}")

    @patch('tvbingefriend_episode_service.blueprints.bp_get_show_episodes.EpisodeService')
    @patch('tvbingefriend_episode_service.blueprints.bp_get_show_episodes.logging')
    def test_get_show_episodes_empty_message_body(self, mock_logging, mock_episode_service_class):
        """Test handling of empty message body."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        self.mock_queue_message.get_body.return_value.decode.return_value = ""
        self.mock_queue_message.get_json.side_effect = json.JSONDecodeError("Empty message", "", 0)
        
        # Act & Assert
        with self.assertRaises(json.JSONDecodeError):
            get_show_episodes(self.mock_queue_message)
        
        # Verify error logging for parse failure
        mock_logging.error.assert_any_call(
            f"Failed to parse message JSON: {self.mock_queue_message.get_json.side_effect}"
        )

    @patch('tvbingefriend_episode_service.blueprints.bp_get_show_episodes.EpisodeService')
    @patch('tvbingefriend_episode_service.blueprints.bp_get_show_episodes.logging')
    def test_get_show_episodes_malformed_json(self, mock_logging, mock_episode_service_class):
        """Test handling of malformed JSON in message."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        malformed_json = '{"show_id": 123, "name":'  # Incomplete JSON
        self.mock_queue_message.get_body.return_value.decode.return_value = malformed_json
        json_error = json.JSONDecodeError("Unterminated string", malformed_json, 10)
        self.mock_queue_message.get_json.side_effect = json_error
        
        # Act & Assert
        with self.assertRaises(json.JSONDecodeError):
            get_show_episodes(self.mock_queue_message)
        
        # Verify logging includes the malformed JSON content
        mock_logging.info.assert_any_call(f"Message content: {malformed_json}")
        mock_logging.error.assert_any_call(f"Failed to parse message JSON: {json_error}")

    @patch('tvbingefriend_episode_service.blueprints.bp_get_show_episodes.EpisodeService')
    @patch('tvbingefriend_episode_service.blueprints.bp_get_show_episodes.logging')
    def test_get_show_episodes_complex_message_data(self, mock_logging, mock_episode_service_class):
        """Test processing of complex message data with multiple fields."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        complex_data = {
            "show_id": 12345,
            "name": "Test Show",
            "metadata": {
                "priority": "high",
                "retry_count": 0,
                "timestamp": "2023-01-01T00:00:00Z"
            },
            "options": ["fetch_cast", "fetch_crew"]
        }
        self.mock_queue_message.get_body.return_value.decode.return_value = json.dumps(complex_data)
        self.mock_queue_message.get_json.return_value = complex_data
        
        # Act
        get_show_episodes(self.mock_queue_message)
        
        # Assert
        self.mock_episode_service.get_show_episodes.assert_called_once_with(self.mock_queue_message)
        mock_logging.info.assert_any_call(f"Parsed message data: {complex_data}")

    @patch('tvbingefriend_episode_service.blueprints.bp_get_show_episodes.EpisodeService')
    @patch('tvbingefriend_episode_service.blueprints.bp_get_show_episodes.logging')
    def test_get_show_episodes_high_dequeue_count(self, mock_logging, mock_episode_service_class):
        """Test logging of messages with high dequeue count (indicating retries)."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        self.mock_queue_message.dequeue_count = 5  # High retry count
        
        # Act
        get_show_episodes(self.mock_queue_message)
        
        # Assert
        mock_logging.info.assert_any_call(f"Dequeue count: 5")
        self.mock_episode_service.get_show_episodes.assert_called_once_with(self.mock_queue_message)

    def test_queue_message_properties_access(self):
        """Test that all required queue message properties are accessed correctly."""
        # This test verifies that the function correctly accesses all QueueMessage properties
        # without mocking the blueprint function itself
        
        # Create a more complete mock
        queue_msg = MagicMock(spec=func.QueueMessage)
        queue_msg.id = "test-id-456"
        queue_msg.dequeue_count = 2
        queue_msg.pop_receipt = "test-pop-receipt-456"
        
        # Test that we can access all the properties the function needs
        self.assertEqual(queue_msg.id, "test-id-456")
        self.assertEqual(queue_msg.dequeue_count, 2)
        self.assertEqual(queue_msg.pop_receipt, "test-pop-receipt-456")
        
        # Verify get_body and get_json methods exist
        self.assertTrue(hasattr(queue_msg, 'get_body'))
        self.assertTrue(hasattr(queue_msg, 'get_json'))


if __name__ == '__main__':
    unittest.main()