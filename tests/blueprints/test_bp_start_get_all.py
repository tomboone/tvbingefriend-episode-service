"""
Unit tests for bp_start_get_all blueprint.
Tests the HTTP endpoint that initiates bulk episode processing for all shows.
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

from tvbingefriend_episode_service.blueprints.bp_start_get_all import start_get_all


class TestBpStartGetAll(unittest.TestCase):
    """Test cases for the start_get_all HTTP endpoint function."""

    def setUp(self):
        """Set up test environment for each test."""
        self.mock_episode_service = MagicMock()
        self.mock_http_request = MagicMock(spec=func.HttpRequest)
        
        # Default import ID for successful operations
        self.test_import_id = "import-12345-abcde"

    def tearDown(self):
        """Clean up after each test."""
        pass

    @patch('tvbingefriend_episode_service.blueprints.bp_start_get_all.EpisodeService')
    def test_start_get_all_success(self, mock_episode_service_class):
        """Test successful initiation of bulk episode processing."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        self.mock_episode_service.start_get_all_shows_episodes.return_value = self.test_import_id
        
        # Act
        response = start_get_all(self.mock_http_request)
        
        # Assert
        mock_episode_service_class.assert_called_once()
        self.mock_episode_service.start_get_all_shows_episodes.assert_called_once()
        
        # Verify response
        self.assertIsInstance(response, func.HttpResponse)
        self.assertEqual(response.status_code, 202)
        expected_message = f"Getting all episodes from TV Maze for all shows. Import ID: {self.test_import_id}"
        self.assertEqual(response.get_body().decode(), expected_message)

    @patch('tvbingefriend_episode_service.blueprints.bp_start_get_all.EpisodeService')
    def test_start_get_all_service_initialization_error(self, mock_episode_service_class):
        """Test handling of EpisodeService initialization errors."""
        # Arrange
        init_error = RuntimeError("Database connection failed")
        mock_episode_service_class.side_effect = init_error
        
        # Act & Assert
        with self.assertRaises(RuntimeError) as context:
            start_get_all(self.mock_http_request)
        
        self.assertEqual(str(context.exception), "Database connection failed")
        mock_episode_service_class.assert_called_once()

    @patch('tvbingefriend_episode_service.blueprints.bp_start_get_all.EpisodeService')
    def test_start_get_all_service_method_error(self, mock_episode_service_class):
        """Test handling of errors from start_get_all_shows_episodes method."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        service_error = Exception("Failed to queue shows for processing")
        self.mock_episode_service.start_get_all_shows_episodes.side_effect = service_error
        
        # Act & Assert
        with self.assertRaises(Exception) as context:
            start_get_all(self.mock_http_request)
        
        self.assertEqual(str(context.exception), "Failed to queue shows for processing")
        self.mock_episode_service.start_get_all_shows_episodes.assert_called_once()

    @patch('tvbingefriend_episode_service.blueprints.bp_start_get_all.EpisodeService')
    def test_start_get_all_with_none_import_id(self, mock_episode_service_class):
        """Test handling when service returns None as import ID."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        self.mock_episode_service.start_get_all_shows_episodes.return_value = None
        
        # Act
        response = start_get_all(self.mock_http_request)
        
        # Assert
        self.assertIsInstance(response, func.HttpResponse)
        self.assertEqual(response.status_code, 202)
        expected_message = "Getting all episodes from TV Maze for all shows. Import ID: None"
        self.assertEqual(response.get_body().decode(), expected_message)

    @patch('tvbingefriend_episode_service.blueprints.bp_start_get_all.EpisodeService')
    def test_start_get_all_with_empty_string_import_id(self, mock_episode_service_class):
        """Test handling when service returns empty string as import ID."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        self.mock_episode_service.start_get_all_shows_episodes.return_value = ""
        
        # Act
        response = start_get_all(self.mock_http_request)
        
        # Assert
        self.assertIsInstance(response, func.HttpResponse)
        self.assertEqual(response.status_code, 202)
        expected_message = "Getting all episodes from TV Maze for all shows. Import ID: "
        self.assertEqual(response.get_body().decode(), expected_message)

    @patch('tvbingefriend_episode_service.blueprints.bp_start_get_all.EpisodeService')
    def test_start_get_all_with_complex_import_id(self, mock_episode_service_class):
        """Test handling of complex import ID formats."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        complex_import_id = "bulk-import-2023-12-01T10:30:00Z-uuid-12345-retry-0"
        self.mock_episode_service.start_get_all_shows_episodes.return_value = complex_import_id
        
        # Act
        response = start_get_all(self.mock_http_request)
        
        # Assert
        self.assertIsInstance(response, func.HttpResponse)
        self.assertEqual(response.status_code, 202)
        expected_message = f"Getting all episodes from TV Maze for all shows. Import ID: {complex_import_id}"
        self.assertEqual(response.get_body().decode(), expected_message)

    @patch('tvbingefriend_episode_service.blueprints.bp_start_get_all.EpisodeService')
    def test_start_get_all_multiple_calls(self, mock_episode_service_class):
        """Test multiple consecutive calls to the endpoint."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        import_ids = ["import-1", "import-2", "import-3"]
        self.mock_episode_service.start_get_all_shows_episodes.side_effect = import_ids
        
        # Act & Assert
        for i, expected_id in enumerate(import_ids):
            with self.subTest(call_number=i+1):
                response = start_get_all(self.mock_http_request)
                
                self.assertEqual(response.status_code, 202)
                expected_message = f"Getting all episodes from TV Maze for all shows. Import ID: {expected_id}"
                self.assertEqual(response.get_body().decode(), expected_message)
        
        # Verify service was created multiple times (once per call)
        self.assertEqual(mock_episode_service_class.call_count, 3)
        self.assertEqual(self.mock_episode_service.start_get_all_shows_episodes.call_count, 3)

    @patch('tvbingefriend_episode_service.blueprints.bp_start_get_all.EpisodeService')
    def test_start_get_all_response_format(self, mock_episode_service_class):
        """Test the exact format and type of the HTTP response."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        self.mock_episode_service.start_get_all_shows_episodes.return_value = self.test_import_id
        
        # Act
        response = start_get_all(self.mock_http_request)
        
        # Assert response properties
        self.assertIsInstance(response, func.HttpResponse)
        self.assertEqual(response.status_code, 202)  # Accepted status
        self.assertEqual(response.charset, 'utf-8')  # Default charset
        
        # Verify response body is a string
        body = response.get_body()
        self.assertIsInstance(body, bytes)
        
        # Verify decoded body format
        decoded_body = body.decode()
        self.assertIsInstance(decoded_body, str)
        self.assertTrue(decoded_body.startswith("Getting all episodes from TV Maze"))
        self.assertIn("Import ID:", decoded_body)
        self.assertIn(self.test_import_id, decoded_body)

    def test_http_request_object_not_used(self):
        """Test that the HTTP request object is not actually used in the function."""
        # This test documents the behavior that the req parameter is marked as unused
        # The function signature includes it but doesn't use it
        
        # Create different request objects
        req1 = MagicMock(spec=func.HttpRequest)
        req2 = MagicMock(spec=func.HttpRequest)
        
        # Set different properties
        req1.method = "POST"
        req1.url = "http://example.com/start_get_episodes"
        req2.method = "GET" 
        req2.url = "http://different.com/other"
        
        # The function should behave identically regardless of request content
        # This is verified by the fact that the function doesn't access any req properties
        with patch('tvbingefriend_episode_service.blueprints.bp_start_get_all.EpisodeService') as mock_service_class:
            mock_service = MagicMock()
            mock_service_class.return_value = mock_service
            mock_service.start_get_all_shows_episodes.return_value = "test-id"
            
            response1 = start_get_all(req1)
            response2 = start_get_all(req2)
            
            # Both responses should be identical
            self.assertEqual(response1.status_code, response2.status_code)
            self.assertEqual(response1.get_body(), response2.get_body())

    @patch('tvbingefriend_episode_service.blueprints.bp_start_get_all.EpisodeService')
    def test_start_get_all_service_method_timeout(self, mock_episode_service_class):
        """Test handling of timeout errors from the service method."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        timeout_error = TimeoutError("Operation timed out after 30 seconds")
        self.mock_episode_service.start_get_all_shows_episodes.side_effect = timeout_error
        
        # Act & Assert
        with self.assertRaises(TimeoutError) as context:
            start_get_all(self.mock_http_request)
        
        self.assertEqual(str(context.exception), "Operation timed out after 30 seconds")
        self.mock_episode_service.start_get_all_shows_episodes.assert_called_once()

    @patch('tvbingefriend_episode_service.blueprints.bp_start_get_all.EpisodeService')
    def test_start_get_all_memory_error(self, mock_episode_service_class):
        """Test handling of memory errors during bulk processing initiation."""
        # Arrange
        mock_episode_service_class.return_value = self.mock_episode_service
        memory_error = MemoryError("Insufficient memory to queue all shows")
        self.mock_episode_service.start_get_all_shows_episodes.side_effect = memory_error
        
        # Act & Assert
        with self.assertRaises(MemoryError) as context:
            start_get_all(self.mock_http_request)
        
        self.assertEqual(str(context.exception), "Insufficient memory to queue all shows")


if __name__ == '__main__':
    unittest.main()