import unittest
from unittest.mock import MagicMock, patch

from sqlalchemy.exc import SQLAlchemyError

from tvbingefriend_episode_service.repos.episode_repo import EpisodeRepository


class TestEpisodeRepository(unittest.TestCase):

    def setUp(self):
        self.repo = EpisodeRepository()
        self.mock_db_session = MagicMock()

    @patch('tvbingefriend_episode_service.repos.episode_repo.inspect')
    @patch('tvbingefriend_episode_service.repos.episode_repo.mysql_insert')
    def test_upsert_episode_success(self, mock_mysql_insert, mock_inspect):
        """Test successful episode upsert."""
        mock_mapper = MagicMock()
        mock_prop1 = MagicMock()
        mock_prop1.key = 'id'
        mock_prop2 = MagicMock()
        mock_prop2.key = 'name'
        mock_prop3 = MagicMock()
        mock_prop3.key = 'show_id'
        mock_mapper.attrs.values.return_value = [mock_prop1, mock_prop2, mock_prop3]
        mock_inspect.return_value = mock_mapper

        episode_data = {"id": 1, "name": "Pilot", "season": 1, "number": 1}
        show_id = 123
        self.repo.upsert_episode(episode_data, show_id, self.mock_db_session)

        mock_mysql_insert.assert_called_once()
        self.mock_db_session.execute.assert_called_once()
        self.mock_db_session.flush.assert_called_once()

    def test_upsert_episode_no_id(self):
        """Test episode upsert when episode has no ID."""
        episode_data = {"name": "Pilot", "season": 1, "number": 1}
        show_id = 123
        
        with patch('tvbingefriend_episode_service.repos.episode_repo.logging') as mock_logging:
            self.repo.upsert_episode(episode_data, show_id, self.mock_db_session)

        self.mock_db_session.execute.assert_not_called()
        mock_logging.error.assert_called_once()

    @patch('tvbingefriend_episode_service.repos.episode_repo.inspect')
    @patch('tvbingefriend_episode_service.repos.episode_repo.mysql_insert')
    def test_upsert_episode_with_show_id_mapping(self, mock_mysql_insert, mock_inspect):
        """Test that show_id is correctly mapped to episode data."""
        mock_mapper = MagicMock()
        mock_prop = MagicMock()
        mock_prop.key = 'show_id'
        mock_mapper.attrs.values.return_value = [mock_prop]
        mock_inspect.return_value = mock_mapper

        mock_stmt = MagicMock()
        mock_mysql_insert.return_value = mock_stmt
        mock_stmt.on_duplicate_key_update.return_value = mock_stmt

        episode_data = {"id": 1, "name": "Pilot"}
        show_id = 456
        self.repo.upsert_episode(episode_data, show_id, self.mock_db_session)

        # Verify the insert statement was called with show_id
        mock_mysql_insert.assert_called_once()
        insert_call_args = mock_mysql_insert.call_args[0]  # First positional argument
        # Check that values() was called and show_id was in the insert values
        values_call = mock_stmt.values.call_args
        if values_call:
            insert_values = values_call[0][0] if values_call[0] else values_call[1] if len(values_call) > 1 else {}
            self.assertEqual(insert_values.get('show_id'), show_id)

    @patch('tvbingefriend_episode_service.repos.episode_repo.logging')
    @patch('tvbingefriend_episode_service.repos.episode_repo.inspect')
    @patch('tvbingefriend_episode_service.repos.episode_repo.mysql_insert')
    def test_upsert_episode_sqlalchemy_error_in_execute(self, mock_mysql_insert, mock_inspect, mock_logging):
        """Test SQLAlchemy error during statement execution."""
        mock_mapper = MagicMock()
        mock_prop = MagicMock()
        mock_prop.key = 'id'
        mock_mapper.attrs.values.return_value = [mock_prop]
        mock_inspect.return_value = mock_mapper
        
        # Mock execute to raise SQLAlchemyError
        self.mock_db_session.execute.side_effect = SQLAlchemyError("Execute failed")
        
        episode_data = {"id": 1, "name": "Pilot"}
        show_id = 123
        self.repo.upsert_episode(episode_data, show_id, self.mock_db_session)
        
        # Should log the error but not raise it
        mock_logging.error.assert_called()
        error_call = mock_logging.error.call_args[0][0]
        self.assertIn("Database error during upsert of episode_id 1", error_call)

    @patch('tvbingefriend_episode_service.repos.episode_repo.logging')
    @patch('tvbingefriend_episode_service.repos.episode_repo.inspect')
    @patch('tvbingefriend_episode_service.repos.episode_repo.mysql_insert')
    def test_upsert_episode_general_exception_in_execute(self, mock_mysql_insert, mock_inspect, mock_logging):
        """Test general exception during statement execution."""
        mock_mapper = MagicMock()
        mock_prop = MagicMock()
        mock_prop.key = 'id'
        mock_mapper.attrs.values.return_value = [mock_prop]
        mock_inspect.return_value = mock_mapper
        
        # Mock execute to raise general Exception
        self.mock_db_session.execute.side_effect = Exception("Unexpected execute error")
        
        episode_data = {"id": 1, "name": "Pilot"}
        show_id = 123
        self.repo.upsert_episode(episode_data, show_id, self.mock_db_session)
        
        # Should log the error but not raise it
        mock_logging.error.assert_called()
        error_call = mock_logging.error.call_args[0][0]
        self.assertIn("Unexpected error during upsert of episode episode_id 1", error_call)

    @patch('tvbingefriend_episode_service.repos.episode_repo.inspect')
    @patch('tvbingefriend_episode_service.repos.episode_repo.mysql_insert')
    def test_upsert_episode_filters_columns(self, mock_mysql_insert, mock_inspect):
        """Test that only valid columns are included in insert values."""
        mock_mapper = MagicMock()
        mock_prop1 = MagicMock()
        mock_prop1.key = 'id'
        mock_prop2 = MagicMock() 
        mock_prop2.key = 'name'
        mock_prop3 = MagicMock()
        mock_prop3.key = 'season'
        mock_mapper.attrs.values.return_value = [mock_prop1, mock_prop2, mock_prop3]
        mock_inspect.return_value = mock_mapper

        mock_stmt = MagicMock()
        mock_mysql_insert.return_value = mock_stmt

        episode_data = {"id": 1, "name": "Pilot", "season": 1, "invalid_field": "should_be_filtered"}
        show_id = 123
        self.repo.upsert_episode(episode_data, show_id, self.mock_db_session)

        # Verify mysql_insert was called
        mock_mysql_insert.assert_called_once()

    @patch('tvbingefriend_episode_service.repos.episode_repo.inspect')
    @patch('tvbingefriend_episode_service.repos.episode_repo.mysql_insert')
    def test_upsert_episode_with_all_episode_fields(self, mock_mysql_insert, mock_inspect):
        """Test episode upsert with episode-specific fields."""
        mock_mapper = MagicMock()
        mock_props = []
        fields = ['id', 'show_id', 'name', 'season', 'number', 'type', 'airdate', 'airtime', 'runtime', 'rating', 'summary']
        for field in fields:
            prop = MagicMock()
            prop.key = field
            mock_props.append(prop)
        mock_mapper.attrs.values.return_value = mock_props
        mock_inspect.return_value = mock_mapper

        mock_stmt = MagicMock()
        mock_mysql_insert.return_value = mock_stmt

        episode_data = {
            "id": 12345,
            "name": "The Pilot",
            "season": 1,
            "number": 1,
            "type": "regular",
            "airdate": "2023-01-01",
            "airtime": "21:00",
            "runtime": 60,
            "rating": {"average": 8.5},
            "summary": "<p>The pilot episode</p>"
        }
        show_id = 456
        self.repo.upsert_episode(episode_data, show_id, self.mock_db_session)

        mock_mysql_insert.assert_called_once()
        self.mock_db_session.execute.assert_called_once()
        self.mock_db_session.flush.assert_called_once()


if __name__ == '__main__':
    unittest.main()