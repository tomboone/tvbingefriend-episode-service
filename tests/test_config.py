import os
import unittest
from unittest.mock import patch

# Set required env vars for module import
os.environ['SQLALCHEMY_CONNECTION_STRING'] = 'mysql://test_user:test_pass@test_host:3306/test_db'

# This needs to be imported after the environment is patched
from tvbingefriend_episode_service.config import _get_setting, SQLALCHEMY_CONNECTION_STRING


class TestConfig(unittest.TestCase):

    @patch.dict(os.environ, {'SQLALCHEMY_CONNECTION_STRING': 'mysql://test_user:test_pass@test_host:3306/test_db'})
    def test_db_connection_string_loaded_from_env(self):
        """Check if the DB connection string is loaded correctly from the environment."""
        # Import after patching to get the patched value
        from tvbingefriend_episode_service.config import _get_setting
        result = _get_setting('SQLALCHEMY_CONNECTION_STRING')
        self.assertEqual(result, 'mysql://test_user:test_pass@test_host:3306/test_db')

    @patch.dict(os.environ, {'TEST_VAR': 'test_value'})
    def test_get_setting_found_in_env(self):
        """Test _get_setting when a variable is present in the environment."""
        self.assertEqual(_get_setting('TEST_VAR'), 'test_value')

    @patch('tvbingefriend_episode_service.config._local_settings', {'TEST_VAR': 'local_value'})
    def test_get_setting_fallback_to_local_settings(self):
        """Test _get_setting falls back to local settings when env var is missing."""
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(_get_setting('TEST_VAR'), 'local_value')

    def test_get_setting_missing_required(self):
        """Test _get_setting raises ValueError when a required variable is missing."""
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "Missing required setting: 'MISSING_VAR'"):
                _get_setting('MISSING_VAR', required=True)

    def test_get_setting_missing_not_required(self):
        """Test _get_setting returns None when a non-required variable is missing."""
        with patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(_get_setting('MISSING_VAR', required=False))

    def test_get_setting_missing_not_required_with_default(self):
        """Test _get_setting returns the default value for a missing non-required variable."""
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(_get_setting('MISSING_VAR', required=False, default='default_val'), 'default_val')

    @patch.dict(os.environ, {'INDEX_QUEUE': 'test-episodes-queue'})
    def test_episodes_queue_config(self):
        """Test EPISODES_QUEUE configuration loading."""
        # Import after patching to get the patched value
        import importlib
        import tvbingefriend_episode_service.config
        importlib.reload(tvbingefriend_episode_service.config)
        from tvbingefriend_episode_service.config import EPISODES_QUEUE
        # Note: EPISODES_QUEUE uses INDEX_QUEUE env var as configured
        self.assertEqual(EPISODES_QUEUE, 'test-episodes-queue')

    @patch.dict(os.environ, {'SHOW_IDS_TABLE': 'test_show_ids'})
    def test_show_ids_table_config(self):
        """Test SHOW_IDS_TABLE configuration loading."""
        # Import after patching to get the patched value
        import importlib
        import tvbingefriend_episode_service.config
        importlib.reload(tvbingefriend_episode_service.config)
        from tvbingefriend_episode_service.config import SHOW_IDS_TABLE
        self.assertEqual(SHOW_IDS_TABLE, 'test_show_ids')

    @patch.dict(os.environ, {'AzureWebJobsStorage': 'DefaultEndpointsProtocol=https;AccountName=test'})
    def test_storage_connection_string_config(self):
        """Test STORAGE_CONNECTION_STRING configuration loading."""
        import importlib
        import tvbingefriend_episode_service.config
        importlib.reload(tvbingefriend_episode_service.config)
        from tvbingefriend_episode_service.config import STORAGE_CONNECTION_STRING
        self.assertEqual(STORAGE_CONNECTION_STRING, 'DefaultEndpointsProtocol=https;AccountName=test')

    @patch.dict(os.environ, {'UPDATES_NCRON': '0 0 3 * * *'})
    def test_updates_ncron_config(self):
        """Test UPDATES_NCRON configuration loading."""
        import importlib
        import tvbingefriend_episode_service.config
        importlib.reload(tvbingefriend_episode_service.config)
        from tvbingefriend_episode_service.config import UPDATES_NCRON
        self.assertEqual(UPDATES_NCRON, '0 0 3 * * *')


if __name__ == '__main__':
    unittest.main()