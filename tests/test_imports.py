"""Test that all modules can be imported successfully."""

def test_episode_model_import():
    """Test that Episode model can be imported."""
    from tvbingefriend_episode_service.models.episode import Episode
    assert Episode is not None

def test_episode_service_import():
    """Test that EpisodeService can be imported."""
    from tvbingefriend_episode_service.services.episode_service import EpisodeService
    assert EpisodeService is not None

def test_episode_repository_import():
    """Test that EpisodeRepository can be imported."""
    from tvbingefriend_episode_service.repos.episode_repo import EpisodeRepository
    assert EpisodeRepository is not None

def test_blueprints_import():
    """Test that all blueprints can be imported."""
    from tvbingefriend_episode_service.blueprints import bp_get_show_episodes
    from tvbingefriend_episode_service.blueprints import bp_health_monitoring
    from tvbingefriend_episode_service.blueprints import bp_start_get_all
    from tvbingefriend_episode_service.blueprints import bp_updates_manual
    from tvbingefriend_episode_service.blueprints import bp_updates_timer
    
    assert bp_get_show_episodes is not None
    assert bp_health_monitoring is not None
    assert bp_start_get_all is not None
    assert bp_updates_manual is not None
    assert bp_updates_timer is not None

def test_function_app_import():
    """Test that function app can be imported."""
    from function_app import app
    assert app is not None