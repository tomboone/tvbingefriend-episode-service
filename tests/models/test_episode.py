import os
import unittest
from datetime import date
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Set required env vars for module import
os.environ['SQLALCHEMY_CONNECTION_STRING'] = 'sqlite:///:memory:'

from tvbingefriend_episode_service.models.base import Base
from tvbingefriend_episode_service.models.episode import Episode


class TestEpisode(unittest.TestCase):

    def setUp(self):
        """Set up test database."""
        self.engine = create_engine('sqlite:///:memory:', echo=False)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    def tearDown(self):
        """Clean up test database."""
        self.session.close()
        Base.metadata.drop_all(self.engine)

    def test_episode_creation(self):
        """Test creating an episode with all fields."""
        episode_data = {
            'id': 12345,
            'show_id': 123,
            'url': 'https://www.tvmaze.com/episodes/12345/test-show-1x01-pilot',
            'name': 'Pilot',
            'season': 1,
            'number': 1,
            'type': 'regular',
            'airdate': date(2023, 1, 1),
            'airtime': '21:00',
            'airstamp': '2023-01-01T21:00:00+00:00',
            'runtime': 60,
            'rating': {'average': 8.5},
            'image': {
                'medium': 'https://static.tvmaze.com/uploads/images/medium_landscape/1/1.jpg',
                'original': 'https://static.tvmaze.com/uploads/images/original_untouched/1/1.jpg'
            },
            'summary': '<p>This is the pilot episode.</p>',
            '_links': {
                'self': {'href': 'https://api.tvmaze.com/episodes/12345'},
                'show': {'href': 'https://api.tvmaze.com/shows/123'}
            }
        }
        
        episode = Episode(**episode_data)
        self.session.add(episode)
        self.session.commit()
        
        # Verify the episode was created correctly
        saved_episode = self.session.query(Episode).filter_by(id=12345).first()
        self.assertIsNotNone(saved_episode)
        self.assertEqual(saved_episode.id, 12345)
        self.assertEqual(saved_episode.show_id, 123)
        self.assertEqual(saved_episode.name, 'Pilot')
        self.assertEqual(saved_episode.season, 1)
        self.assertEqual(saved_episode.number, 1)
        self.assertEqual(saved_episode.type, 'regular')
        self.assertEqual(saved_episode.airdate, date(2023, 1, 1))
        self.assertEqual(saved_episode.airtime, '21:00')
        self.assertEqual(saved_episode.runtime, 60)
        self.assertEqual(saved_episode.rating, {'average': 8.5})
        self.assertEqual(saved_episode.image['medium'], 'https://static.tvmaze.com/uploads/images/medium_landscape/1/1.jpg')
        self.assertIn('This is the pilot episode', saved_episode.summary)

    def test_episode_minimal_fields(self):
        """Test creating an episode with minimal required fields."""
        episode = Episode(id=54321, show_id=456, url="http://example.com/episode/54321")
        self.session.add(episode)
        self.session.commit()
        
        saved_episode = self.session.query(Episode).filter_by(id=54321).first()
        self.assertIsNotNone(saved_episode)
        self.assertEqual(saved_episode.id, 54321)
        self.assertEqual(saved_episode.show_id, 456)
        self.assertIsNone(saved_episode.name)
        self.assertIsNone(saved_episode.season)
        self.assertIsNone(saved_episode.number)

    def test_episode_nullable_fields(self):
        """Test that nullable fields can be None."""
        episode = Episode(
            id=67890,
            show_id=789,
            url="http://example.com/episode/67890",
            name=None,
            season=None,
            number=None,
            type=None,
            airdate=None,
            airtime=None,
            airstamp=None,
            runtime=None,
            rating=None,
            image=None,
            summary=None,
            _links=None
        )
        self.session.add(episode)
        self.session.commit()
        
        saved_episode = self.session.query(Episode).filter_by(id=67890).first()
        self.assertIsNotNone(saved_episode)
        self.assertEqual(saved_episode.show_id, 789)
        self.assertIsNone(saved_episode.name)
        self.assertIsNone(saved_episode.rating)
        self.assertIsNone(saved_episode.image)

    def test_episode_json_fields(self):
        """Test that JSON fields work correctly."""
        complex_rating = {
            'average': 7.8,
            'count': 1250
        }
        complex_image = {
            'medium': 'https://example.com/medium.jpg',
            'original': 'https://example.com/original.jpg',
            'meta': {'width': 1920, 'height': 1080}
        }
        complex_links = {
            'self': {'href': 'https://api.tvmaze.com/episodes/99999'},
            'show': {'href': 'https://api.tvmaze.com/shows/999'},
            'nextepisode': {'href': 'https://api.tvmaze.com/episodes/99998'}
        }
        
        episode = Episode(
            id=99999,
            show_id=999,
            url="http://example.com/episode/99999",
            rating=complex_rating,
            image=complex_image,
            _links=complex_links
        )
        self.session.add(episode)
        self.session.commit()
        
        saved_episode = self.session.query(Episode).filter_by(id=99999).first()
        self.assertIsNotNone(saved_episode)
        self.assertEqual(saved_episode.rating, complex_rating)
        self.assertEqual(saved_episode.image, complex_image)
        self.assertEqual(saved_episode._links, complex_links)

    def test_episode_table_name(self):
        """Test that the table name is correct."""
        self.assertEqual(Episode.__tablename__, 'episodes')

    def test_episode_primary_key(self):
        """Test that the primary key is set correctly."""
        episode1 = Episode(id=111, show_id=1, url="http://example.com/episode/111")
        episode2 = Episode(id=222, show_id=1, url="http://example.com/episode/222")
        
        self.session.add(episode1)
        self.session.add(episode2)
        self.session.commit()
        
        # Should be able to query by primary key
        found_episode = self.session.query(Episode).filter_by(id=111).first()
        self.assertIsNotNone(found_episode)
        self.assertEqual(found_episode.id, 111)


if __name__ == '__main__':
    unittest.main()