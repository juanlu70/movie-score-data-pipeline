import pytest
from unittest.mock import Mock

from models import Movie, CriticData, AudienceData, BoxOfficeData
from repository import MovieRepository
from pipeline import MovieDataPipeline
from readers.base import DataReader


class TestMovieRepository:
    def test_add_new_movie(self):
        repo = MovieRepository()
        movie = Movie(
            title="Test Movie", 
            year=2020, 
            critic_score_pct=85
        )

        repo.add_update(movie)

        assert repo.count() == 1
        retrieved = repo.search("Test Movie", 2020)
        assert retrieved.title == "Test Movie"
        assert retrieved.critic_score_pct == 85

    def test_update_existent_movie(self):
        repo = MovieRepository()

        movie1 = Movie(title="Test Movie", year=2020, critic_score_pct=85)
        repo.add_update(movie1)

        movie2 = Movie(title="Test Movie", year=2020, audience_avg_score=8.5)
        repo.add_update(movie2)

        assert repo.count() == 1
        retrieved = repo.search("Test Movie", 2020)
        assert retrieved.critic_score_pct == 85
        assert retrieved.audience_avg_score == 8.5
    
    def test_search_returns_none(self):
        repo = MovieRepository()

        result = repo.search("Nonexistent Movie", 2020)

        assert result is None

    def test_key_case_insensitive(self):
        repo = MovieRepository()
        movie = Movie(title="Test Movie", year=2020)

        repo.add_update(movie)

        assert repo.search("test movie", 2020) is not None
        assert repo.search("TEST MOVIE", 2020) is not None
        assert repo.search("Test Movie", 2020) is not None


class TestMovie:
    def test_calculate_total_box_office(self):
        movie = Movie(
            title="Test",
            year=2020,
            domestic_box_office=100000000,
            intl_box_office=200000000
        )

        assert movie.get_total_box_office() == 300000000
    
    def test_total_box_office_none_if_missing_data(self):
        movie = Movie(title="Test", year=2020, domestic_box_office=100000000)
        
        assert movie.get_total_box_office() is None
    
    def test_generate_corrent_key(self):
        movie = Movie(title="Test Movie", year=2020)
        
        assert movie.get_movie_key() == "test movie_2020"

class TestMovieDataPipeline:
    def test_critic_data_processing(self):
        repo = MovieRepository()
        pipeline = MovieDataPipeline(repo)

        mock_reader = Mock(spec=DataReader)
        mock_reader.read.return_value = [
            CriticData(
                movie_title="Test Movie",
                release_year=2020,
                critic_score_pct=85,
                top_critic_score=8.5,
                total_critic_reviews_counted=100
            )
        ]

        pipeline.process_critic_data(mock_reader)

        movie = repo.search("Test Movie", 2020)
        assert movie is not None
        assert movie.critic_score_pct == 85
        assert movie.top_critic_score == 8.5

    def test_audience_data_processing(self):
        repo = MovieRepository()
        pipeline = MovieDataPipeline(repo)
        
        mock_reader = Mock(spec=DataReader)
        mock_reader.read.return_value = [
            AudienceData(
                title="Test Movie",
                year=2020,
                audience_avg_score=8.5,
                total_audience_ratings=100000,
                domestic_box_office_gross=50000000
            )
        ]

        pipeline.process_audience_data(mock_reader)

        movie = repo.search("Test Movie", 2020)
        assert movie is not None
        assert movie.audience_avg_score == 8.5
        assert movie.domestic_box_office == 50000000

    def test_process_box_office_data(self):
        repo = MovieRepository()
        pipeline = MovieDataPipeline(repo)

        mock_reader = Mock(spec=DataReader)
        mock_reader.read.return_value = [
            BoxOfficeData(
                film_name="Test Movie",
                release_year=2020,
                domestic_gross=100000000,
                intl_gross=200000000,
                prd_budget=50000000,
                market_spend=25000000
            )
        ]

        pipeline.process_box_office_data(mock_reader)

        movie = repo.search("Test Movie", 2020)
        assert movie is not None
        assert movie.domestic_box_office == 100000000
        assert movie.prd_budget == 50000000
    
    def test_full_pipeline_combines_multiple_providers(self):
        repo = MovieRepository()
        pipeline = MovieDataPipeline(repo)

        critic_reader = Mock(spec=DataReader)
        critic_reader.read.return_value = [
            CriticData("Movie", 2020, 85, 8.5, 100)
        ]

        audience_reader = Mock(spec=DataReader)
        audience_reader.read.return_value = [
            AudienceData("Movie", 2020, 9.0, 50000, 100000000)
        ]

        pipeline.run({
            'critic': critic_reader,
            'audience': audience_reader
        })

        movie = repo.search("Movie", 2020)
        assert movie.critic_score_pct == 85
        assert movie.audience_avg_score == 9.0
        assert movie.domestic_box_office == 100000000


class TestIntegration:
    def test_three_providers_full_system(self):
        repo = MovieRepository()
        pipeline = MovieDataPipeline(repo)

        critic_reader = Mock(spec=DataReader)
        critic_reader.read.return_value = [
            CriticData("Inception", 2010, 87, 8.1, 450),
            CriticData("Parasite", 2019, 99, 9.5, 475)
        ]

        audience_reader = Mock(spec=DataReader)
        audience_reader.read.return_value = [
            AudienceData("Inception", 2010, 9.1, 1500000, 292576195),
            AudienceData("Parasite", 2019, 9.0, 800000, 53369749)
        ]

        box_office_reader = Mock(spec=DataReader)
        box_office_reader.read.return_value = [
            BoxOfficeData("Inception", 2010, 292576195, 535700000, 160000000, 100000000),
            BoxOfficeData("Parasite", 2019, None, None, None, None)
        ]

        pipeline.run({
            'critic': critic_reader,
            'audience': audience_reader,
            'box_office': box_office_reader
        })

        assert repo.count() == 2

        inception = repo.search("Inception", 2010)
        assert inception.critic_score_pct == 87
        assert inception.audience_avg_score == 9.1
        assert inception.get_total_box_office() == 828276195
        assert inception.prd_budget == 160000000

        parasite = repo.search("Parasite", 2019)
        assert parasite.critic_score_pct == 99
        assert parasite.audience_avg_score == 9.0
