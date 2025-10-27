from typing import List

from models import Movie, CriticData, AudienceData, BoxOfficeData
from repository import MovieRepository
from readers.base import DataReader


class MovieDataPipeline:
    def __init__(self, repository: MovieRepository):
        self.repository = repository
        
        return

    def process_critic_data(self, reader: DataReader) -> None:
        critic_data_list: List[CriticData] = reader.read()

        for critic_data in critic_data_list:
            movie = Movie(
                title=critic_data.movie_title,
                year=critic_data.release_year,
                critic_score_pct=critic_data.critic_score_pct,
                top_critic_score=critic_data.top_critic_score,
                total_critic_reviews=critic_data.total_critic_reviews_counted
            )

            self.repository.add_update(movie)

        return
    
    def process_audience_data(self, reader: DataReader) -> None:
        audience_data_list: List[AudienceData] = reader.read()

        for audience_data in audience_data_list:
            movie = Movie(
                title=audience_data.title,
                year=audience_data.year,
                audience_avg_score=audience_data.audience_avg_score,
                tot_audience_ratings=audience_data.total_audience_ratings,
                domestic_box_office=audience_data.domestic_box_office_gross
            )

            self.repository.add_update(movie)
    
        return
    
    def process_box_office_data(self, reader: DataReader) -> None:
        box_office_data_list: List[BoxOfficeData] = reader.read()
        
        for box_office_data in box_office_data_list:
            movie = Movie(
                title=box_office_data.film_name,
                year=box_office_data.release_year,
                domestic_box_office=box_office_data.domestic_gross,
                intl_box_office=box_office_data.intl_gross,
                prd_budget=box_office_data.prd_budget,
                market_spend=box_office_data.market_spend
            )

            self.repository.add_update(movie)
    
        return

    def run(self, readers: dict) -> None:
        # Process data in a specific order to ensure proper merging
        # 1. Box office data (base financial information)
        # 2. Audience data (ratings and additional box office)
        # 3. Critic data (reviews and scores)
        
        if 'box_office' in readers:
            self.process_box_office_data(readers['box_office'])

        if 'audience' in readers:
            self.process_audience_data(readers['audience'])

        if 'critic' in readers:
            self.process_critic_data(readers['critic'])

        return
