from typing import Dict, Optional, List
from models import Movie


class MovieRepository:
    def __init__(self):
        self._movies: Dict[str, Movie] = {}

        return

    def add_update(self, movie: Movie) -> None:
        key = movie.get_movie_key()

        if key in self._movies:
            existing_movie = self._movies[key]
            self._merge_movie_data(existing_movie, movie)
        else:
            self._movies[key] = movie

        return

    def search(self, title: str, year: int) -> Optional[Movie]:
        key = f"{title.lower().strip()}_{year}"

        return self._movies.get(key)
    
    def search_all(self) -> List[Movie]:
        return list(self._movies.values())
    
    def count(self) -> int:
        return len(self._movies)

    def _merge_movie_data(self, existing: Movie, new: Movie) -> None:
        if new.critic_score_pct is not None:
            existing.critic_score_pct = new.critic_score_pct
        
        if new.top_critic_score is not None:
            existing.top_critic_score = new.top_critic_score
        
        if new.total_critic_reviews is not None:
            existing.total_critic_reviews = new.total_critic_reviews
        
        if new.audience_avg_score is not None:
            existing.audience_avg_score = new.audience_avg_score
        
        if new.tot_audience_ratings is not None:
            existing.tot_audience_ratings = new.tot_audience_ratings
        
        if new.domestic_box_office is not None:
            existing.domestic_box_office = new.domestic_box_office
        
        if new.intl_box_office is not None:
            existing.intl_box_office = new.intl_box_office
        
        if new.prd_budget is not None:
            existing.prd_budget = new.prd_budget
        
        if new.market_spend is not None:
            existing.market_spend = new.market_spend

        return
