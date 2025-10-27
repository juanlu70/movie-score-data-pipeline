from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Movie:
    title: str = ""
    year: int = 0

    critic_score_pct: Optional[int] = None
    top_critic_score: Optional[float] = None
    total_critic_reviews: Optional[int] = None

    audience_avg_score: Optional[float] = None
    tot_audience_ratings: Optional[int] = None

    domestic_box_office: Optional[int] = None
    intl_box_office: Optional[int] = None
    prd_budget: Optional[int] = None
    market_spend: Optional[int] = None

    def get_total_box_office(self) -> Optional[int]:
        if self.domestic_box_office is not None and self.intl_box_office is not None:
            return self.domestic_box_office + self.intl_box_office
        return None

    def get_movie_key(self) -> str:
        return f"{self.title.lower().strip()}_{self.year}"


@dataclass
class CriticData:
    movie_title: str
    release_year: int
    critic_score_pct: int
    top_critic_score: float
    total_critic_reviews_counted: int


@dataclass
class AudienceData:
    title: str
    year: int
    audience_avg_score: float
    total_audience_ratings: int
    domestic_box_office_gross: int


@dataclass
class BoxOfficeData:
    film_name: str
    release_year: int
    domestic_gross: Optional[int] = None
    intl_gross: Optional[int] = None
    prd_budget: Optional[int] = None
    market_spend: Optional[int] = None
