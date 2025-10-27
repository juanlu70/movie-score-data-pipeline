import pytest
import json
import csv
from pathlib import Path
from tempfile import TemporaryDirectory

from readers.critic_agg import CriticAggReader
from readers.audience_pulse import AudiencePulseReader
from readers.box_office_metrics import BoxOfficeMetricsReader


class TestCriticAggReader:
    def test_read_csv(self) -> None:
        csv_file = f"data/test_provider1.csv"
        with open(csv_file, 'w', newline='') as fp:
            writer = csv.writer(fp)
            writer.writerow(['movie_title', 'release_year', 'critic_score_percentage', 
                           'top_critic_score', 'total_critic_reviews_counted'])
            writer.writerow(['Test Movie', '2020', '85', '8.5', '100'])
            writer.writerow(['Another Movie', '2021', '90', '9.0', '150'])

        reader = CriticAggReader(str(csv_file))
        data = reader.read()

        assert len(data) == 2
        assert data[0].movie_title == 'Test Movie'
        assert data[0].release_year == 2020
        assert data[0].critic_score_pct == 85
        assert data[1].movie_title == 'Another Movie'
    
        return
    
    def test_convert_datatypes(self) -> None:
        csv_file = "data/test_provider1.csv"
        with open(csv_file, 'w', newline='') as fp:
            writer = csv.writer(fp)
            writer.writerow(['movie_title', 'release_year', 'critic_score_percentage', 
                           'top_critic_score', 'total_critic_reviews_counted'])
            writer.writerow(['Test', '2020', '85', '8.5', '100'])

        reader = CriticAggReader(str(csv_file))
        data = reader.read()

        assert isinstance(data[0].release_year, int)
        assert isinstance(data[0].critic_score_pct, int)
        assert isinstance(data[0].top_critic_score, float)
        assert isinstance(data[0].total_critic_reviews_counted, int)

        return


class TestAudiencePulseReader:
    def test_read_json(self) -> None:
        json_file = "data/test_provider2.json"
        test_data = [
            {
                "title": "Test Movie",
                "year": "2020",
                "audience_average_score": 8.5,
                "total_audience_ratings": 100000,
                "domestic_box_office_gross": 50000000
            }
        ]

        with open(json_file, 'w') as fp:
            json.dump(test_data, fp)

        reader = AudiencePulseReader(str(json_file))
        data = reader.read()

        assert len(data) == 1
        assert data[0].title == 'Test Movie'
        assert data[0].year == 2020
        assert data[0].audience_avg_score == 8.5

        return

    def test_convert_str_year_to_int(self) -> None:
        json_file = "data/test_provider2.json"
        test_data = [{
                        "title": "Test",
                        "year": "2020",
                        "audience_average_score": 8.5,
                        "total_audience_ratings": 1000, 
                        "domestic_box_office_gross": 1000000
                    }]

        with open(json_file, 'w') as fp:
            json.dump(test_data, fp)

        reader = AudiencePulseReader(str(json_file))
        data = reader.read()

        assert isinstance(data[0].year, int)
        assert data[0].year == 2020
        
        return


class TestBoxOfficeMetricsReader:
    def test_combine_files(self) -> None:
        domestic_file = "data/test_provider3_domestic.csv"
        with open(domestic_file, 'w', newline='') as fp:
            writer = csv.writer(fp)
            writer.writerow([
                'film_name',
                'year_of_release',
                'box_office_gross_usd'
            ])
            writer.writerow([
                'Test Movie',
                '2020',
                '100000000'
            ])

        international_file = "data/test_provider3_international.csv"
        with open(international_file, 'w', newline='') as fp:
            writer = csv.writer(fp)
            writer.writerow([
                'film_name',
                'year_of_release',
                'box_office_gross_usd'
            ])
            writer.writerow([
                'Test Movie',
                '2020',
                '200000000'
            ])

        financials_file = f"data/test_provider3_financials.csv"
        with open(financials_file, 'w', newline='') as fp:
            writer = csv.writer(fp)
            writer.writerow([
                'film_name',
                'year_of_release',
                'production_budget_usd',
                'marketing_spend_usd'
            ])
            writer.writerow([
                'Test Movie',
                '2020',
                '50000000',
                '25000000'
            ])

        reader = BoxOfficeMetricsReader(
            str(domestic_file),
            str(international_file),
            str(financials_file)
        )
        data = reader.read()

        assert len(data) == 1
        movie = data[0]
        assert movie.film_name == 'Test Movie'
        assert movie.domestic_gross == 100000000
        assert movie.intl_gross == 200000000
        assert movie.prd_budget == 50000000
        assert movie.market_spend == 25000000
        
        return

    def test_process_movies_with_partial_data(self) -> None:
        domestic_file = "data/test_provider3_domestic.csv"
        with open(domestic_file, 'w', newline='') as fp:
            writer = csv.writer(fp)
            writer.writerow([
                'film_name',
                'year_of_release',
                'box_office_gross_usd'
            ])
            writer.writerow([
                'Movie A',
                '2020',
                '100000000'
            ])
            writer.writerow([
                'Movie B',
                '2021',
                '50000000'
            ])

        international_file = "data/test_provider3_international.csv"
        with open(international_file, 'w', newline='') as fp:
            writer = csv.writer(fp)
            writer.writerow([
                'film_name',
                'year_of_release',
                'box_office_gross_usd'
            ])
            writer.writerow([
                'Movie A',
                '2020',
                '200000000'
            ])

        financials_file = "data/test_provider3_financials.csv"
        with open(financials_file, 'w', newline='') as fp:
            writer = csv.writer(fp)
            writer.writerow([
                'film_name',
                'year_of_release',
                'production_budget_usd',
                'marketing_spend_usd'
            ])
            writer.writerow([
                'Movie C',
                '2022',
                '30000000',
                '10000000'
            ])

        reader = BoxOfficeMetricsReader(
            str(domestic_file),
            str(international_file),
            str(financials_file)
        )
        data = reader.read()

        assert len(data) == 3
        
        movie_a = next(m for m in data if m.film_name == 'Movie A')
        assert movie_a.domestic_gross == 100000000
        assert movie_a.intl_gross == 200000000
        assert movie_a.prd_budget is None

        movie_b = next(m for m in data if m.film_name == 'Movie B')
        assert movie_b.domestic_gross == 50000000
        assert movie_b.intl_gross is None

        return
