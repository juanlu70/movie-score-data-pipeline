import csv
from typing import List, Dict
from pathlib import Path

from readers.base import DataReader
from models import BoxOfficeData


class BoxOfficeMetricsReader(DataReader):
    def __init__(self, domestic_path: str, 
                 international_path: str, financials_path: str):
        self.domestic_path = Path(domestic_path)
        self.international_path = Path(international_path)
        self.financials_path = Path(financials_path)
    
        return

    def read(self) -> List[BoxOfficeData]:
        movies_dict: Dict[tuple, BoxOfficeData] = {}

        self._read_domestic_box_office(movies_dict)
        self._read_international_box_office(movies_dict)
        self._read_financial_data(movies_dict)

        return list(movies_dict.values())

    def _read_domestic_box_office(self, 
                                  movies_dict: 
                                      Dict[tuple, BoxOfficeData]) -> None:
        with open(self.domestic_path, 'r') as fp:
            reader = csv.DictReader(fp)

            for row in reader:
                key = (row['film_name'], int(row['year_of_release']))

                if key not in movies_dict:
                    movies_dict[key] = BoxOfficeData(
                        film_name=row['film_name'],
                        release_year=int(row['year_of_release'])
                    )

                movies_dict[key].domestic_gross = int(
                    row['box_office_gross_usd'])

        return
    
    def _read_international_box_office(self, 
                                       movies_dict: 
                                           Dict[tuple, BoxOfficeData]) -> None:
        with open(self.international_path, 'r') as fp:
            reader = csv.DictReader(fp)

            for row in reader:
                key = (row['film_name'], int(row['year_of_release']))

                if key not in movies_dict:
                    movies_dict[key] = BoxOfficeData(
                        film_name=row['film_name'],
                        release_year=int(row['year_of_release'])
                    )

                movies_dict[key].intl_gross = int(row['box_office_gross_usd'])

        return

    def _read_financial_data(self, 
                             movies_dict: Dict[tuple, BoxOfficeData]) -> None:
        with open(self.financials_path, 'r') as fp:
            reader = csv.DictReader(fp)

            for row in reader:
                key = (row['film_name'], int(row['year_of_release']))

                if key not in movies_dict:
                    movies_dict[key] = BoxOfficeData(
                        film_name=row['film_name'],
                        release_year=int(row['year_of_release'])
                    )

                movies_dict[key].prd_budget = int(row['production_budget_usd'])
                movies_dict[key].market_spend = int(row['marketing_spend_usd'])

        return
