import csv
from typing import List
from pathlib import Path

from readers.base import DataReader
from models import CriticData


class CriticAggReader(DataReader):
    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        
        return
    
    def read(self) -> List[CriticData]:
        critic_data_list = []
        
        with open(self.file_path, 'r') as fp:
            reader = csv.DictReader(fp)

            for row in reader:
                critic_data = CriticData(
                    movie_title=row['movie_title'],
                    release_year=int(row['release_year']),
                    critic_score_pct=int(row['critic_score_percentage']),
                    top_critic_score=float(row['top_critic_score']),
                    total_critic_reviews_counted=int(row[
                        'total_critic_reviews_counted'])
                )
                critic_data_list.append(critic_data)

        return critic_data_list
