import json
from typing import List
from pathlib import Path

from readers.base import DataReader
from models import AudienceData


class AudiencePulseReader(DataReader):
    def __init__(self, file_path: str):
        self.file_path = Path(file_path)

        return
    
    def read(self) -> List[AudienceData]:
        with open(self.file_path, 'r') as fp:
            raw_data = json.load(fp)

        audience_data_list = []

        for item in raw_data:
            audience_data = AudienceData(
                title=item['title'],
                year=int(item['year']),
                audience_avg_score=float(item['audience_average_score']),
                total_audience_ratings=int(item['total_audience_ratings']),
                domestic_box_office_gross=int(item['domestic_box_office_gross'])
            )
            audience_data_list.append(audience_data)

        return audience_data_list
