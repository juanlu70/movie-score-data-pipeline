from pathlib import Path

from repository import MovieRepository
from pipeline import MovieDataPipeline
from readers.critic_agg import CriticAggReader
from readers.audience_pulse import AudiencePulseReader
from readers.box_office_metrics import BoxOfficeMetricsReader


def main():
    print(" Movie Data Pipeline - Initializing...\n")
    
    data_dir = Path("data")

    print("Setting data readers...")

    critic_reader = CriticAggReader(f"{data_dir}/critic_aggregator.csv")
    
    audience_reader = AudiencePulseReader(f"{data_dir}/audience_pulse.json")

    box_office_reader = BoxOfficeMetricsReader(
        domestic_path=f"{data_dir}/box_office_metrics_domestic.csv",
        international_path=f"{data_dir}/box_office_metrics_international.csv",
        financials_path=f"{data_dir}/box_office_metrics_financials.csv"
    )

    print("Starting repository and pipeline...")
    repository = MovieRepository()
    pipeline = MovieDataPipeline(repository)

    print("Processing data from providers...\n")

    readers = {
        'critic': critic_reader,
        'audience': audience_reader,
        'box_office': box_office_reader
    }
    
    pipeline.run(readers)

    print(f"Pipeline finished with success!")
    print(f"Total movies processed: {repository.count()}\n")
    
    print("=" * 80)
    print("Movies in repository:\n")
    
    for movie in repository.search_all():
        print(f" {movie.title} ({movie.year})")
        print(f"- Critic Score Percentage: {movie.critic_score_pct}% "
              f"| Top Critic Score: {movie.top_critic_score}")
        print(f"- Audience Average Score: {movie.audience_avg_score} "
              f"| Ratings: {movie.tot_audience_ratings:,}")

        total_box_office = movie.get_total_box_office()
        if total_box_office:
            print(f"- Total Box Office: ${total_box_office:,}")
        
        if movie.prd_budget:
            print(f"- Budget: ${movie.prd_budget:,}")
        
        print()

    print("=" * 80)
    print("- Search example: Looking for 'Inception'...\n")

    inception = repository.search("Inception", 2010)
    if inception:
        total_box_office = inception.get_total_box_office()
        roi = None
        if total_box_office is not None and inception.prd_budget is not None \
        and inception.market_spend is not None:
            income = total_box_office - inception.prd_budget - inception.market_spend
            expenses = inception.prd_budget + inception.market_spend

            if expenses > 0:
                roi = (income / expenses * 100)
            else:
                roi = 0

        print(f"- Found: ${inception.prd_budget:,}" 
              if inception.prd_budget else " Budget: Not available")
        print(f"- World Box Office: ${inception.get_total_box_office():,}" 
              if inception.get_total_box_office() 
              else " Box Office: Not available")
        print(f"- ROI: {roi:.1f}%" 
              if roi is not None 
              else " ROI: Not available")


if __name__ == "__main__":
    main()
