from google.api_core.exceptions import ClientError
from google.cloud.bigquery import job

from world_cup_stats.load_world_cup_players_stats_bigquery import load_world_cup_players_stats_bigquery, \
    input_players_stats_with_error_domain_file_path


def insert_world_cup_stats(input_player_stats_domain_file: str):
    load_job: job.LoadJob = load_world_cup_players_stats_bigquery(input_player_stats_domain_file)

    try:
        load_job.result()
    except ClientError as e:
        print(load_job.errors)
        raise e


if __name__ == '__main__':
    insert_world_cup_stats(input_players_stats_with_error_domain_file_path)

    print("Data was inserted correctly to BigQuery")
