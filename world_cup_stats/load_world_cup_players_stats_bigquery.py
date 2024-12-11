import pathlib
from typing import List, Dict

from google.cloud import bigquery
from google.cloud.bigquery import job

from world_cup_stats.file_loader import load_nd_json_file_as_string
from world_cup_stats.root import ROOT_DIR
from world_cup_stats.team_player_stats_with_fifa_ranking_mapper import to_stats_domain_dicts_with_fifa_ranking

input_players_stats_domain_file_path = f'{ROOT_DIR}/files/world_cup_players_stats_domain_ndjson.json'
input_players_stats_with_error_domain_file_path = (
    f'{ROOT_DIR}/files/world_cup_players_stats_domain_ndjson.json'
)


def load_world_cup_players_stats_bigquery(input_player_stats_domain_file: str) -> job.LoadJob:
    input_teams_fifa_ranking_file_path = f'{ROOT_DIR}/files/team_fifa_ranking.json'

    # Given.
    teams_players_stats_domain_as_byte = (
        load_nd_json_file_as_string(input_player_stats_domain_file)
        .encode('utf-8')
    )

    teams_fifa_ranking_as_byte = (
        load_nd_json_file_as_string(input_teams_fifa_ranking_file_path)
        .encode('utf-8')
    )

    teams_player_stats_domain_with_fifa_ranking: List[Dict] = to_stats_domain_dicts_with_fifa_ranking(
        teams_players_stats_domain_as_byte,
        teams_fifa_ranking_as_byte
    )

    current_directory = pathlib.Path(__file__).parent
    schema_path = str(current_directory / "schema/world_cup_team_player_stat_schema.json")

    bigquery_client = bigquery.Client()
    schema = bigquery_client.schema_from_json(schema_path)

    job_config = bigquery.LoadJobConfig(
        create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )

    table_id = "qatar_fifa_world_cup.world_cup_team_players_stat_test"

    return bigquery_client.load_table_from_json(
        json_rows=teams_player_stats_domain_with_fifa_ranking,
        destination=table_id,
        job_config=job_config
    )
