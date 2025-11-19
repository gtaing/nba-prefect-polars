import os
import yaml

from dataclasses import dataclass

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PARAMETERS_FILE = os.path.join(CURRENT_DIR, "parameters.yml")


@dataclass
class BucketRaw:
    games_detail: str
    player_stats: str


@dataclass
class BucketProcessed:
    team_season_stats: str
    player_season_stats: str


@dataclass
class BucketConf:
    raw: BucketRaw
    processed: BucketProcessed

    @classmethod
    def from_yaml(cls, path: str):
        with open(path, 'r') as f:
            config = yaml.safe_load(f)

        return cls(
            raw=BucketRaw(**config['bucket_files']['raw']),
            processed=BucketProcessed(**config['bucket_files']['processed'])
        )
    

bucket_conf = BucketConf.from_yaml(PARAMETERS_FILE)