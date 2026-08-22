import yaml
from pathlib import Path

#ONFIG_PATH = Path(__file__).parents[1] / "config" / "aws.yaml"
CONFIG_PATH = Path(__file__).parents[2] / "config" / "aws.yaml"

with open(CONFIG_PATH) as file:
    config = yaml.safe_load(file)

#rom src.config import config

print(config)