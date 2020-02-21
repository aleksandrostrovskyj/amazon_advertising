import pathlib
import yaml

BASE_DIR = pathlib.Path(__file__).parent
config_path = BASE_DIR / 'config' / 'config.yaml'
metric_path = BASE_DIR / 'config' / 'metrics.yaml'

def get_config(path):
    with open(path) as f:
        config = yaml.safe_load(f)
    return config


def get_metrics(path):
    with open(path) as f:
        metrics = yaml.safe_load(f)
    return metrics


config = get_config(config_path)
metrics = get_metrics(metric_path)