import tomllib

from pathlib import Path
from pydantic import BaseModel, ConfigDict, Field

from constants import CONFIG_PATH

# 1. Based config shared by all config sections, to ensure consistency and immutability
class StrictBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

# 2. Section [general]: Paths, executions, behavior of the script
class GeneralConfig(StrictBaseModel):
    runs_per_query: int = Field(default=5, gt=0)  # gt=0 makes sure that always >= 1

# 3. Section [spark]: Optimizations, hints and performance tests
class SparkConfig(StrictBaseModel):
    use_partition: bool = False
    use_broadcast_joins_hints: bool = False
    adaptative_enabled: bool = False
    adaptative_coalesce_partitions: bool = False
    adapatative_skew_join: bool = False
    broadcast_threshold_mb: int = Field(default=10, ge=-1)  # -1 disables auto broadcast

# 4. Principal model that joins all sections
class RunConfig(StrictBaseModel):
    general: GeneralConfig = GeneralConfig()
    spark: SparkConfig = SparkConfig()


def load_config(path: str | Path = CONFIG_PATH) -> RunConfig:
    with open(path, "rb") as f:
        data = tomllib.load(f)
    return RunConfig(**data)

options = load_config()