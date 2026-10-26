import tomllib

from pathlib import Path
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, model_validator

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
    broadcast_threshold_mb: int = Field(default=10, ge=-1)    # -1 disables auto broadcast
    register_tables_as: Literal["views", "tables"] = "views"  # tables allow ANALYZE TABLE
    cbo_enabled: bool = False                                 # runs ANALYZE TABLE on registration
    cbo_join_reorder: bool = False                            # requires cbo_enabled

    # The CBO needs statistics, and ANALYZE TABLE does not work on temp views
    @model_validator(mode="after")
    def cbo_requires_tables(self):
        if self.cbo_enabled and self.register_tables_as != "tables":
            raise ValueError('cbo_enabled = true requires register_tables_as = "tables"')
        # Spark ignores join reordering unless the CBO is enabled
        if self.cbo_join_reorder and not self.cbo_enabled:
            raise ValueError("cbo_join_reorder = true requires cbo_enabled = true")
        return self

# 4. Principal model that joins all sections
class RunConfig(StrictBaseModel):
    general: GeneralConfig = GeneralConfig()
    spark: SparkConfig = SparkConfig()


def load_config(path: str | Path = CONFIG_PATH) -> RunConfig:
    with open(path, "rb") as f:
        data = tomllib.load(f)
    return RunConfig(**data)

options = load_config()