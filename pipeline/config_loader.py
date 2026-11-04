import tomllib
from pathlib import Path
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, model_validator

from constants import CONFIG_PATH
from pydantic import BaseModel, ConfigDict, Field, PositiveInt


class StrictBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

class GeneralConfig(StrictBaseModel):
    runs_per_query: int = Field(default=5, gt=0)  # gt=0 makes sure that always >= 1

class SparkConfig(StrictBaseModel):
    use_partition: bool = False
    use_repartition: bool = False   # requires use_partition: one file per partition value
    use_broadcast_joins_hints: bool = False
    adaptative_enabled: bool = False
    adaptative_coalesce_partitions: bool = False
    adapatative_skew_join: bool = False
    broadcast_threshold_mb: int = Field(default=10, ge=-1)    # -1 disables auto broadcast
    register_tables_as: Literal["views", "tables"] = "views"  # tables allow ANALYZE TABLE
    cbo_enabled: bool = False                                 # runs ANALYZE TABLE on registration
    cbo_join_reorder: bool = False                            # requires cbo_enabled
    cbo_plan_stats_enabled: bool = False                       # requires cbo_enabled
    shuffle_partitions: PositiveInt | Literal[-1] = Field(default=200)    # Spark default is 200, -1 dynamic calculation
    use_all_cores: bool = True    # if False, use 4 cores for local testing
    tune_memory_allocation: bool = False
    enable_static_caching: bool = False

    # The CBO needs statistics, and ANALYZE TABLE does not work on temp views
    @model_validator(mode="after")
    def cbo_requires_tables(self):
        if self.cbo_enabled and self.register_tables_as != "tables":
            raise ValueError('cbo_enabled = true requires register_tables_as = "tables"')
        # Spark ignores join reordering unless the CBO is enabled
        if self.cbo_join_reorder and not self.cbo_enabled:
            raise ValueError("cbo_join_reorder = true requires cbo_enabled = true")
        # Repartitioning only changes how partitioned tables are written
        if self.use_repartition and not self.use_partition:
            raise ValueError("use_repartition = true requires use_partition = true")
        return self


class RunConfig(StrictBaseModel):
    general: GeneralConfig = GeneralConfig()
    spark: SparkConfig = SparkConfig()


def load_config(path: str | Path = CONFIG_PATH) -> RunConfig:
    with open(path, "rb") as f:
        data = tomllib.load(f)
    return RunConfig(**data)

options = load_config()