# Databricks notebook source
"""Create or verify the CareWatch namespace, landing Volume, and control tables."""

# COMMAND ----------

import sys
from pathlib import Path


def _add_project_src() -> None:
    cwd = Path.cwd()
    for candidate in (cwd / "src", cwd.parent / "src"):
        if (candidate / "carewatch").is_dir():
            value = str(candidate)
            if value not in sys.path:
                sys.path.insert(0, value)
            return
    raise RuntimeError("Could not locate src/carewatch from the Databricks Git folder")


_add_project_src()

from carewatch.audit import ensure_control_tables  # noqa: E402
from carewatch.config import qualified_name  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalog", "carewatch")
dbutils.widgets.text("schema", "pipeline")
dbutils.widgets.text("volume", "landing")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

# Validate every identifier before interpolating it into DDL.
volume_name = qualified_name(catalog, schema_name, volume)

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_name}")

# COMMAND ----------

from pyspark.sql.types import StringType, StructField, StructType


SETUP_RESULT_SCHEMA = StructType(
    [
        StructField("object_name", StringType(), False),
        StructField("object_type", StringType(), False),
        StructField("action", StringType(), False),
    ]
)

setup_results = [
    {"object_name": catalog, "object_type": "CATALOG", "action": "VERIFIED"},
    {
        "object_name": f"{catalog}.{schema_name}",
        "object_type": "SCHEMA",
        "action": "VERIFIED",
    },
    {"object_name": volume_name, "object_type": "VOLUME", "action": "VERIFIED"},
]
setup_results.extend(
    {
        "object_name": item["table_name"],
        "object_type": "DELTA_TABLE",
        "action": item["action"],
    }
    for item in ensure_control_tables(spark, catalog, schema_name)
)

display(
    spark.createDataFrame(setup_results, schema=SETUP_RESULT_SCHEMA).orderBy(
        "object_type", "object_name"
    )
)

# COMMAND ----------

landing_root = f"/Volumes/{catalog}/{schema_name}/{volume}"
print(f"Control tables are ready. Landing root: {landing_root}")
