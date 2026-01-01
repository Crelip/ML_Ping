import os
os.environ["POLARS_UNKNOWN_EXTENSION_TYPE_BEHAVIOR"] = "load_as_storage"
from dotenv import load_dotenv
load_dotenv()
import polars as pl
# Querying characteristics for each IP address and storing the result locally
characteristic_query = """
SELECT
    ip_addr,
    AVG(ping_rttavg) as mean_rtt,
    STDDEV(ping_rttavg) as jitter,
    AVG(ping_ploss) as mean_loss,
    COUNT(*) as observation_count
FROM public.ping
WHERE ping_ploss < 100
    AND ping_ploss >= 0
    AND ping_date >= '2009-01-01'
    AND ping_rttavg < 20000
GROUP BY ip_addr
HAVING COUNT(*) > 50
"""

df_characteristic = (pl.read_database_uri(query=characteristic_query, uri=db_uri, engine=db_eng)
                     .with_columns(pl.col("mean_loss").cast(pl.Float64, strict=False)))
os.makedirs("data", exist_ok=True)
df_characteristic.write_parquet("data/characteristics.parquet")