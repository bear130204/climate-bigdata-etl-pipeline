# import os
# import glob
# import pandas as pd
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, substring, when, avg, max, min, count

# spark = SparkSession.builder \
#     .appName("Weather ETL 50 Years") \
#     .master("local[4]") \
#     .config("spark.driver.memory", "6g") \
#     .config("spark.executor.memory", "6g") \
#     .config("spark.sql.shuffle.partitions", "8") \
#     .getOrCreate()

# input_dir = "data/raw/ghcnd_all_years"

# files = sorted(
#     glob.glob(os.path.join(input_dir, "*.csv.gz"))
# )

# print("Tổng số file:", len(files))

# # Lấy 50 năm gần nhất
# files = files[-50:]

# print("Số file dùng:", len(files))
# print("Từ:", os.path.basename(files[0]))
# print("Đến:", os.path.basename(files[-1]))

# df = spark.read.csv(
#     files,
#     header=False,
#     inferSchema=False
# )

# df = df.toDF(
#     "station_id",
#     "date",
#     "element",
#     "value",
#     "m_flag",
#     "q_flag",
#     "s_flag",
#     "obs_time"
# )

# # Chỉ lấy 3 chỉ số chính
# df = df.filter(
#     col("element").isin("TMAX", "TMIN", "PRCP")
# )

# df = df.withColumn(
#     "value",
#     col("value").cast("double")
# )

# # Quy đổi TMAX/TMIN từ phần mười độ C sang độ C
# df = df.withColumn(
#     "value",
#     when(
#         col("element").isin("TMAX", "TMIN"),
#         col("value") / 10.0
#     ).otherwise(col("value"))
# )

# df = df.withColumn("year", substring(col("date"), 1, 4))
# df = df.withColumn("month", substring(col("date"), 5, 2))

# # Tạo thư mục output
# os.makedirs("data/processed", exist_ok=True)
# os.makedirs("warehouse", exist_ok=True)

# # Lưu 1 triệu dòng dữ liệu sạch để minh chứng Big Data
# print("Đang lưu sample 1,000,000 dòng...")

# clean_sample = df.limit(10000000).toPandas()

# clean_sample.to_csv(
#     "data/processed/weather_clean_sample.csv",
#     index=False
# )

# print("Đã lưu: data/processed/weather_clean_sample.csv")

# # Tổng hợp dữ liệu cho phân tích
# summary = df.groupBy(
#     "element",
#     "year",
#     "month"
# ).agg(
#     avg("value").alias("avg_value"),
#     max("value").alias("max_value"),
#     min("value").alias("min_value"),
#     count("*").alias("record_count")
# )

# print("Đang tạo summary...")

# summary_pd = summary.toPandas()

# summary_pd.to_csv(
#     "warehouse/weather_summary.csv",
#     index=False
# )

# print("ETL completed")
# print("Đã lưu: warehouse/weather_summary.csv")

# spark.stop()


import os
import glob
import pandas as pd
import pyodbc

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, when, avg, max, min, count

INPUT_DIR = "data/raw/ghcnd_all_years"
COUNTRIES_PATH = "data/raw/ghcnd-countries.txt"

os.makedirs("data/processed", exist_ok=True)
os.makedirs("warehouse", exist_ok=True)

# server = "host.docker.internal"
# database = "weather_db"
# table_name = "weather_summary"

# connection_string = (
#     "DRIVER={ODBC Driver 18 for SQL Server};"
#     f"SERVER={server};"
#     f"DATABASE={database};"
#     "Trusted_Connection=yes;"
#     "TrustServerCertificate=yes;"
# )
server = "host.docker.internal,1433"
database = "weather_db"
username = "airflow_user"
password = "123456"
table_name = "weather_summary"

connection_string = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
    "TrustServerCertificate=yes;"
)

conn = pyodbc.connect(connection_string)

spark = SparkSession.builder \
    .appName("Weather Incremental ETL") \
    .master("local[4]") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "6g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

files = sorted(glob.glob(os.path.join(INPUT_DIR, "*.csv.gz")))

print("Tổng số file trong thư mục:", len(files))

check_table_sql = """
SELECT COUNT(*)
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME = ?
"""

cursor = conn.cursor()
cursor.execute(check_table_sql, table_name)
table_exists = cursor.fetchone()[0] > 0
cursor.close()

existing_years = []

if table_exists:
    existing_years_sql = f"""
    SELECT DISTINCT year
    FROM {table_name}
    """

    existing_years = pd.read_sql_query(existing_years_sql, conn)["year"].astype(str).tolist()

conn.close()

print("Các năm đã có trong SQL Server:", existing_years)

files = [
    f for f in files
    if os.path.basename(f).replace(".csv.gz", "") not in existing_years
]

if not files:
    print("Không có năm mới cần xử lý. Dừng ETL.")
    spark.stop()
    exit()

print("Số file mới cần xử lý:", len(files))
print("Từ:", os.path.basename(files[0]))
print("Đến:", os.path.basename(files[-1]))

df = spark.read.csv(
    files,
    header=False,
    inferSchema=False
)

df = df.toDF(
    "station_id",
    "date",
    "element",
    "value",
    "m_flag",
    "q_flag",
    "s_flag",
    "obs_time"
)

df = df.filter(
    col("element").isin("TMAX", "TMIN", "PRCP")
)

df = df.withColumn(
    "value",
    col("value").cast("double")
)

df = df.withColumn(
    "value",
    when(
        col("element").isin("TMAX", "TMIN"),
        col("value") / 10.0
    ).otherwise(col("value"))
)

df = df.withColumn("year", substring(col("date"), 1, 4))
df = df.withColumn("month", substring(col("date"), 5, 2))

df = df.withColumn(
    "country_code",
    substring(col("station_id"), 1, 2)
)

country_rdd = spark.sparkContext.textFile(COUNTRIES_PATH)

country_df = country_rdd.map(lambda line: (
    line[0:2].strip(),
    line[3:].strip()
)).toDF([
    "country_code",
    "country_name"
])

df = df.join(
    country_df,
    on="country_code",
    how="left"
)

print("Đang lưu sample dữ liệu mới...")

clean_sample = df.select(
    "station_id",
    "country_code",
    "country_name",
    "date",
    "year",
    "month",
    "element",
    "value"
).limit(1000000).toPandas()

clean_sample.to_csv(
    "data/processed/weather_clean_sample.csv",
    index=False,
    encoding="utf-8-sig"
)

print("Đã lưu: data/processed/weather_clean_sample.csv")

print("Đang tạo summary cho năm mới...")

summary = df.groupBy(
    "country_name",
    "country_code",
    "element",
    "year",
    "month"
).agg(
    avg("value").alias("avg_value"),
    max("value").alias("max_value"),
    min("value").alias("min_value"),
    count("*").alias("record_count")
)

summary_pd = summary.toPandas()

summary_pd.to_csv(
    "warehouse/weather_summary.csv",
    index=False,
    encoding="utf-8-sig"
)

print("ETL completed")
print("Đã lưu summary năm mới vào warehouse/weather_summary.csv")

spark.stop()