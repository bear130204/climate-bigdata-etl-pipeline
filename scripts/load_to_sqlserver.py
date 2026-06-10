import os
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

csv_file = "warehouse/weather_summary.csv"

# server = "localhost"
# database = "weather_db"
# table_name = "weather_summary"

# if not os.path.exists(csv_file):
#     raise FileNotFoundError(f"Không tìm thấy file: {csv_file}")

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

params = quote_plus(connection_string)

engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={params}",
    fast_executemany=True
)

df = pd.read_csv(csv_file)
df["year"] = df["year"].astype(int)

check_table_sql = text(f"""
SELECT COUNT(*)
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME = '{table_name}'
""")

with engine.connect() as conn:
    table_exists = conn.execute(check_table_sql).scalar() > 0

if not table_exists:
    print("Table chưa tồn tại. Tạo mới và load toàn bộ dữ liệu...")

    df.to_sql(
        table_name,
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=50000
    )

    print("Created table and loaded rows:", len(df))

else:
    existing_years_sql = text(f"""
    SELECT DISTINCT year
    FROM {table_name}
    """)

    existing_years = pd.read_sql(existing_years_sql, engine)["year"].astype(int).tolist()

    print("Các năm đã có trong SQL Server:", existing_years)

    new_df = df[~df["year"].isin(existing_years)]

    if new_df.empty:
        print("Không có năm mới để append. SQL Server đã có đủ dữ liệu.")
    else:
        print("Các năm mới sẽ append:", sorted(new_df["year"].unique()))

        new_df.to_sql(
            table_name,
            con=engine,
            if_exists="append",
            index=False,
            chunksize=50000
        )

        print("Append completed")
        print("Rows appended:", len(new_df))