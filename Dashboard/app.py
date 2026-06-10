# import pandas as pd
# import streamlit as st
# import plotly.express as px
# from sqlalchemy import create_engine
# from urllib.parse import quote_plus

# # =========================
# # PAGE CONFIG
# # =========================
# st.set_page_config(
#     page_title="Climate Big Data Dashboard",
#     page_icon="🌍",
#     layout="wide"
# )

# # =========================
# # SQL SERVER CONFIG
# # =========================
# SERVER = "localhost,1433"
# DATABASE = "weather_db"
# USERNAME = "airflow_user"
# PASSWORD = "123456"
# TABLE_NAME = "weather_summary"

# connection_string = (
#     "DRIVER={ODBC Driver 18 for SQL Server};"
#     f"SERVER={SERVER};"
#     f"DATABASE={DATABASE};"
#     f"UID={USERNAME};"
#     f"PWD={PASSWORD};"
#     "TrustServerCertificate=yes;"
# )

# params = quote_plus(connection_string)
# engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")


# # =========================
# # LOAD DATA
# # =========================
# @st.cache_data(ttl=300)
# def load_data():
#     query = f"""
#     SELECT
#         country_name,
#         country_code,
#         element,
#         year,
#         month,
#         avg_value,
#         max_value,
#         min_value,
#         record_count
#     FROM {TABLE_NAME}
#     """
#     df = pd.read_sql(query, engine)

#     df["year"] = df["year"].astype(int)
#     df["month"] = df["month"].astype(int)
#     df["avg_value"] = pd.to_numeric(df["avg_value"], errors="coerce")
#     df["record_count"] = pd.to_numeric(df["record_count"], errors="coerce")

#     df = df.dropna(subset=["country_name", "element", "year", "avg_value"])
#     return df


# df = load_data()

# # =========================
# # SIDEBAR
# # =========================
# st.sidebar.title("🔎 Bộ lọc dữ liệu")

# years = sorted(df["year"].unique())
# countries = sorted(df["country_name"].dropna().unique())

# selected_years = st.sidebar.multiselect(
#     "Chọn năm",
#     years,
#     default=years[-10:] if len(years) >= 10 else years
# )

# selected_countries = st.sidebar.multiselect(
#     "Chọn quốc gia",
#     countries,
#     default=countries[:10] if len(countries) >= 10 else countries
# )

# selected_elements = st.sidebar.multiselect(
#     "Chọn chỉ số khí hậu",
#     ["TMAX", "TMIN", "PRCP"],
#     default=["TMAX", "TMIN", "PRCP"]
# )

# filtered_df = df[
#     (df["year"].isin(selected_years)) &
#     (df["country_name"].isin(selected_countries)) &
#     (df["element"].isin(selected_elements))
# ]

# # =========================
# # HEADER
# # =========================
# st.title("🌍 Climate Big Data Dashboard")
# st.caption("Hệ thống phân tích dữ liệu khí hậu tự động từ NOAA bằng Airflow, Spark, SQL Server và Streamlit.")

# # =========================
# # KPI
# # =========================
# total_records = int(filtered_df["record_count"].sum()) if not filtered_df.empty else 0
# total_countries = filtered_df["country_name"].nunique()
# min_year = filtered_df["year"].min() if not filtered_df.empty else "-"
# max_year = filtered_df["year"].max() if not filtered_df.empty else "-"

# tmax_avg = filtered_df[filtered_df["element"] == "TMAX"]["avg_value"].mean()
# tmin_avg = filtered_df[filtered_df["element"] == "TMIN"]["avg_value"].mean()
# prcp_avg = filtered_df[filtered_df["element"] == "PRCP"]["avg_value"].mean()

# col1, col2, col3, col4 = st.columns(4)

# col1.metric("Tổng records", f"{total_records:,}")
# col2.metric("Số quốc gia", total_countries)
# col3.metric("Khoảng năm", f"{min_year} - {max_year}")
# col4.metric("Avg TMAX", f"{tmax_avg:.2f} °C" if pd.notna(tmax_avg) else "N/A")

# col5, col6, col7 = st.columns(3)

# col5.metric("Avg TMIN", f"{tmin_avg:.2f} °C" if pd.notna(tmin_avg) else "N/A")
# col6.metric("Avg PRCP", f"{prcp_avg:.2f}" if pd.notna(prcp_avg) else "N/A")
# col7.metric("Pipeline", "Airflow Auto ✅")

# st.divider()

# if filtered_df.empty:
#     st.warning("Không có dữ liệu phù hợp với bộ lọc.")
#     st.stop()

# # =========================
# # 1. GLOBAL CLIMATE TREND
# # =========================
# st.subheader("1. Xu hướng khí hậu theo năm")

# trend_df = (
#     filtered_df
#     .groupby(["year", "element"], as_index=False)
#     .agg(avg_value=("avg_value", "mean"))
# )

# fig_trend = px.line(
#     trend_df,
#     x="year",
#     y="avg_value",
#     color="element",
#     markers=True,
#     title="Xu hướng TMAX, TMIN, PRCP theo năm"
# )

# st.plotly_chart(fig_trend, use_container_width=True)

# # =========================
# # 2. TOP HOTTEST COUNTRIES
# # =========================
# st.subheader("2. Top 10 quốc gia có nhiệt độ cao nhất")

# hot_df = (
#     filtered_df[filtered_df["element"] == "TMAX"]
#     .groupby("country_name", as_index=False)
#     .agg(avg_tmax=("avg_value", "mean"))
#     .sort_values("avg_tmax", ascending=False)
#     .head(10)
# )

# fig_hot = px.bar(
#     hot_df,
#     x="avg_tmax",
#     y="country_name",
#     orientation="h",
#     title="Top 10 quốc gia có Avg TMAX cao nhất",
#     labels={"avg_tmax": "Avg TMAX (°C)", "country_name": "Quốc gia"}
# )

# fig_hot.update_layout(yaxis={"categoryorder": "total ascending"})
# st.plotly_chart(fig_hot, use_container_width=True)

# # =========================
# # 3. RAINFALL COMPARISON
# # =========================
# st.subheader("3. So sánh lượng mưa theo quốc gia")

# rain_df = (
#     filtered_df[filtered_df["element"] == "PRCP"]
#     .groupby("country_name", as_index=False)
#     .agg(avg_prcp=("avg_value", "mean"))
#     .sort_values("avg_prcp", ascending=False)
#     .head(10)
# )

# fig_rain = px.bar(
#     rain_df,
#     x="avg_prcp",
#     y="country_name",
#     orientation="h",
#     title="Top 10 quốc gia có lượng mưa trung bình cao nhất",
#     labels={"avg_prcp": "Avg PRCP", "country_name": "Quốc gia"}
# )

# fig_rain.update_layout(yaxis={"categoryorder": "total ascending"})
# st.plotly_chart(fig_rain, use_container_width=True)

# # =========================
# # 4. HEATMAP MONTHLY
# # =========================
# st.subheader("4. Heatmap khí hậu theo tháng")

# heat_df = (
#     filtered_df
#     .groupby(["year", "month", "element"], as_index=False)
#     .agg(avg_value=("avg_value", "mean"))
# )

# selected_heat_element = st.selectbox(
#     "Chọn chỉ số cho heatmap",
#     ["TMAX", "TMIN", "PRCP"],
#     index=0
# )

# heat_data = heat_df[heat_df["element"] == selected_heat_element]

# fig_heat = px.density_heatmap(
#     heat_data,
#     x="month",
#     y="year",
#     z="avg_value",
#     title=f"Heatmap {selected_heat_element} theo năm và tháng",
#     labels={"month": "Tháng", "year": "Năm", "avg_value": selected_heat_element}
# )

# st.plotly_chart(fig_heat, use_container_width=True)

# # =========================
# # 5. TEMPERATURE ANOMALY
# # =========================
# st.subheader("5. Temperature Anomaly")

# anomaly_df = (
#     filtered_df[filtered_df["element"] == "TMAX"]
#     .groupby("year", as_index=False)
#     .agg(avg_tmax=("avg_value", "mean"))
# )

# baseline = anomaly_df["avg_tmax"].mean()
# anomaly_df["anomaly"] = anomaly_df["avg_tmax"] - baseline

# fig_anomaly = px.bar(
#     anomaly_df,
#     x="year",
#     y="anomaly",
#     title="Độ lệch nhiệt độ TMAX so với trung bình toàn kỳ",
#     labels={"anomaly": "Temperature Anomaly (°C)", "year": "Năm"}
# )

# st.plotly_chart(fig_anomaly, use_container_width=True)

# # =========================
# # 6. COUNTRY DETAIL
# # =========================
# st.subheader("6. Phân tích chi tiết theo quốc gia")

# country_detail = st.selectbox(
#     "Chọn quốc gia để phân tích chi tiết",
#     countries
# )

# country_df = df[df["country_name"] == country_detail]

# country_trend = (
#     country_df
#     .groupby(["year", "element"], as_index=False)
#     .agg(avg_value=("avg_value", "mean"))
# )

# fig_country = px.line(
#     country_trend,
#     x="year",
#     y="avg_value",
#     color="element",
#     markers=True,
#     title=f"Xu hướng khí hậu tại {country_detail}"
# )

# st.plotly_chart(fig_country, use_container_width=True)

# # =========================
# # 7. RAW DATA
# # =========================
# st.subheader("7. Dữ liệu tổng hợp")

# st.dataframe(
#     filtered_df.sort_values(["year", "country_name", "element"]),
#     use_container_width=True,
#     height=400
# )

# csv = filtered_df.to_csv(index=False, encoding="utf-8-sig")
# st.download_button(
#     label="⬇️ Tải dữ liệu đã lọc",
#     data=csv,
#     file_name="filtered_weather_summary.csv",
#     mime="text/csv"
# )

# # =========================
# # FOOTER
# # =========================
# st.divider()
# st.caption(
#     "Big Data Pipeline: NOAA → Airflow → Spark ETL → SQL Server → Streamlit Dashboard"
# )

import os
import joblib
import pandas as pd
import streamlit as st
import plotly.express as px
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# =========================
# PAGE CONFIG
# =========================
st.set_page_config(
    page_title="Climate Big Data Dashboard",
    page_icon="🌍",
    layout="wide"
)

# =========================
# CONFIG
# =========================
SERVER = "localhost,1433"
DATABASE = "weather_db"
USERNAME = "airflow_user"
PASSWORD = "123456"
TABLE_NAME = "weather_summary"

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

MODEL_PATH = os.path.join(
    BASE_DIR,
    "scripts",
    "models",
    "weather_prediction_model.pkl"
)

connection_string = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={USERNAME};"
    f"PWD={PASSWORD};"
    "TrustServerCertificate=yes;"
)

params = quote_plus(connection_string)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# =========================
# LOAD DATA
# =========================
@st.cache_data(ttl=300)
def load_data():
    query = f"""
    SELECT
        country_name,
        country_code,
        element,
        year,
        month,
        avg_value,
        max_value,
        min_value,
        record_count
    FROM {TABLE_NAME}
    """
    df = pd.read_sql(query, engine)

    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)

    for col in ["avg_value", "max_value", "min_value", "record_count"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["country_name", "country_code", "element", "year", "month", "avg_value"])
    return df


@st.cache_resource
def load_model():

    BASE_DIR = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..")
    )

    MODEL_PATH = os.path.join(
        BASE_DIR,
        "scripts",
        "models",
        "weather_prediction_model.pkl"
    )

    st.write("BASE_DIR:", BASE_DIR)
    st.write("MODEL_PATH:", MODEL_PATH)

    if os.path.exists(MODEL_PATH):
        st.success("Đã tìm thấy model ✅")

        model = joblib.load(MODEL_PATH)

        st.success("Load model thành công ✅")

        return model

    else:
        st.error("Không tìm thấy model ❌")
        return None


df = load_data()
model = load_model()

# =========================
# HEADER
# =========================
st.title("🌍 Climate Big Data Analytics Dashboard")
st.caption("NOAA → Airflow → Spark ETL → SQL Server → Machine Learning → Streamlit Dashboard")

# =========================
# SIDEBAR
# =========================
st.sidebar.title("🔎 Bộ lọc dữ liệu")

years = sorted(df["year"].unique())
countries = sorted(df["country_name"].dropna().unique())
elements = ["TMAX", "TMIN", "PRCP"]

selected_years = st.sidebar.multiselect(
    "Chọn năm",
    years,
    default=years[-10:] if len(years) >= 10 else years
)

selected_countries = st.sidebar.multiselect(
    "Chọn quốc gia",
    countries,
    default=countries[:10] if len(countries) >= 10 else countries
)

selected_elements = st.sidebar.multiselect(
    "Chọn chỉ số khí hậu",
    elements,
    default=elements
)

filtered_df = df[
    (df["year"].isin(selected_years)) &
    (df["country_name"].isin(selected_countries)) &
    (df["element"].isin(selected_elements))
]

# =========================
# TABS
# =========================
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Tổng quan",
    "📈 Xu hướng khí hậu",
    "🤖 Dự đoán ML",
    "⚠️ Anomaly Detection",
    "🌍 Bản đồ khí hậu"
])

# =========================
# TAB 1: OVERVIEW
# =========================
with tab1:
    st.subheader("📊 Tổng quan dữ liệu khí hậu")

    total_records = int(filtered_df["record_count"].sum()) if not filtered_df.empty else 0
    total_countries = filtered_df["country_name"].nunique()
    min_year = filtered_df["year"].min() if not filtered_df.empty else "-"
    max_year = filtered_df["year"].max() if not filtered_df.empty else "-"

    avg_tmax = filtered_df[filtered_df["element"] == "TMAX"]["avg_value"].mean()
    avg_tmin = filtered_df[filtered_df["element"] == "TMIN"]["avg_value"].mean()
    avg_prcp = filtered_df[filtered_df["element"] == "PRCP"]["avg_value"].mean()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Tổng records", f"{total_records:,}")
    c2.metric("Số quốc gia", total_countries)
    c3.metric("Khoảng năm", f"{min_year} - {max_year}")
    c4.metric("Pipeline", "Airflow Auto ✅")

    c5, c6, c7 = st.columns(3)
    c5.metric("Avg TMAX", f"{avg_tmax:.2f} °C" if pd.notna(avg_tmax) else "N/A")
    c6.metric("Avg TMIN", f"{avg_tmin:.2f} °C" if pd.notna(avg_tmin) else "N/A")
    c7.metric("Avg PRCP", f"{avg_prcp:.2f}" if pd.notna(avg_prcp) else "N/A")

    st.divider()

    st.subheader("Dữ liệu sau khi lọc")
    st.dataframe(filtered_df, use_container_width=True, height=400)

# =========================
# TAB 2: TREND
# =========================
with tab2:
    st.subheader("📈 Xu hướng khí hậu theo thời gian")

    if filtered_df.empty:
        st.warning("Không có dữ liệu phù hợp.")
    else:
        trend_df = (
            filtered_df
            .groupby(["year", "element"], as_index=False)
            .agg(avg_value=("avg_value", "mean"))
        )

        fig_trend = px.line(
            trend_df,
            x="year",
            y="avg_value",
            color="element",
            markers=True,
            title="Xu hướng TMAX, TMIN, PRCP theo năm"
        )
        st.plotly_chart(fig_trend, use_container_width=True)

        col1, col2 = st.columns(2)

        with col1:
            hot_df = (
                filtered_df[filtered_df["element"] == "TMAX"]
                .groupby("country_name", as_index=False)
                .agg(avg_tmax=("avg_value", "mean"))
                .sort_values("avg_tmax", ascending=False)
                .head(10)
            )

            fig_hot = px.bar(
                hot_df,
                x="avg_tmax",
                y="country_name",
                orientation="h",
                title="Top 10 quốc gia nóng nhất",
                labels={"avg_tmax": "Avg TMAX", "country_name": "Quốc gia"}
            )
            fig_hot.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig_hot, use_container_width=True)

        with col2:
            rain_df = (
                filtered_df[filtered_df["element"] == "PRCP"]
                .groupby("country_name", as_index=False)
                .agg(avg_prcp=("avg_value", "mean"))
                .sort_values("avg_prcp", ascending=False)
                .head(10)
            )

            fig_rain = px.bar(
                rain_df,
                x="avg_prcp",
                y="country_name",
                orientation="h",
                title="Top 10 quốc gia mưa nhiều nhất",
                labels={"avg_prcp": "Avg PRCP", "country_name": "Quốc gia"}
            )
            fig_rain.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig_rain, use_container_width=True)

        st.subheader("Heatmap khí hậu năm - tháng")

        heat_element = st.selectbox(
            "Chọn chỉ số heatmap",
            elements,
            index=0
        )

        heat_df = (
            filtered_df[filtered_df["element"] == heat_element]
            .groupby(["year", "month"], as_index=False)
            .agg(avg_value=("avg_value", "mean"))
        )

        fig_heat = px.density_heatmap(
            heat_df,
            x="month",
            y="year",
            z="avg_value",
            title=f"Heatmap {heat_element} theo năm và tháng"
        )
        st.plotly_chart(fig_heat, use_container_width=True)

# =========================
# TAB 3: ML PREDICTION
# =========================
with tab3:
    st.subheader("🤖 Dự đoán khí hậu bằng Machine Learning")

    if model is None:
        st.error("Chưa tìm thấy model. Hãy train model trước: models/weather_prediction_model.pkl")
    else:
        st.success("Đã load model Random Forest thành công ✅")

        col1, col2, col3 = st.columns(3)

        with col1:
            pred_country = st.selectbox("Quốc gia", countries)

        country_code_map = (
            df[["country_name", "country_code"]]
            .drop_duplicates()
            .set_index("country_name")["country_code"]
            .to_dict()
        )

        pred_country_code = country_code_map.get(pred_country, "")

        with col2:
            pred_element = st.selectbox("Chỉ số khí hậu", elements)

        with col3:
            pred_year = st.number_input(
                "Năm cần dự đoán",
                min_value=int(df["year"].min()),
                max_value=2100,
                value=2027,
                step=1
            )

        pred_month = st.slider("Tháng", 1, 12, 1)

        # Lấy giá trị mặc định theo lịch sử
        base_df = df[
            (df["country_name"] == pred_country) &
            (df["element"] == pred_element) &
            (df["month"] == pred_month)
        ]

        if base_df.empty:
            base_df = df[
                (df["country_name"] == pred_country) &
                (df["element"] == pred_element)
            ]

        default_max = base_df["max_value"].mean()
        default_min = base_df["min_value"].mean()
        default_record = base_df["record_count"].mean()

        if pd.isna(default_max):
            default_max = df[df["element"] == pred_element]["max_value"].mean()

        if pd.isna(default_min):
            default_min = df[df["element"] == pred_element]["min_value"].mean()

        if pd.isna(default_record):
            default_record = df[df["element"] == pred_element]["record_count"].mean()

        st.markdown("### Tham số đầu vào mô hình")

        c1, c2, c3 = st.columns(3)

        with c1:
            input_max = st.number_input("Max value", value=float(default_max))

        with c2:
            input_min = st.number_input("Min value", value=float(default_min))

        with c3:
            input_record = st.number_input("Record count", value=float(default_record))

        if st.button("🔮 Dự đoán khí hậu"):
            input_df = pd.DataFrame([{
                "country_name": pred_country,
                "country_code": pred_country_code,
                "element": pred_element,
                "year": int(pred_year),
                "month": int(pred_month),
                "max_value": float(input_max),
                "min_value": float(input_min),
                "record_count": float(input_record)
            }])

            prediction = model.predict(input_df)[0]

            unit = "°C" if pred_element in ["TMAX", "TMIN"] else "PRCP"

            st.metric(
                label=f"Kết quả dự đoán {pred_element}",
                value=f"{prediction:.2f} {unit}"
            )

            st.info(
                f"Mô hình dự đoán {pred_element} tại {pred_country} "
                f"tháng {pred_month}/{pred_year} là {prediction:.2f} {unit}."
            )

            st.dataframe(input_df, use_container_width=True)

# =========================
# TAB 4: ANOMALY DETECTION
# =========================
with tab4:
    st.subheader("⚠️ Phát hiện bất thường khí hậu")

    anomaly_element = st.selectbox(
        "Chọn chỉ số anomaly",
        elements,
        index=0,
        key="anomaly_element"
    )

    anomaly_df = df[df["element"] == anomaly_element].copy()

    baseline_df = (
        anomaly_df
        .groupby(["country_name", "month"], as_index=False)
        .agg(
            baseline_mean=("avg_value", "mean"),
            baseline_std=("avg_value", "std")
        )
    )

    anomaly_df = anomaly_df.merge(
        baseline_df,
        on=["country_name", "month"],
        how="left"
    )

    anomaly_df["z_score"] = (
        (anomaly_df["avg_value"] - anomaly_df["baseline_mean"]) /
        anomaly_df["baseline_std"]
    )

    threshold = st.slider("Ngưỡng bất thường Z-score", 1.5, 4.0, 2.0, 0.1)

    anomaly_df["is_anomaly"] = anomaly_df["z_score"].abs() >= threshold

    total_anomalies = anomaly_df["is_anomaly"].sum()

    st.metric("Số điểm bất thường", int(total_anomalies))

    top_anomaly = (
        anomaly_df[anomaly_df["is_anomaly"]]
        .sort_values("z_score", key=abs, ascending=False)
        .head(20)
    )

    st.dataframe(
        top_anomaly[[
            "country_name",
            "country_code",
            "element",
            "year",
            "month",
            "avg_value",
            "baseline_mean",
            "z_score"
        ]],
        use_container_width=True
    )

    fig_anomaly = px.scatter(
        anomaly_df,
        x="year",
        y="avg_value",
        color="is_anomaly",
        hover_data=["country_name", "month", "z_score"],
        title=f"Anomaly Detection cho {anomaly_element}"
    )

    st.plotly_chart(fig_anomaly, use_container_width=True)

# =========================
# TAB 5: WORLD MAP
# =========================
with tab5:
    st.subheader("🌍 Bản đồ khí hậu theo quốc gia")

    map_element = st.selectbox(
        "Chọn chỉ số bản đồ",
        elements,
        index=0,
        key="map_element"
    )

    map_year = st.selectbox(
        "Chọn năm bản đồ",
        years,
        index=len(years) - 1
    )

    map_df = (
        df[
            (df["element"] == map_element) &
            (df["year"] == map_year)
        ]
        .groupby(["country_name", "country_code"], as_index=False)
        .agg(avg_value=("avg_value", "mean"))
    )

    fig_map = px.choropleth(
        map_df,
        locations="country_code",
        color="avg_value",
        hover_name="country_name",
        title=f"Bản đồ {map_element} năm {map_year}",
        color_continuous_scale="Turbo"
    )

    st.plotly_chart(fig_map, use_container_width=True)

# =========================
# FOOTER
# =========================
st.divider()
st.caption("Big Data Pipeline: NOAA → Airflow → Spark ETL → SQL Server → ML Prediction → Streamlit")