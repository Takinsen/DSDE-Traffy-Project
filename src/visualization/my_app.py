from pathlib import Path
from datetime import date, timedelta

import pandas as pd
import pydeck as pdk
import streamlit as st
import os 

if "mapbox" in st.secrets:
    os.environ["MAPBOX_API_KEY"] = st.secrets["mapbox"]["public_token"]

st.set_page_config(page_title="Bangkok Traffy Tickets", layout="wide")
st.title("🗺️ Bangkok Traffy Fondue Dashboard")
st.caption("สำรวจปัญหาที่แจ้งผ่านระบบ Traffy Fondue พร้อมแผนที่ โหมดการกรอง และสถิติเชิงลึก")

DATA_PATH = Path(__file__).resolve().parents[2] / "data" / "cleansed" / "bangkok_traffy_clean.csv"
DEFAULT_CENTER = {"latitude": 13.7563, "longitude": 100.5018}

MAP_STYLES = {
    "Dark": "mapbox://styles/mapbox/dark-v10",
    "Light": "mapbox://styles/mapbox/light-v10",
    "Road": "mapbox://styles/mapbox/streets-v11",
    "Satellite": "mapbox://styles/mapbox/satellite-v9",
}

STATE_COLORS = {
    "เสร็จสิ้น": [0, 148, 50, 180],
    "กำลังดำเนินการ": [252, 186, 3, 200],
    "รอรับเรื่อง": [252, 39, 40, 200],
    "ส่งต่อ": [156, 39, 176, 200],
    "ไม่สามารถแก้ไข": [220, 76, 70, 200],
}


def _parse_types(raw: str) -> list[str]:
    cleaned = str(raw or "").strip("{} ").strip()
    if not cleaned:
        return []
    return [token.strip() for token in cleaned.split(",") if token.strip()]


def _state_color(state: str) -> list[int]:
    return STATE_COLORS.get(state, [128, 128, 128, 180])


@st.cache_data(show_spinner=True)
def load_data(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df.dropna(subset=["latitude", "longitude"])
    df["timestamp_dt"] = pd.to_datetime(df["timestamp_dt"], errors="coerce")
    df["last_activity_dt"] = pd.to_datetime(df["last_activity_dt"], errors="coerce")
    df["count_reopen"] = pd.to_numeric(df["count_reopen"], errors="coerce").fillna(0).astype(int)
    df["type_tokens"] = df["type"].apply(_parse_types)
    df["state"] = df["state"].fillna("ไม่ทราบสถานะ")
    df["district"] = df["district"].fillna("ไม่ระบุเขต")
    df["subdistrict"] = df["subdistrict"].fillna("ไม่ระบุตำบล")
    df["organization"] = df["organization"].fillna("ไม่ระบุหน่วยงาน")
    df["state_color"] = df["state"].apply(_state_color)
    df["resolution_days"] = (
        (df["last_activity_dt"] - df["timestamp_dt"]).dt.total_seconds() / (60 * 60 * 24)
    )
    return df


df = load_data(DATA_PATH)

st.sidebar.header("ตัวกรองข้อมูล")
st.sidebar.caption("ปรับค่าสำหรับดูประเด็นที่สนใจบนแผนที่")

all_types = sorted({t for tokens in df["type_tokens"] for t in tokens if t})
selected_types = st.sidebar.multiselect("ประเภทปัญหา", options=all_types, default=all_types[:5] if len(all_types) > 5 else all_types)

states = sorted(df["state"].dropna().unique().tolist())
selected_states = st.sidebar.multiselect("สถานะการดำเนินงาน", options=states, default=states)

districts = sorted(df["district"].dropna().unique().tolist())
selected_districts = st.sidebar.multiselect("เขต", options=districts)

orgs = sorted(df["organization"].dropna().unique().tolist())
selected_orgs = st.sidebar.multiselect("หน่วยงาน", options=orgs)

min_ts = df["timestamp_dt"].min()
max_ts = df["timestamp_dt"].max()
min_date = min_ts.date() if pd.notnull(min_ts) else date(2021, 1, 1)
max_date = max_ts.date() if pd.notnull(max_ts) else date.today()
default_start = max_date - timedelta(days=180)
if default_start < min_date:
    default_start = min_date
date_range = st.sidebar.date_input(
    "วันที่แจ้งเรื่อง",
    value=(default_start, max_date),
    min_value=min_date,
    max_value=max_date,
)

map_style = st.sidebar.selectbox("รูปแบบแผนที่", list(MAP_STYLES.keys()), index=0)
point_radius = st.sidebar.slider("ขนาดจุด (เมตร)", min_value=50, max_value=600, value=200, step=10)
max_points = st.sidebar.slider("จำนวน Ticket สูงสุดที่แสดง", min_value=500, max_value=20000, value=5000, step=500)
center_mode = st.sidebar.radio("ตำแหน่งศูนย์กลางแผนที่", ["กลางกรุงเทพฯ", "ตามข้อมูลที่กรอง"], index=0)

filtered = df.copy()

if selected_types:
    filtered = filtered[
        filtered["type_tokens"].apply(lambda tokens: bool(set(tokens).intersection(set(selected_types))))
    ]
if selected_states:
    filtered = filtered[filtered["state"].isin(selected_states)]
if selected_districts:
    filtered = filtered[filtered["district"].isin(selected_districts)]
if selected_orgs:
    filtered = filtered[filtered["organization"].isin(selected_orgs)]
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
    filtered = filtered[
        (filtered["timestamp_dt"].dt.date >= start_date) & (filtered["timestamp_dt"].dt.date <= end_date)
    ]

st.subheader("แผนที่ปัญหา Traffy Fondue")
total_filtered = len(filtered)
display_df = filtered.sort_values(by="timestamp_dt", ascending=False).head(max_points).copy()
st.caption(f"แสดง {len(display_df):,} จาก {total_filtered:,} Tickets (จำกัดด้วยตัวกรองและเพดานการแสดงผล)")

if display_df.empty:
    st.warning("ไม่พบข้อมูลที่ตรงกับตัวกรองที่เลือก กรุณาปรับตัวกรองเพื่อดูข้อมูล")
else:
    if center_mode == "กลางกรุงเทพฯ":
        center_lat = DEFAULT_CENTER["latitude"]
        center_lon = DEFAULT_CENTER["longitude"]
        center_zoom = 11
    else:
        center_lat = (
            display_df["latitude"].mean()
            if not display_df["latitude"].isna().all()
            else DEFAULT_CENTER["latitude"]
        )
        center_lon = (
            display_df["longitude"].mean()
            if not display_df["longitude"].isna().all()
            else DEFAULT_CENTER["longitude"]
        )
        center_zoom = 10

    view_state = pdk.ViewState(
        latitude=center_lat,
        longitude=center_lon,
        zoom=center_zoom,
        min_zoom=8,
        max_zoom=16,
        pitch=40,
    )

    scatter_layer = pdk.Layer(
        "ScatterplotLayer",
        data=display_df,
        get_position="[longitude, latitude]",
        get_radius=point_radius,
        get_fill_color="state_color",
        pickable=True,
        stroked=True,
        get_line_color=[0, 0, 0, 120],
    )

    

    is_dark_map = map_style in {"Dark", "Satellite"}
    tooltip_bg = "rgba(30, 30, 30, 0.92)" if is_dark_map else "rgba(255, 255, 255, 0.95)"
    tooltip_text = "#DADADA" if is_dark_map else "#353535"
    tooltip_accent = "#FFFFFF" if is_dark_map else "#161616"

    tooltip_scatter = {
    "html": f"""
    <div style="padding:10px;border-radius:16px;background-color:{tooltip_bg};color:{tooltip_text};min-width:220px;">
        <div style="font-size:16px;font-weight:700;color:{tooltip_accent};margin-bottom:6px;">Ticket {'{ticket_id}'}</div>
        <div><strong>ประเภท:</strong> {{type}}</div>
        <div><strong>สถานะ:</strong> {{state}}</div>
        <div><strong>หน่วยงาน:</strong> {{organization}}</div>
        <div><strong>เขต:</strong> {{district}}</div>
        <div style="margin-top:6px;"><strong>รายละเอียด:</strong> {{comment}}</div>
    </div>
    """,
    "style": {"backgroundColor": "transparent", "color": tooltip_text},
    }

    tooltip_hexagon = {
        "html": f"""
        <div style="padding:10px;border-radius:16px;background-color:{tooltip_bg};color:{tooltip_text};min-width:150px;">
            <div style="font-size:16px;font-weight:700;color:{tooltip_accent};margin-bottom:6px;">พื้นที่หนาแน่น</div>
            <div><strong>จำนวนเรื่องร้องเรียน:</strong> {{elevationValue}} เรื่อง</div>
            <div style="font-size:12px;color:grey;margin-top:4px;">(พิกัด: {{position}})</div>
        </div>
        """,
        "style": {"backgroundColor": "transparent", "color": tooltip_text},
    }

    # Create a toggle for layer type
    layer_type = st.sidebar.radio("รูปแบบการแสดงผลแผนที่", ["จุด (Scatter)", "ความหนาแน่น (Hexagon)", "ความร้อน (Heatmap)"])

    layers = []

    if layer_type == "จุด (Scatter)":
        layers.append(
            pdk.Layer(
                "ScatterplotLayer",
                data=display_df,
                get_position="[longitude, latitude]",
                get_radius=point_radius,
                get_fill_color="state_color",
                pickable=True,
                stroked=True,
                get_line_color=[0, 0, 0, 120],
            )
        )

    elif layer_type == "ความหนาแน่น (Hexagon)":
        layers.append(
            pdk.Layer(
                "HexagonLayer",
                data=display_df,
                get_position="[longitude, latitude]",
                radius=200,          # Size of hexagons
                elevation_scale=2,   # Height multiplier
                elevation_range=[0, 1000],
                pickable=True,
                extruded=True,       # Make them 3D
            )
        )

    elif layer_type == "ความร้อน (Heatmap)":
        layers.append(
            pdk.Layer(
                "HeatmapLayer",
                data=display_df,
                get_position="[longitude, latitude]",
                opacity=0.9,
                get_weight=1,
                radius_pixels=50,    # Adjust for smoothness
            )
        )

    if layer_type == "จุด (Scatter)":
        current_tooltip = tooltip_scatter
    else:
        current_tooltip = tooltip_hexagon

    deck = pdk.Deck(
    map_style=MAP_STYLES[map_style], 
    initial_view_state=view_state, 
    layers=layers, 
    tooltip=current_tooltip 
)
    
st.subheader("แนวโน้มการแจ้งปัญหาตามช่วงเวลา")
# Resample data by day to count tickets over time
daily_counts = display_df.set_index("timestamp_dt").resample("D").size().rename("Ticket Count")

if not daily_counts.empty:
    st.line_chart(daily_counts, use_container_width=True)
else:
    st.info("ไม่เพียงพอสำหรับการแสดงกราฟเส้น")
st.pydeck_chart(deck, use_container_width=True)

st.subheader("สถิติภาพรวม")
col1, col2, col3 = st.columns(3)
col1.metric("จำนวน Ticket (ที่แสดง)", len(display_df))

resolved_mask = display_df["state"] == "เสร็จสิ้น"
resolution_rate = (resolved_mask.sum() / len(display_df) * 100) if len(display_df) else 0
col2.metric("อัตราปิดงาน", f"{resolution_rate:.1f}%")

avg_reopen = display_df["count_reopen"].mean() if len(display_df) else 0
col3.metric("การเปิดซ้ำเฉลี่ย", f"{avg_reopen:.2f}")

avg_resolution_days = display_df["resolution_days"].dropna().mean()
if pd.notnull(avg_resolution_days):
    st.caption(f"เวลาจากการแจ้งถึงกิจกรรมล่าสุดเฉลี่ย {avg_resolution_days:.1f} วัน")

top_types = (
    display_df[["ticket_id", "type_tokens"]]
    .explode("type_tokens")
    .dropna(subset=["type_tokens"])
    .groupby("type_tokens")
    .size()
    .sort_values(ascending=False)
    .head(10)
)

top_districts = (
    display_df.groupby("district")
    .size()
    .sort_values(ascending=False)
    .head(10)
)

chart_col1, chart_col2 = st.columns(2)
with chart_col1:
    st.markdown("**จำนวนแจ้งเรื่องตามประเภท**")
    if not top_types.empty:
        st.bar_chart(top_types, use_container_width=True)
    else:
        st.info("ไม่มีข้อมูลประเภทปัญหาให้แสดง")

with chart_col2:
    st.markdown("**Top 10 เขตที่ถูกรายงาน**")
    if not top_districts.empty:
        st.bar_chart(top_districts, use_container_width=True)
    else:
        st.info("ไม่มีข้อมูลเขตให้แสดง")

st.subheader("ตัวอย่างรายการ Ticket")
st.dataframe(
    display_df[
        [
            "ticket_id",
            "type",
            "organization",
            "district",
            "subdistrict",
            "state",
            "timestamp_dt",
            "comment",
        ]
    ].sort_values(by="timestamp_dt", ascending=False),
    use_container_width=True,
    height=350,
)