import pandas as pd
from sqlalchemy import create_engine, text, bindparam
import plotly.express as px
from dash import Dash, dcc, html, Output, Input, State
from datetime import datetime
import calendar

GREEN_PRIMARY = "#2E8B57"
GREEN_DARK = "#1F5D3A"
GREEN_LIGHT = "#7BC96F"
BLUE_SOFT = "#4C78A8"
TEAL = "#2A9D8F"
GOLD = "#E9C46A"
ORANGE = "#F4A261"
RED_SOFT = "#E76F51"
BAR_COLORS = [BLUE_SOFT, TEAL, GREEN_PRIMARY, GOLD, ORANGE, RED_SOFT]


engine = create_engine(
    "postgresql://postgres:1998@localhost:5432/parking_dwh"
)

app = Dash(__name__)

locations_df = pd.read_sql("SELECT DISTINCT location_group FROM dim_location", engine)
payments_df = pd.read_sql("SELECT DISTINCT payment_method FROM dim_payment", engine)


def format_hour_label(hour):
    return datetime.strptime(str(int(hour)), "%H").strftime("%I %p")

app.layout = html.Div([

    html.H1("Austin TX Parking OLAP Dashboard", style={"textAlign": "center"}),

    html.Div([
        dcc.DatePickerRange(
            id="date-range",
            start_date=datetime(2019,1,1),
            end_date=datetime(2026,1,31)
        ),

        dcc.Dropdown(
            id="payment-filter",
            options=[{"label": p, "value": p} for p in payments_df["payment_method"]],
            multi=True,
            placeholder="Select payment"
        ),

        dcc.Dropdown(
            id="location-filter",
            options=[{"label": l, "value": l} for l in locations_df["location_group"]],
            multi=True,
            placeholder="Select location"
        ),

        html.Button("Apply Filters", id="filter-btn")
    ], style={"display": "flex", "gap": "10px"}),

    # KPI
    html.Div([
        html.Div(id="kpi1"),
        html.Div(id="kpi2"),
        html.Div(id="kpi3"),
        html.Div(id="kpi4")
    ], style={"display": "flex", "justifyContent": "space-around", "margin": "20px"}),

    # OLAP Charts
    dcc.Graph(id="rollup"),
    dcc.Graph(id="slice"),
    dcc.Graph(id="dice"),
    dcc.Graph(id="pivot"),
    dcc.Graph(id="peak-hours-chart"),
    dcc.Graph(id="rain-chart"),
    dcc.Graph(id="temperature-chart"),

])


@app.callback(
    Output("rollup", "figure"),
    Output("slice", "figure"),
    Output("dice", "figure"),
    Output("pivot", "figure"),
    Output("kpi1", "children"),
    Output("kpi2", "children"),
    Output("kpi3", "children"),
    Output("kpi4", "children"),
    Output("peak-hours-chart", "figure"),
    Output("rain-chart", "figure"),
    Output("temperature-chart", "figure"),
    Input("filter-btn", "n_clicks"),
    State("date-range", "start_date"),
    State("date-range", "end_date"),
    State("payment-filter", "value"),
    State("location-filter", "value")
)

def update_dashboard(n, start_date, end_date, payments, locations):

    base_query = """
    SELECT 
        f.transaction_id,
        d.full_date,
        d.month,
        d.year,
        l.location_group,
        p.payment_method,
        t.hour,
        f.amount,
        f.duration,
        w.precipitation,
        w.temperature
    FROM fact_parking_transaction f
    JOIN dim_date d ON f.date_id = d.date_id
    JOIN dim_time t ON f.time_id = t.time_id
    JOIN dim_location l ON f.location_id = l.location_id
    JOIN dim_payment p ON f.payment_id = p.payment_id
    LEFT JOIN (
        SELECT
            date_id,
            AVG(precipitation) AS precipitation,
            AVG(temperature) AS temperature
        FROM dim_weather
        GROUP BY date_id
    ) w ON f.date_id = w.date_id
    WHERE d.full_date BETWEEN :start_date AND :end_date
    """

    params = {
        "start_date": start_date,
        "end_date": end_date
    }

    if payments:
        base_query += " AND p.payment_method IN :payments"
        params["payments"] = payments

    if locations:
        base_query += " AND l.location_group IN :locations"
        params["locations"] = locations

    query = text(base_query)

    if payments:
        query = query.bindparams(bindparam("payments", expanding=True))

    if locations:
        query = query.bindparams(bindparam("locations", expanding=True))

    df = pd.read_sql(query, engine, params=params)

   
    if df.empty:
        empty_fig = px.bar(title="No data")
        return empty_fig, empty_fig, empty_fig, empty_fig, "0", "0", "0", "0", empty_fig, empty_fig, empty_fig

    kpi1 = f"💰 Revenue: ${round(df['amount'].sum(),2)}"
    kpi2 = f"📊 Transactions: {len(df)}"
    avg_min = df["duration"].mean()
    kpi2 = f"Transactions: {df['transaction_id'].nunique():,}"
    hours = int(avg_min // 60)
    minutes = int(avg_min % 60)
    kpi3 = f"⏱ Avg Duration: {hours}h {minutes}min"

    best_revenue_df = (
        df.groupby(["year", "month", "location_group"], as_index=False)["amount"]
        .sum()
        .sort_values("amount", ascending=False)
    )
    best_row = best_revenue_df.iloc[0]
    best_month_name = calendar.month_name[int(best_row["month"])]
    kpi4 = (
        f"Best Revenue: {int(best_row['year'])} {best_month_name}, "
        f"{best_row['location_group']} (${best_row['amount']:,.0f})"
    )

    # ================= ROLL-UP =================
    rollup_df = df.groupby(["year","month"])["amount"].sum().reset_index()
    rollup_df["month_name"] = rollup_df["month"].apply(lambda m: calendar.month_name[int(m)])
    rollup_df["year"] = rollup_df["year"].astype(str)
    fig_rollup = px.bar(
        rollup_df,
        x="month_name",
        y="amount",
        color="year",
        title="Monthly Parking Revenue by Year (Roll-up)",
        text_auto=True,
        category_orders={"month_name": list(calendar.month_name[1:])},
        color_discrete_sequence=BAR_COLORS
    )
    fig_rollup.update_traces(texttemplate="$%{y:,.0f}", textposition="outside")
    fig_rollup.update_yaxes(tickprefix="$", tickformat=",.0f")

    # ================= SLICE =================
    slice_df = df.groupby("payment_method")["amount"].sum().reset_index()
    fig_slice = px.pie(
        slice_df,
        names="payment_method",
        values="amount",
        title="Revenue Share by Payment Method (Slice)",
        color_discrete_sequence=BAR_COLORS
    )
    fig_slice.update_traces(texttemplate="%{label}<br>%{percent}<br>$%{value:,.0f}")

    # ================= DICE =================
    dice_df = df.groupby(["location_group","payment_method"])["amount"].sum().reset_index()
    fig_dice = px.bar(
        dice_df,
        x="location_group",
        y="amount",
        color="payment_method",
        title="Revenue by Location and Payment Method (Dice)",
        text_auto=True,
        color_discrete_sequence=[GREEN_PRIMARY, TEAL, GOLD, ORANGE, RED_SOFT, BLUE_SOFT]
    )
    fig_dice.update_traces(texttemplate="$%{y:,.0f}", textposition="outside")
    fig_dice.update_yaxes(tickprefix="$", tickformat=",.0f")

    # ================= PIVOT =================
    pivot_df = df.groupby(["hour","location_group"])["amount"].sum().reset_index()
    fig_pivot = px.density_heatmap(
        pivot_df,
        x="hour",
        y="location_group",
        z="amount",
        title="Parking Revenue Heatmap by Hour and Location (Pivot)",
        text_auto=True,
        color_continuous_scale="YlGn"
    )
    fig_pivot.update_traces(texttemplate="$%{z:,.0f}")
    fig_pivot.update_coloraxes(colorbar_tickprefix="$", colorbar_tickformat=",.0f")

    # ================= PEAK HOURS =================
    peak_hours_df = (
        df.groupby("hour")["transaction_id"]
        .nunique()
        .reset_index(name="transactions")
        .sort_values("hour")
    )
    all_hours_df = pd.DataFrame({"hour": list(range(24))})
    peak_hours_df = all_hours_df.merge(peak_hours_df, on="hour", how="left").fillna({"transactions": 0})
    peak_hours_df["transactions"] = peak_hours_df["transactions"].astype(int)
    peak_hours_df["hour_label"] = peak_hours_df["hour"].apply(format_hour_label)
    fig_peak_hours = px.bar(
        peak_hours_df,
        x="hour_label",
        y="transactions",
        title="Peak Hours on Parking Activity (Roll-up)",
        text_auto=True,
        color_discrete_sequence=[GREEN_PRIMARY],
        labels={
            "hour_label": "Hour of day",
            "transactions": "Parking transactions"
        }
    )
    fig_peak_hours.update_traces(texttemplate="%{y:,.0f}", textposition="outside")
    fig_peak_hours.update_yaxes(tickformat=",.0f")

    # ================= 🌧 RAIN OLAP =================
    rain_df = df.copy()

    def rain_category(x):
        if x == 0:
            return "No Rain"
        elif x < 2:
            return "Light Rain"
        elif x < 5:
            return "Moderate Rain"
        else:
            return "Heavy Rain"

    rain_df["rain_type"] = rain_df["precipitation"].fillna(0).apply(rain_category)

    rain_grouped = (
        rain_df.groupby("rain_type", observed=False)
        .size()
        .reset_index(name="transactions")
    )
    fig_rain = px.bar(
        rain_grouped,
        x="rain_type",
        y="transactions",
        text_auto=True,
        color="rain_type",
        color_discrete_map={
            "No Rain": GREEN_LIGHT,
            "Light Rain": TEAL,
            "Moderate Rain": ORANGE,
            "Heavy Rain": RED_SOFT
        },
        labels={
            "rain_type": "Rain category",
            "transactions": "Total parking transactions"
        },
        title="🌧 Rain Impact on Parking",
    )

    fig_rain.update_traces(texttemplate="%{y:,.0f}", textposition="outside")
    fig_rain.update_yaxes(tickformat=",.0f")
    fig_rain.update_layout(showlegend=False)

    # ================= TEMPERATURE IMPACT =================
    temp_df = df.groupby("full_date").agg({
        "amount": "sum",
        "temperature": "mean"
    }).reset_index()

    temp_df = temp_df.dropna(subset=["temperature"])

    if temp_df.empty:
        fig_temperature = px.bar(title="Temperature impact on Parking")
    else:
        temp_bins = [-float("inf"), 10, 16, 24, 32, float("inf")]
        temp_labels = [
            "Cold (<40°F)",
            "Cool (40-59°F)",
            "Mild (60-74°F)",
            "Warm (75-89°F)",
            "Hot (90°F+)"
        ]

        temp_labels = [
            "Cold (<10C)",
            "Cool (10-15C)",
            "Mild (16-23C)",
            "Warm (24-31C)",
            "Hot (32C+)"
        ]

        temp_df["temperature_band"] = pd.cut(
            temp_df["temperature"],
            bins=temp_bins,
            labels=temp_labels,
            right=False
        )

        temp_grouped = (
            temp_df.groupby("temperature_band", observed=False)["amount"]
            .sum()
            .reset_index()
        )

        fig_temperature = px.bar(
            temp_grouped,
            x="temperature_band",
            y="amount",
            title="Temperature impact on Parking",
            text_auto=True,
            color="temperature_band",
            color_discrete_map={
                "Cold (<10C)": BLUE_SOFT,
                "Cool (10-15C)": TEAL,
                "Mild (16-23C)": GREEN_PRIMARY,
                "Warm (24-31C)": GOLD,
                "Hot (32C+)": RED_SOFT
            },
            labels={
                "temperature_band": "Temperature band",
                "amount": "Total parking revenue"
            }
        )
        fig_temperature.update_traces(texttemplate="$%{y:,.0f}", textposition="outside")
        fig_temperature.update_yaxes(tickprefix="$", tickformat=",.0f")
        fig_temperature.update_layout(showlegend=False)

    return (
        fig_rollup,
        fig_slice,
        fig_dice,
        fig_pivot,
        kpi1,
        kpi2,
        kpi3,
        kpi4,
        fig_peak_hours,
        fig_rain,
        fig_temperature,
    )


if __name__ == "__main__":
    app.run(debug=True)
