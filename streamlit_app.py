"""
📊 Rating Monitor — MR.EQUIPP
Streamlit Cloud app. Подключается к Heroku PostgreSQL.

Secrets (в Streamlit Cloud → Settings → Secrets):
  DATABASE_URL = "postgresql://..."
"""

import streamlit as st
import pandas as pd
import psycopg2
import psycopg2.extras
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta, date

# ── Настройки ──
st.set_page_config(page_title="Rating Monitor", page_icon="📊", layout="wide")

BRANDS = ["Woolcano", "Iguana", "BEENIUBEE", "YESWELL", "Forvevo"]
COUNTRIES = ["US", "CA", "DE", "GB", "FR", "IT", "ES", "NL"]


# ══════════════════════════════════════════════════════════════
#  DB Connection — через st.secrets (Streamlit Cloud)
# ══════════════════════════════════════════════════════════════
@st.cache_resource
def get_connection():
    url = st.secrets["DATABASE_URL"]
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url)


def run_query(query: str, params=None) -> pd.DataFrame:
    try:
        conn = get_connection()
        return pd.read_sql(query, conn, params=params)
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        st.cache_resource.clear()
        conn = get_connection()
        return pd.read_sql(query, conn, params=params)


# ══════════════════════════════════════════════════════════════
#  Data Loaders
# ══════════════════════════════════════════════════════════════
def load_zone_summary() -> pd.DataFrame:
    return run_query("SELECT zone, cnt FROM v_zone_summary")


def load_latest_snapshots() -> pd.DataFrame:
    return run_query("""
        SELECT asin, country, brand, title, rating, bsr, reviews_count,
               price, quality_score, collected_date
        FROM v_latest_snapshots
        ORDER BY rating ASC NULLS LAST
    """)


def load_alerts(target_date: date = None) -> pd.DataFrame:
    if target_date is None:
        target_date = date.today()
    return run_query("""
        SELECT asin, country, brand, zone, rating, streak_days,
               max_streak_days, rating_trend, bsr, alert_date
        FROM rating_alerts
        WHERE alert_date = %s AND NOT resolved
        ORDER BY
            CASE zone WHEN 'red' THEN 1 WHEN 'yellow' THEN 2 ELSE 3 END,
            streak_days DESC
    """, (target_date,))


def load_rating_history(days: int = 30, asin_filter=None,
                        country_filter=None, brand_filter=None) -> pd.DataFrame:
    since = date.today() - timedelta(days=days)
    query = """
        SELECT asin, country, brand, rating, bsr, reviews_count,
               price, collected_date
        FROM rating_snapshots
        WHERE collected_date >= %s AND rating IS NOT NULL
    """
    params = [since]

    if asin_filter:
        query += " AND asin = ANY(%s)"
        params.append(asin_filter)
    if country_filter and country_filter != "Все":
        query += " AND country = %s"
        params.append(country_filter)
    if brand_filter and brand_filter != "Все":
        query += " AND brand = %s"
        params.append(brand_filter)

    query += " ORDER BY collected_date"
    return run_query(query, params)


def load_credit_usage(days: int = 30) -> pd.DataFrame:
    since = date.today() - timedelta(days=days)
    return run_query("""
        SELECT run_date, credits_used, successful_requests,
               failed_requests, efficiency_pct, remaining_credits
        FROM api_credit_usage
        WHERE run_date >= %s
        ORDER BY run_date
    """, (since,))


def load_portfolio_avg(days: int = 30) -> pd.DataFrame:
    since = date.today() - timedelta(days=days)
    return run_query("""
        SELECT collected_date,
               ROUND(AVG(rating)::numeric, 2) AS avg_rating,
               COUNT(*) AS total_asins
        FROM rating_snapshots
        WHERE collected_date >= %s AND rating IS NOT NULL
        GROUP BY collected_date
        ORDER BY collected_date
    """, (since,))


# ══════════════════════════════════════════════════════════════
#  UI
# ══════════════════════════════════════════════════════════════
def main():
    st.title("📊 Rating Monitor")
    st.caption("MR.EQUIPP — мониторинг рейтингов Parent ASIN")

    # ── Фильтры ──
    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        country_filter = st.selectbox("Страна", ["Все"] + COUNTRIES)
    with col_f2:
        brand_filter = st.selectbox("Бренд", ["Все"] + BRANDS)
    with col_f3:
        period = st.selectbox("Период", [7, 14, 30, 60, 90], index=2,
                              format_func=lambda x: f"{x} дней")

    # ── Метрики ──
    zones = load_zone_summary()
    zone_map = dict(zip(zones["zone"], zones["cnt"])) if not zones.empty else {}

    total = sum(zone_map.values())
    red_count = zone_map.get("red", 0)
    yellow_count = zone_map.get("yellow", 0)
    green_count = zone_map.get("green", 0)
    unknown_count = zone_map.get("unknown", 0)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Всего ASIN", total)
    col2.metric("🔴 Красная зона", red_count)
    col3.metric("🟡 Жёлтая зона", yellow_count)
    col4.metric("✅ В норме", green_count + unknown_count)

    st.divider()

    # ── Алерты ──
    st.subheader("Алерты — красная и жёлтая зоны")
    alerts = load_alerts()

    if alerts.empty:
        st.success("Все товары в зелёной зоне! 🎉")
    else:
        if country_filter != "Все":
            alerts = alerts[alerts["country"] == country_filter]
        if brand_filter != "Все":
            alerts = alerts[alerts["brand"] == brand_filter]

        if alerts.empty:
            st.info("Нет алертов для выбранных фильтров.")
        else:
            def color_zone(val):
                if val == "red":
                    return "background-color: #FCEBEB; color: #A32D2D; font-weight: bold"
                elif val == "yellow":
                    return "background-color: #FAEEDA; color: #854F0B; font-weight: bold"
                return ""

            def color_rating(val):
                try:
                    v = float(val)
                    if v < 4.3:
                        return "color: #A32D2D; font-weight: bold"
                    elif v == 4.3:
                        return "color: #854F0B; font-weight: bold"
                except (ValueError, TypeError):
                    pass
                return ""

            def color_trend(val):
                try:
                    v = float(val)
                    if v < 0:
                        return "color: #A32D2D"
                    elif v > 0:
                        return "color: #3B6D11"
                except (ValueError, TypeError):
                    pass
                return ""

            display_df = alerts[[
                "zone", "asin", "brand", "country", "rating",
                "streak_days", "max_streak_days", "rating_trend", "bsr"
            ]].copy()
            display_df.columns = [
                "Зона", "ASIN", "Бренд", "Страна", "Рейтинг",
                "Дней подряд", "Макс streak", "Тренд 7д", "BSR"
            ]

            styled = display_df.style.map(
                color_zone, subset=["Зона"]
            ).map(
                color_rating, subset=["Рейтинг"]
            ).map(
                color_trend, subset=["Тренд 7д"]
            ).format({
                "Рейтинг": "{:.1f}",
                "Тренд 7д": "{:+.1f}",
                "BSR": lambda x: f"{x:,.0f}" if pd.notna(x) else "N/A",
            })

            st.dataframe(styled, use_container_width=True, hide_index=True)

    st.divider()

    # ── Тренд рейтингов ──
    st.subheader("Тренд рейтингов")

    problem_asins = None
    if not alerts.empty:
        problem_asins = alerts["asin"].unique().tolist()

    history = load_rating_history(
        days=period,
        asin_filter=problem_asins,
        country_filter=country_filter,
        brand_filter=brand_filter,
    )

    if history.empty:
        st.info("Нет данных за выбранный период.")
    else:
        history["label"] = history["asin"] + " (" + history["country"] + ")"

        fig_rating = px.line(
            history, x="collected_date", y="rating", color="label",
            markers=False,
            labels={"collected_date": "", "rating": "Рейтинг", "label": "ASIN"},
        )
        fig_rating.add_hline(y=4.3, line_dash="dash", line_color="#E24B4A",
                             annotation_text="4.3 — красная",
                             annotation_position="top right")
        fig_rating.add_hline(y=4.4, line_dash="dash", line_color="#EF9F27",
                             annotation_text="4.4 — жёлтая",
                             annotation_position="top right")
        fig_rating.update_layout(
            height=400,
            yaxis=dict(range=[3.5, 5.0]),
            legend=dict(orientation="h", yanchor="bottom", y=-0.3),
            margin=dict(l=0, r=0, t=20, b=0),
        )
        st.plotly_chart(fig_rating, use_container_width=True)

    st.divider()

    # ── BSR тренд ──
    st.subheader("BSR тренд")

    bsr_history = load_rating_history(
        days=period, asin_filter=problem_asins,
        country_filter=country_filter, brand_filter=brand_filter,
    )
    bsr_data = bsr_history[bsr_history["bsr"].notna()].copy()

    if bsr_data.empty:
        st.info("Нет данных BSR.")
    else:
        bsr_data["label"] = bsr_data["asin"] + " (" + bsr_data["country"] + ")"
        fig_bsr = px.line(
            bsr_data, x="collected_date", y="bsr", color="label",
            markers=False,
            labels={"collected_date": "", "bsr": "BSR (ниже = лучше)", "label": "ASIN"},
        )
        fig_bsr.update_layout(
            height=350,
            yaxis=dict(autorange="reversed"),
            legend=dict(orientation="h", yanchor="bottom", y=-0.3),
            margin=dict(l=0, r=0, t=20, b=0),
        )
        st.plotly_chart(fig_bsr, use_container_width=True)

    st.divider()

    # ── Нижние метрики ──
    col_b1, col_b2 = st.columns(2)

    with col_b1:
        st.subheader("Средний рейтинг портфеля")
        portfolio = load_portfolio_avg(days=period)
        if not portfolio.empty:
            current_avg = portfolio.iloc[-1]["avg_rating"]
            prev_avg = portfolio.iloc[0]["avg_rating"] if len(portfolio) > 1 else current_avg
            delta = round(float(current_avg) - float(prev_avg), 2)
            st.metric("Средний рейтинг", f"{current_avg}", f"{delta:+.2f} за период")

            fig_avg = px.line(
                portfolio, x="collected_date", y="avg_rating",
                labels={"collected_date": "", "avg_rating": "Avg"},
            )
            fig_avg.update_layout(
                height=200, margin=dict(l=0, r=0, t=10, b=0),
                showlegend=False, yaxis=dict(range=[4.0, 5.0]),
            )
            fig_avg.update_traces(line_color="#534AB7")
            st.plotly_chart(fig_avg, use_container_width=True)
        else:
            st.info("Нет данных.")

    with col_b2:
        st.subheader("ScrapingDog кредиты")
        credits = load_credit_usage(days=period)
        if not credits.empty:
            latest = credits.iloc[-1]
            remaining = int(latest["remaining_credits"])
            used_today = int(latest["credits_used"])
            efficiency = latest["efficiency_pct"]

            st.metric("Остаток кредитов", f"{remaining:,}")
            col_c1, col_c2 = st.columns(2)
            col_c1.metric("Использовано", used_today)
            col_c2.metric("Эффективность", f"{efficiency}%")

            fig_credits = px.bar(
                credits, x="run_date", y="credits_used",
                labels={"run_date": "", "credits_used": "Кредиты"},
            )
            fig_credits.update_layout(
                height=200, margin=dict(l=0, r=0, t=10, b=0), showlegend=False,
            )
            fig_credits.update_traces(marker_color="#1D9E75")
            st.plotly_chart(fig_credits, use_container_width=True)
        else:
            st.info("Нет данных.")

    st.divider()

    # ── Полная таблица ──
    with st.expander("📋 Все ASIN — полная таблица", expanded=False):
        all_data = load_latest_snapshots()
        if country_filter != "Все":
            all_data = all_data[all_data["country"] == country_filter]
        if brand_filter != "Все":
            all_data = all_data[all_data["brand"] == brand_filter]

        if all_data.empty:
            st.info("Нет данных.")
        else:
            def color_full_rating(val):
                try:
                    v = float(val)
                    if v >= 4.4:
                        return "background-color: #EAF3DE; color: #27500A"
                    elif v >= 4.3:
                        return "background-color: #FAEEDA; color: #633806"
                    else:
                        return "background-color: #FCEBEB; color: #791F1F"
                except (ValueError, TypeError):
                    return ""

            display_all = all_data[[
                "asin", "country", "brand", "title", "rating",
                "bsr", "reviews_count", "price", "quality_score", "collected_date"
            ]].copy()
            display_all.columns = [
                "ASIN", "Страна", "Бренд", "Название", "Рейтинг",
                "BSR", "Отзывы", "Цена", "Качество", "Дата"
            ]
            styled_all = display_all.style.map(
                color_full_rating, subset=["Рейтинг"]
            ).format({
                "Рейтинг": lambda x: f"{x:.1f}" if pd.notna(x) else "N/A",
                "BSR": lambda x: f"{x:,.0f}" if pd.notna(x) else "N/A",
                "Отзывы": lambda x: f"{x:,.0f}" if pd.notna(x) else "N/A",
                "Качество": lambda x: f"{x}/100" if pd.notna(x) else "N/A",
            })
            st.dataframe(styled_all, use_container_width=True, hide_index=True)

    # ── Детальный просмотр ──
    with st.expander("🔍 Детальный просмотр ASIN", expanded=False):
        all_asins = load_latest_snapshots()
        if not all_asins.empty:
            asin_options = sorted(all_asins["asin"].unique())
            selected_asin = st.selectbox("Выбери ASIN", asin_options)

            if selected_asin:
                detail = run_query("""
                    SELECT collected_date, rating, bsr, reviews_count, price, quality_score
                    FROM rating_snapshots
                    WHERE asin = %s
                    ORDER BY collected_date DESC
                    LIMIT 90
                """, (selected_asin,))

                if not detail.empty:
                    lr = detail.iloc[0]
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Рейтинг", f"{lr['rating']:.1f}" if pd.notna(lr['rating']) else "N/A")
                    c2.metric("BSR", f"{lr['bsr']:,.0f}" if pd.notna(lr['bsr']) else "N/A")
                    c3.metric("Отзывы", f"{lr['reviews_count']:,.0f}" if pd.notna(lr['reviews_count']) else "N/A")
                    c4.metric("Цена", lr['price'] or "N/A")

                    ds = detail.sort_values("collected_date")
                    fig_d = go.Figure()
                    fig_d.add_trace(go.Scatter(
                        x=ds["collected_date"], y=ds["rating"],
                        mode="lines+markers", name="Рейтинг",
                        line=dict(color="#534AB7", width=2),
                    ))
                    fig_d.add_hline(y=4.3, line_dash="dash", line_color="#E24B4A")
                    fig_d.add_hline(y=4.4, line_dash="dash", line_color="#EF9F27")
                    fig_d.update_layout(
                        height=300, yaxis=dict(range=[3.5, 5.0], title="Рейтинг"),
                        xaxis=dict(title=""), margin=dict(l=0, r=0, t=20, b=0),
                    )
                    st.plotly_chart(fig_d, use_container_width=True)

    # ── Футер ──
    st.caption(f"Обновлено: {datetime.now().strftime('%d.%m.%Y %H:%M')} | "
               f"Rating Monitor v2 — PostgreSQL")


if __name__ == "__main__":
    main()
