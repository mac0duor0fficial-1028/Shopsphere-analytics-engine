"""ShopSphere dashboards (Streamlit) - reads ANALYTICS.MARTS as the read-only REPORTER role."""

import os
from pathlib import Path

import pandas as pd
import snowflake.connector
import streamlit as st
from cryptography.hazmat.primitives import serialization

st.set_page_config(page_title="ShopSphere Analytics", page_icon="🛒", layout="wide")


@st.cache_resource
def get_connection():
    key = serialization.load_pem_private_key(
        Path(os.environ["BI_PRIVATE_KEY_PATH"]).read_bytes(),
        os.environ.get("BI_PRIVATE_KEY_PASSPHRASE", "").encode() or None,
    )
    der = key.private_bytes(
        serialization.Encoding.DER, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ.get("BI_SNOWFLAKE_USER", "SVC_BI"),
        private_key=der,
        role="REPORTER",
        warehouse="BI_WH",
        database="SHOPSPHERE_ANALYTICS",
        schema="MARTS",
    )


@st.cache_data(ttl=60, show_spinner=False)
def q(sql: str) -> pd.DataFrame:
    cur = get_connection().cursor()
    cur.execute(sql)
    df = cur.fetch_pandas_all()
    df.columns = [c.lower() for c in df.columns]
    return df


st.title("🛒 ShopSphere Analytics")
days = st.sidebar.slider("Look-back (days)", 7, 180, 30)
st.sidebar.caption("Data refreshes about every 15 minutes (Airbyte CDC micro-batches + dbt).")

tab_kpi, tab_products, tab_customers, tab_funnel, tab_inventory, tab_health = st.tabs(
    [
        "Executive KPIs",
        "Sales & Products",
        "Customers & Cohorts",
        "Funnel",
        "Inventory & Ops",
        "Pipeline health",
    ]
)

with tab_kpi:
    daily = q(
        f"""
        select order_date,
               sum(gross_revenue) as gross_revenue, sum(revenue_orders) as revenue_orders,
               sum(orders_placed) as orders_placed, sum(cancelled_orders) as cancelled_orders,
               sum(refunded_amount) as refunded_amount
        from mart_daily_revenue where order_date >= dateadd(day, -{days}, current_date())
        group by order_date order by order_date"""
    )
    if daily.empty:
        st.info("No data yet - run the pipeline.")
    else:
        rev, orders = float(daily.gross_revenue.sum()), int(daily.revenue_orders.sum())
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Gross revenue", f"${rev:,.0f}")
        c2.metric("Revenue orders", f"{orders:,}")
        c3.metric("Average order value", f"${rev / orders:,.2f}" if orders else "-")
        c4.metric(
            "Cancellation rate",
            f"{daily.cancelled_orders.sum() / max(daily.orders_placed.sum(), 1):.1%}",
        )
        st.line_chart(daily.set_index("order_date")["gross_revenue"])
        by_channel = q(
            f"""select channel, sum(gross_revenue) as gross_revenue from mart_daily_revenue
                           where order_date >= dateadd(day, -{days}, current_date()) group by channel"""
        )
        st.bar_chart(by_channel.set_index("channel"))
        hourly = q(
            """select order_hour_utc, sum(gross_revenue) as gross_revenue from mart_hourly_revenue
                      where order_hour_utc >= dateadd(hour, -48, current_timestamp()::timestamp_ntz)
                      group by 1 order by 1"""
        )
        st.subheader("Last 48 hours (UTC)")
        st.area_chart(hourly.set_index("order_hour_utc"))

with tab_products:
    perf = q("select * from mart_product_performance order by revenue desc")
    left, right = st.columns(2)
    left.subheader("Top 10 products")
    left.dataframe(
        perf[perf.is_top_10][["product_name", "category", "units_sold", "revenue"]], hide_index=True
    )
    right.subheader("Bottom 10 products")
    right.dataframe(
        perf[perf.is_bottom_10][["product_name", "category", "units_sold", "revenue"]],
        hide_index=True,
    )
    st.subheader("Revenue by category")
    st.bar_chart(perf.groupby("category")["revenue"].sum().astype(float))

with tab_customers:
    ltv = q("select * from mart_customer_ltv")
    c1, c2, c3 = st.columns(3)
    c1.metric("Customers", f"{len(ltv):,}")
    c2.metric("Repeat-purchase rate", f"{ltv.is_repeat_customer.mean():.1%}" if len(ltv) else "-")
    c3.metric(
        "Avg lifetime value", f"${float(ltv.lifetime_revenue.mean()):,.2f}" if len(ltv) else "-"
    )
    st.bar_chart(ltv.customer_segment.value_counts())
    coh = q(
        "select cohort_month, months_since_first_order, retention_rate from mart_cohort_retention"
    )
    if not coh.empty:
        st.subheader("Cohort retention")
        pivot = coh.pivot(
            index="cohort_month", columns="months_since_first_order", values="retention_rate"
        )
        st.dataframe(
            pivot.astype(float).style.format("{:.0%}", na_rep="").background_gradient(cmap="Blues")
        )

with tab_funnel:
    f = q(
        f"""select sum(viewed_sessions) v, sum(cart_sessions) c, sum(checkout_sessions) k, sum(purchase_sessions) p
              from mart_funnel where session_date >= dateadd(day, -{days}, current_date())"""
    )
    if not f.empty and f.v.iloc[0]:
        steps = pd.DataFrame(
            {
                "stage": ["Viewed", "Added to cart", "Began checkout", "Purchased"],
                "sessions": [
                    int(f.v.iloc[0]),
                    int(f.c.iloc[0]),
                    int(f.k.iloc[0]),
                    int(f.p.iloc[0]),
                ],
            }
        )
        st.bar_chart(steps.set_index("stage"))
        st.metric("Overall conversion", f"{steps.sessions.iloc[3] / steps.sessions.iloc[0]:.1%}")

with tab_inventory:
    inv = q("select * from dim_products order by stock_qty")
    c1, c2 = st.columns(2)
    c1.metric("Out of stock", int((inv.stock_status == "out_of_stock").sum()))
    c2.metric("Low stock", int((inv.stock_status == "low_stock").sum()))
    st.dataframe(
        inv[inv.stock_status != "in_stock"][
            ["sku", "product_name", "category", "stock_qty", "stock_status"]
        ],
        hide_index=True,
    )

with tab_health:
    h = q(
        """select max(order_last_extracted_at) as last_extracted, max(order_ts) as last_order,
                    count_if(not is_total_consistent and not is_deleted) as inconsistent_orders
             from fct_orders"""
    )
    st.dataframe(h, hide_index=True)
    st.caption(
        "Freshness SLA: core marts within 15 minutes of the source. "
        "Full pipeline status lives in the Dagster UI."
    )
