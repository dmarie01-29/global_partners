import streamlit as st
import pandas as pd
import s3fs

# Base S3 Path configuration
S3_METRICS_BASE = "s3://globalpartners-bucket/transformed/metrics/"

@st.cache_data(ttl=600)
def load_s3_metric_table(folder_name):
    """Safely loads Parquet data partitions using production cloud environment secrets."""
    try:
        # Pulls keys securely from Streamlit's built-in platform management environment
        fs = s3fs.S3FileSystem(
            key=st.secrets["aws_access_key_id"],
            secret=st.secrets["aws_secret_access_key"],
            anon=False
        )
        
        path = f"{S3_METRICS_BASE}{folder_name}/"
        df = pd.read_parquet(path, filesystem=fs)
        return df
    except Exception as e:
        st.error(f"Error loading {folder_name} from S3: {e}")
        return pd.DataFrame()


# --- HEADER SECTION ---
st.title("📊 GlobalPartners Analytics Data Lake Dashboard")
st.markdown("Real-time executive performance monitoring powered by AWS Glue & Amazon S3.")
st.write("---")

# --- NAVIGATION TABS ---
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "👥 Customer Segments", 
    "📈 Sales Trends", 
    "💎 Loyalty Program", 
    "📍 Location Performance", 
    "🏷️ Pricing & Discounts"
])

# =====================================================================
# TAB 1: CUSTOMER SEGMENTS & CHURN PROFILE
# =====================================================================
with tab1:
    st.header("Customer Segmentation (RFM Framework)")
    df_segments = load_s3_metric_table("customer_segments")
    
    if not df_segments.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Distribution of Customer Value Tiers")
            segment_counts = df_segments["rfm_segment"].value_counts().reset_index()
            segment_counts.columns = ["Segment", "Total Customers"]
            st.bar_chart(data=segment_counts, x="Segment", y="Total Customers", color="#2A629A")
            
        with col2:
            st.subheader("Churn Indicator Risk Alert Profile")
            churn_counts = df_segments["churn_indicator"].value_counts().reset_index()
            churn_counts.columns = ["Status", "Volume"]
            st.bar_chart(data=churn_counts, x="Status", y="Volume", color="#E74C3C")
            
        st.subheader("High-Risk/VIP Roster Inspection (Top 100 Rows)")
        st.dataframe(df_segments.sort_values(by="recency_days", ascending=False).head(100), use_container_width=True)

# =====================================================================
# TAB 2: SALES TRENDS MONITORING
# =====================================================================
with tab2:
    st.header("Sales Trends & Volumetric Heat Patterns")
    df_trends = load_s3_metric_table("sales_trends")
    
    if not df_trends.empty:
        st.subheader("Chronological Revenue Timeline by Category")
        
        # 1. Group, pivot, and fill empty slots with 0
        timeline_df = df_trends.groupby(["sales_date", "item_category"])["total_revenue"].sum().unstack().fillna(0)
        
        # 2. Force the index (dates) to standard strings to eliminate complex types
        timeline_df.index = timeline_df.index.astype(str)
        
        # 3. Force all table columns to standard floats to eliminate mixed type exceptions
        timeline_df = timeline_df.astype(float)
        
        # 4. Render the chart explicitly
        st.line_chart(timeline_df)
        
        st.subheader("Peak Demand Volume Window Matrix (By Hour of Day)")
        hourly_df = df_trends.groupby("sales_hour")["total_orders"].sum().reset_index()
        st.bar_chart(data=hourly_df, x="sales_hour", y="total_orders", color="#008DDA")

# =====================================================================
# TAB 3: LOYALTY PROGRAM IMPACT ANALYSIS
# =====================================================================
with tab3:
    st.header("Loyalty Tier ROI Performance Matrix")
    df_loyalty = load_s3_metric_table("loyalty_impact")
    
    if not df_loyalty.empty:
        # Remap boolean labels for clearer visualization display
        df_loyalty["is_loyalty"] = df_loyalty["is_loyalty"].map({True: "Loyalty Member", False: "Standard Guest"})
        
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Average Transaction Ticket Spend ($)")
            st.bar_chart(data=df_loyalty, x="is_loyalty", y="avg_order_spend", color="#41B06E")
        with col2:
            st.subheader("Aggregated Lifetime Value per Customer ($)")
            st.bar_chart(data=df_loyalty, x="is_loyalty", y="avg_lifetime_value_spend", color="#86B6F6")
            
        st.table(df_loyalty.set_index("is_loyalty"))

# =====================================================================
# TAB 4: TOP-PERFORMING LOCATIONS
# =====================================================================
with tab4:
    st.header("Store Location & Restaurant Performance League")
    df_locations = load_s3_metric_table("location_performance")
    
    if not df_locations.empty:
        df_sorted = df_locations.sort_values(by="total_revenue", ascending=False)
        
        st.subheader("Revenue Ranking Matrix by Restaurant ID")
        st.bar_chart(data=df_sorted, x="restaurant_id", y="total_revenue", color="#FF9F66")
        
        st.subheader("Complete Store Performance Metric Roster")
        st.dataframe(df_sorted, use_container_width=True)

# =====================================================================
# TAB 5: PRICING & DISCOUNT EFFECTIVENESS
# =====================================================================
with tab5:
    st.header("Discount Optimization & Elasticity Review")
    df_discounts = load_s3_metric_table("discount_effectiveness")
    
    if not df_discounts.empty:
        df_discounts["is_discounted"] = df_discounts["is_discounted"].map({True: "Promo Applied", False: "Full Price Purchase"})
        
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Basket Volume Distribution Shift")
            st.bar_chart(data=df_discounts, x="is_discounted", y="total_orders", color="#74E291")
        with col2:
            st.subheader("Average Basket Order Value Impact ($)")
            st.bar_chart(data=df_discounts, x="is_discounted", y="average_basket_value", color="#9400D3")
            
        st.table(df_discounts.set_index("is_discounted"))
