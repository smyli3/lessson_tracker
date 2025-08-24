#!/usr/bin/env python3
"""
Streamlit app for snowsports school analytics.
Provides upload, dashboards, streak analysis, and data browsing.
"""

import streamlit as st
import duckdb
import polars as pl
import pandas as pd
from pathlib import Path
import io
from datetime import datetime
import sys
import os
import altair as alt

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from ingest import ingest_csv, setup_database


def _check_password() -> bool:
    """Simple password gate using Streamlit secrets.

    Set APP_PASSWORD in `.streamlit/secrets.toml` locally or in Streamlit Cloud.
    """
    # If no password configured, allow access (development convenience)
    app_pw = st.secrets.get("APP_PASSWORD", None)
    if not app_pw:
        return True

    if "_authed" in st.session_state and st.session_state.get("_authed") is True:
        return True

    def _do_login():
        entered = st.session_state.get("_pw", "")
        if entered == app_pw:
            st.session_state["_authed"] = True
        else:
            st.session_state["_authed"] = False
            st.error("Incorrect password. Try again.")

    st.warning("This app is password protected.")
    st.text_input("Password", type="password", key="_pw")
    st.button("Log in", on_click=_do_login)
    return st.session_state.get("_authed", False)


def migrate_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Ensure required derived columns exist for legacy databases.

    Adds missing columns used by the app so queries referencing them don't fail.
    """
    try:
        conn.execute("ALTER TABLE bookings ADD COLUMN IF NOT EXISTS instructor VARCHAR")
        conn.execute("ALTER TABLE bookings ADD COLUMN IF NOT EXISTS is_teaching BOOLEAN")
        conn.execute("ALTER TABLE bookings ADD COLUMN IF NOT EXISTS date DATE")
        conn.execute("ALTER TABLE bookings ADD COLUMN IF NOT EXISTS start_time TIME")
        conn.execute("ALTER TABLE bookings ADD COLUMN IF NOT EXISTS end_time TIME")
        conn.execute("ALTER TABLE bookings ADD COLUMN IF NOT EXISTS age_band VARCHAR")
        conn.execute("ALTER TABLE bookings ADD COLUMN IF NOT EXISTS level VARCHAR")
        conn.execute("ALTER TABLE bookings ADD COLUMN IF NOT EXISTS task_category VARCHAR")
        conn.execute("ALTER TABLE bookings ADD COLUMN IF NOT EXISTS week INTEGER")
        conn.execute("ALTER TABLE bookings ADD COLUMN IF NOT EXISTS booking_id VARCHAR")
        conn.execute("ALTER TABLE bookings ADD COLUMN IF NOT EXISTS age_inferred INTEGER")
        conn.execute("ALTER TABLE bookings ADD COLUMN IF NOT EXISTS ability_hint VARCHAR")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_booking_id ON bookings(booking_id)")
    except Exception:
        # Best effort; missing table or other issues will be handled elsewhere
        pass


def init_database():
    """Initialize database connection."""
    if 'db_conn' not in st.session_state:
        st.session_state.db_conn = setup_database('flaik.duckdb')
    # Always run lightweight migrations to ensure columns exist on older DBs
    migrate_schema(st.session_state.db_conn)
    return st.session_state.db_conn


def get_database_stats():
    """Get basic database statistics."""
    conn = init_database()
    try:
        total_rows = conn.execute("SELECT COUNT(*) FROM bookings").fetchone()[0]
        date_range = conn.execute("""
            SELECT MIN(date) as min_date, MAX(date) as max_date 
            FROM bookings WHERE date IS NOT NULL
        """).fetchone()
        return total_rows, date_range
    except:
        return 0, (None, None)


def upload_tab():
    """File upload and ingestion tab."""
    st.header("📁 Upload Daily Hill CSV")
    
    # Database stats
    total_rows, (min_date, max_date) = get_database_stats()
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Records", f"{total_rows:,}")
    with col2:
        st.metric("Date Range", f"{min_date or 'N/A'}")
    with col3:
        st.metric("Latest Date", f"{max_date or 'N/A'}")
    
    st.markdown("---")
    
    uploaded_file = st.file_uploader(
        "Choose a Daily Hill CSV file",
        type=['csv'],
        help="Upload your daily hill sheet CSV. The app will automatically detect headers and handle duplicates."
    )
    
    if uploaded_file is not None:
        # Save uploaded file temporarily
        temp_path = f"temp_{uploaded_file.name}"
        with open(temp_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        if st.button("Process Upload", type="primary"):
            with st.spinner("Processing CSV..."):
                try:
                    # Reuse the existing DB connection to avoid file lock conflicts
                    conn = init_database()
                    rows_inserted = ingest_csv(temp_path, conn=conn)
                    if rows_inserted > 0:
                        st.success(f"✅ Successfully inserted {rows_inserted} new records!")
                    else:
                        st.info("ℹ️ No new records found (all data already exists)")
                    
                    # Refresh stats
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"❌ Error processing file: {str(e)}")
                finally:
                    # Clean up temp file
                    if os.path.exists(temp_path):
                        os.remove(temp_path)

    st.markdown("---")
    st.subheader("🛠️ Admin: Recompute Categories (Backfill)")
    st.caption("Use this if earlier ingests were categorized with the old logic. This will update age_band, level, and task_category for existing rows.")
    if st.button("Recompute categories", type="secondary"):
        try:
            conn = init_database()
            with st.spinner("Recomputing age_band, level, and task_category across existing rows..."):
                # Age band backfill using fallback of task_name to task_type when invalid
                update_age_band = """
                UPDATE bookings
                SET age_band = CASE
                  WHEN (
                    CASE
                      WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type
                      ELSE task_name
                    END
                  ) LIKE '% KD %'
                  OR (
                    CASE
                      WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type
                      ELSE task_name
                    END
                  ) LIKE '% KD'
                  OR (
                    CASE
                      WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type
                      ELSE task_name
                    END
                  ) LIKE '%- KD%'
                  OR (
                    CASE
                      WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type
                      ELSE task_name
                    END
                  ) ILIKE '%Kids%'
                  OR (
                    CASE
                      WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type
                      ELSE task_name
                    END
                  ) ILIKE '%Youth%'
                  OR (
                    CASE
                      WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type
                      ELSE task_name
                    END
                  ) ILIKE '%Lowriders%'
                  OR (
                    CASE
                      WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type
                      ELSE task_name
                    END
                  ) ILIKE '%Skiwees%'
                  OR task_type ILIKE '%Program%'
                  THEN 'Kids' ELSE 'Adults'
                END;
                """
                # Run updates (DuckDB doesn't provide row_count(); we just report completion)
                conn.execute("BEGIN TRANSACTION;")
                conn.execute(update_age_band)

                # Level backfill (priority with task_type first)
                update_level = """
                UPDATE bookings
                SET level = CASE
                  WHEN lower(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END) LIKE '%meet and greet%' OR
                       lower(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END) LIKE '%meet & greet%' OR
                       lower(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END) LIKE '%m&g%' OR
                       lower(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END) LIKE '%m & g%' OR
                       lower(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END) LIKE '%level lead%' THEN 'Meet & Greet'
                  WHEN lower(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END) LIKE '%training%' THEN 'Training'
                  WHEN lower(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END) LIKE '%base area set up%' OR
                       lower(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END) LIKE '%base area setup%' OR
                       lower(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END) LIKE '%base area set down%' OR
                       lower(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END) LIKE '%set up//down%' OR
                       lower(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END) LIKE '%packup%' OR
                       lower(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END) LIKE '%pack down%' OR
                       lower(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END) LIKE '%packdown%' OR
                       lower(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END) LIKE '%pack up%' OR
                       lower(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END) LIKE '%setup%' OR
                       lower(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END) LIKE '%set up%' THEN 'Fencing/Setup'
                  WHEN lower(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END) LIKE '%available%' OR
                       lower(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END) LIKE '%showed up%' THEN 'Showed Up'
                  WHEN task_type ILIKE '%Non Teaching%' THEN 'Non Teaching'
                  WHEN task_type ILIKE '%Private%' THEN 'Private'
                  WHEN lower(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END) LIKE '%1st time%' OR
                       lower(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END) LIKE '%first time%' THEN '1st Time'
                  WHEN lower(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END) LIKE '%big carpet%' THEN 'Big Carpet'
                  WHEN lower(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END) LIKE '%little carpet%' THEN 'Little Carpet'
                  WHEN lower(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END) LIKE '%novice%' THEN 'Novice'
                  WHEN lower(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END) LIKE '%intermediate%' THEN 'Intermediate'
                  WHEN lower(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END) LIKE '%advanced%' THEN 'Advanced'
                  WHEN lower(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END) LIKE '%beginner%' THEN 'Beginner'
                  WHEN lower(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END) LIKE '%freestyle%' THEN 'Freestyle'
                  ELSE 'Other'
                END;
                """
                conn.execute(update_level)
                # Ensure derived columns exist
                conn.execute("ALTER TABLE bookings ADD COLUMN IF NOT EXISTS task_category VARCHAR")
                conn.execute("ALTER TABLE bookings ADD COLUMN IF NOT EXISTS age_inferred INTEGER")
                conn.execute("ALTER TABLE bookings ADD COLUMN IF NOT EXISTS ability_hint VARCHAR")
                # Task category backfill derived from level and task_type
                update_task_category = """
                UPDATE bookings
                SET task_category = CASE
                  WHEN level IN ('1st Time','Novice','Beginner','Intermediate','Advanced','Freestyle','Big Carpet','Little Carpet','Private') THEN 'Lesson'
                  WHEN level = 'Fencing/Setup' THEN 'Fencing/Setup'
                  WHEN level = 'Showed Up' THEN 'Showed Up'
                  WHEN level = 'Meet & Greet' THEN 'Meet & Greet'
                  WHEN level = 'Training' THEN 'Training'
                  WHEN task_type ILIKE '%Non Teaching%' THEN 'Non Teaching'
                  ELSE 'Other'
                END;
                """
                conn.execute(update_task_category)
                # Compute age_inferred from notes fields
                update_age_inferred = """
                UPDATE bookings SET age_inferred = CAST(
                  NULLIF(
                    COALESCE(
                      regexp_extract(lower(COALESCE(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END,'') || ' ' || COALESCE(comments,'') || ' ' || COALESCE(private_guest_note,'') || ' ' || COALESCE(private_guest_name,'')), '\\b(\\d{1,2})\\s*(?:y/?o|yo|yrs?|years?|yr)\\b', 1),
                      regexp_extract(lower(COALESCE(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END,'') || ' ' || COALESCE(comments,'') || ' ' || COALESCE(private_guest_note,'') || ' ' || COALESCE(private_guest_name,'')), '\\b(?:age|aged)\\s*(\\d{1,2})\\b', 1)
                    ),
                    ''
                  ) AS INTEGER
                );
                """
                conn.execute(update_age_inferred)
                # Override age_band for Private using inferred age
                conn.execute("""
                UPDATE bookings
                SET age_band = CASE
                  WHEN task_type ILIKE '%Private%' AND age_inferred IS NOT NULL AND age_inferred < 16 THEN 'Kids'
                  WHEN task_type ILIKE '%Private%' AND age_inferred IS NOT NULL AND age_inferred >= 16 THEN 'Adults'
                  ELSE age_band
                END;
                """)
                # Ability hint from notes
                conn.execute("""
                UPDATE bookings
                SET ability_hint = CASE
                  WHEN lower(COALESCE(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END,'') || ' ' || COALESCE(comments,'') || ' ' || COALESCE(private_guest_note,'') || ' ' || COALESCE(private_guest_name,'')) LIKE '%1st time%' OR 
                       lower(COALESCE(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END,'') || ' ' || COALESCE(comments,'') || ' ' || COALESCE(private_guest_note,'') || ' ' || COALESCE(private_guest_name,'')) LIKE '%first time%' THEN '1st Time'
                  WHEN lower(COALESCE(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END,'') || ' ' || COALESCE(comments,'') || ' ' || COALESCE(private_guest_note,'') || ' ' || COALESCE(private_guest_name,'')) LIKE '%novice%' THEN 'Novice'
                  WHEN lower(COALESCE(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END,'') || ' ' || COALESCE(comments,'') || ' ' || COALESCE(private_guest_note,'') || ' ' || COALESCE(private_guest_name,'')) LIKE '%beginner%' THEN 'Beginner'
                  WHEN lower(COALESCE(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END,'') || ' ' || COALESCE(comments,'') || ' ' || COALESCE(private_guest_note,'') || ' ' || COALESCE(private_guest_name,'')) LIKE '%intermediate%' THEN 'Intermediate'
                  WHEN lower(COALESCE(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END,'') || ' ' || COALESCE(comments,'') || ' ' || COALESCE(private_guest_note,'') || ' ' || COALESCE(private_guest_name,'')) LIKE '%advanced%' THEN 'Advanced'
                  WHEN lower(COALESCE(CASE WHEN task_name IS NULL OR length(task_name) <= 1 OR lower(task_name) = 'a' THEN task_type ELSE task_name END,'') || ' ' || COALESCE(comments,'') || ' ' || COALESCE(private_guest_note,'') || ' ' || COALESCE(private_guest_name,'')) LIKE '%freestyle%' THEN 'Freestyle'
                  ELSE ability_hint
                END;
                """)
                conn.execute("COMMIT;")

                st.success("Recompute complete. Categories updated for existing rows.")
                st.info("Note: booking_id values are not changed by this backfill.")
        except Exception as e:
            st.error(f"Failed to recompute categories: {str(e)}")


def get_filter_options():
    """Get available filter options from database."""
    conn = init_database()
    try:
        weeks = conn.execute("SELECT DISTINCT week FROM bookings WHERE week IS NOT NULL ORDER BY week").fetchall()
        weeks = [w[0] for w in weeks]
        
        age_bands = conn.execute("SELECT DISTINCT age_band FROM bookings ORDER BY age_band").fetchall()
        age_bands = [a[0] for a in age_bands]
        
        levels = conn.execute("SELECT DISTINCT level FROM bookings ORDER BY level").fetchall()
        levels = [l[0] for l in levels]
        task_categories = conn.execute("SELECT DISTINCT task_category FROM bookings ORDER BY task_category").fetchall()
        task_categories = [t[0] for t in task_categories if t[0] is not None]
        
        return weeks, age_bands, levels, task_categories
    except:
        return [], [], [], []


def apply_filters(base_query, week_filter, age_band_filter, level_filter, teaching_only, task_category_filter=None):
    """Apply filters to a base query."""
    conditions = []
    
    if week_filter and week_filter != "All":
        conditions.append(f"week = {week_filter}")
    
    if age_band_filter != "All":
        conditions.append(f"age_band = '{age_band_filter}'")
    
    if level_filter != "All":
        conditions.append(f"level = '{level_filter}'")
    
    if teaching_only:
        conditions.append("is_teaching = TRUE")
    
    if task_category_filter and task_category_filter != "All":
        conditions.append(f"task_category = '{task_category_filter}'")
    if conditions:
        base_query += " WHERE " + " AND ".join(conditions)
    
    return base_query


def dashboards_tab():
    """Analytics dashboards tab."""
    st.header("📊 Dashboards")
    
    # Filters
    st.subheader("Filters")
    weeks, age_bands, levels, task_categories = get_filter_options()
    
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        week_options = ["All"] + [str(w) for w in weeks]
        week_filter = st.selectbox("Week", week_options, key="dash_week")
    
    with col2:
        age_band_options = ["All"] + age_bands
        age_band_filter = st.selectbox("Age Band", age_band_options, key="dash_age_band")
    
    with col3:
        level_options = ["All"] + levels
        level_filter = st.selectbox("Level", level_options, key="dash_level")
    
    with col4:
        teaching_only = st.checkbox("Teaching Only", value=False)
    with col5:
        tc_options = ["All", "Lesson", "Fencing/Setup", "Showed Up", "Meet & Greet", "Training", "Non Teaching", "Other"]
        # Merge with DB distincts to be safe
        for t in task_categories:
            if t not in tc_options:
                tc_options.append(t)
        task_category_filter = st.selectbox("Task Category", tc_options, index=tc_options.index("All"), key="dash_task_category")
    
    st.markdown("---")
    
    conn = init_database()
    
    # Summary Table
    st.subheader("📈 Summary by Instructor, Age Band & Level")
    
    summary_query = """
        WITH base AS (
            SELECT DISTINCT
                CASE WHEN level = 'Fencing/Setup' THEN 
                    CAST(date AS VARCHAR) || '|' || instructor || '|FS'
                ELSE booking_id END AS unit_id,
                instructor, age_band, level, week, is_teaching, task_category,
                CASE WHEN task_category = 'Lesson' AND level <> 'Private' THEN 0.5 ELSE 1 END AS level_weight
            FROM bookings
        )
        SELECT 
            instructor,
            age_band,
            level,
            SUM(level_weight) as count
        FROM base
    """
    
    summary_query = apply_filters(summary_query, week_filter, age_band_filter, level_filter, teaching_only, task_category_filter)
    summary_query += " GROUP BY instructor, age_band, level ORDER BY instructor, age_band, level"
    
    try:
        summary_df = conn.execute(summary_query).df()
        st.dataframe(summary_df, use_container_width=True)
        
        # Download button for summary
        csv_summary = summary_df.to_csv(index=False)
        st.download_button(
            label="📥 Download Summary CSV",
            data=csv_summary,
            file_name=f"summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    except Exception as e:
        st.error(f"Error generating summary: {str(e)}")
    
    st.markdown("---")
    
    # Pivot Table
    st.subheader("🔄 Instructor × Level Pivot")
    
    pivot_query = """
        WITH base AS (
            SELECT DISTINCT
                CASE WHEN level = 'Fencing/Setup' THEN 
                    CAST(date AS VARCHAR) || '|' || instructor || '|FS'
                ELSE booking_id END AS unit_id,
                instructor, level, week, is_teaching, task_category, age_band,
                CASE WHEN task_category = 'Lesson' AND level <> 'Private' THEN 0.5 ELSE 1 END AS level_weight
            FROM bookings
        )
        SELECT 
            instructor,
            SUM(CASE WHEN level = '1st Time' THEN level_weight ELSE 0 END) as "1st Time",
            SUM(CASE WHEN level = 'Novice' THEN level_weight ELSE 0 END) as "Novice",
            SUM(CASE WHEN level = 'Beginner' THEN level_weight ELSE 0 END) as "Beginner",
            SUM(CASE WHEN level = 'Intermediate' THEN level_weight ELSE 0 END) as "Intermediate",
            SUM(CASE WHEN level = 'Advanced' THEN level_weight ELSE 0 END) as "Advanced",
            SUM(CASE WHEN level = 'Freestyle' THEN level_weight ELSE 0 END) as "Freestyle",
            SUM(CASE WHEN level = 'Big Carpet' THEN level_weight ELSE 0 END) as "Big Carpet",
            SUM(CASE WHEN level = 'Little Carpet' THEN level_weight ELSE 0 END) as "Little Carpet",
            SUM(CASE WHEN level = 'Fencing/Setup' THEN 1 ELSE 0 END) as "Fencing/Setup",
            SUM(CASE WHEN level = 'Private' THEN 1 ELSE 0 END) as "Private",
            SUM(CASE WHEN level = 'Training' THEN 1 ELSE 0 END) as "Training",
            SUM(CASE WHEN level = 'Meet & Greet' THEN 1 ELSE 0 END) as "Meet & Greet",
            SUM(CASE WHEN level = 'Showed Up' THEN 1 ELSE 0 END) as "Showed Up",
            SUM(CASE WHEN level = 'Other' THEN 1 ELSE 0 END) as "Other"
        FROM base
    """
    
    pivot_query = apply_filters(pivot_query, week_filter, age_band_filter, level_filter, teaching_only, task_category_filter)
    pivot_query += " GROUP BY instructor ORDER BY instructor"
    
    try:
        pivot_df = conn.execute(pivot_query).df()
        st.dataframe(pivot_df, use_container_width=True)
        
        # Download button for pivot
        csv_pivot = pivot_df.to_csv(index=False)
        st.download_button(
            label="📥 Download Pivot CSV",
            data=csv_pivot,
            file_name=f"pivot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    except Exception as e:
        st.error(f"Error generating pivot: {str(e)}")


def streak_flags_tab():
    """Streak analysis tab."""
    st.header("🔥 Streak Flags")
    
    streak_threshold = st.slider("Minimum Streak Length", min_value=2, max_value=20, value=2)
    
    conn = init_database()
    # Date range controls
    try:
        min_max = conn.execute("SELECT MIN(date) AS min_d, MAX(date) AS max_d FROM bookings WHERE date IS NOT NULL").fetchone()
        min_d, max_d = min_max if min_max else (None, None)
    except Exception:
        min_d, max_d = None, None
    c1, c2 = st.columns(2)
    with c1:
        date_from = st.date_input("From", value=min_d, min_value=min_d, max_value=max_d, key="streak_from")
    with c2:
        date_to = st.date_input("To", value=max_d, min_value=min_d, max_value=max_d, key="streak_to")
    if not (date_from and date_to):
        st.info("Set a valid date range.")
        return
    
    # Complex streak calculation using window functions, restricted by date range
    streak_query = """
    WITH filtered AS (
        SELECT * FROM bookings 
        WHERE is_teaching = TRUE 
          AND task_category = 'Lesson'             -- only group lessons
          AND date IS NOT NULL 
          AND level NOT IN ('Private','Other')     -- exclude privates and 'Other'
          AND date BETWEEN ? AND ?
    ),
    daily_counts AS (
        -- Count by (level, age_band) per day so we can track '1st Time Kids' separately
        SELECT 
            instructor,
            date,
            level,
            age_band,
            COUNT(*) AS cnt
        FROM filtered
        GROUP BY instructor, date, level, age_band
    ),
    daily_dominant AS (
        -- Pick the dominant (level, age_band) per day
        SELECT 
            instructor,
            date,
            level,
            age_band,
            ROW_NUMBER() OVER (
                PARTITION BY instructor, date 
                ORDER BY cnt DESC, level, age_band
            ) AS rn
        FROM daily_counts
    ),
    dominant_only AS (
        SELECT instructor, date, level, age_band
        FROM daily_dominant 
        WHERE rn = 1
    ),
    lagged AS (
        -- Compare to previous day to enforce consecutiveness and same level+age_band
        SELECT 
            instructor,
            date,
            level,
            age_band,
            LAG(date) OVER (PARTITION BY instructor ORDER BY date)  AS prev_date,
            LAG(level) OVER (PARTITION BY instructor ORDER BY date) AS prev_level,
            LAG(age_band) OVER (PARTITION BY instructor ORDER BY date) AS prev_age_band
        FROM dominant_only
    ),
    group_marks AS (
        SELECT 
            instructor,
            date,
            level,
            age_band,
            CASE 
                WHEN prev_date IS NULL THEN 1
                WHEN level <> prev_level OR age_band <> prev_age_band THEN 1
                WHEN date_diff('day', prev_date, date) <> 1 THEN 1  -- break on calendar gaps
                ELSE 0
            END AS is_new_group
        FROM lagged
    ),
    streak_groups AS (
        SELECT 
            instructor,
            date,
            level,
            age_band,
            SUM(is_new_group) OVER (PARTITION BY instructor ORDER BY date ROWS UNBOUNDED PRECEDING) AS grp_id
        FROM group_marks
    ),
    streaks AS (
        SELECT 
            instructor,
            level,
            age_band,
            grp_id,
            COUNT(*) AS streak_len,
            MIN(date) AS start_date,
            MAX(date) AS end_date
        FROM streak_groups
        GROUP BY instructor, level, age_band, grp_id
    )
    SELECT 
        instructor,
        level,
        age_band,
        streak_len,
        start_date,
        end_date
    FROM streaks 
    WHERE streak_len >= ?
    ORDER BY instructor, start_date DESC
    """
    try:
        streak_df = conn.execute(streak_query, [str(date_from), str(date_to), streak_threshold]).df()
        
        if len(streak_df) > 0:
            # KPIs (overall)
            c1, c2, c3 = st.columns(3)
            with c1:
                st.metric("Total streaks", f"{len(streak_df):,}")
            with c2:
                st.metric("Longest streak", int(streak_df["streak_len"].max()))
            with c3:
                st.metric("Avg length", f"{streak_df['streak_len'].mean():.1f}")

            # Prepare dominant-per-day frame for all instructors (for heatmap/matrix)
            dom_query = """
            WITH filtered AS (
                SELECT * FROM bookings 
                WHERE is_teaching = TRUE 
                  AND task_category = 'Lesson'
                  AND date IS NOT NULL 
                  AND level NOT IN ('Private','Other')
                  AND date BETWEEN ? AND ?
            ),
            daily_counts AS (
                SELECT instructor, date, level, age_band, COUNT(*) AS cnt
                FROM filtered
                GROUP BY instructor, date, level, age_band
            ),
            daily_dominant AS (
                SELECT instructor, date, level, age_band,
                       ROW_NUMBER() OVER (PARTITION BY instructor, date ORDER BY cnt DESC, level, age_band) AS rn
                FROM daily_counts
            )
            SELECT instructor, date, level, age_band
            FROM daily_dominant
            WHERE rn = 1
            ORDER BY date, instructor
            """
            dom = conn.execute(dom_query, [str(date_from), str(date_to)]).df()
            if not dom.empty:
                dom["date"] = pd.to_datetime(dom["date"]).dt.date
                dom["label"] = dom["level"].astype(str) + " - " + dom["age_band"].astype(str)

            tab1, tab2, tab3, tab4 = st.tabs(["Top Risks", "Heatmap", "Weekly Matrix", "Details"])

            # Top Risks
            with tab1:
                st.caption("Ranked by length; optionally show only current streaks that end on the most recent date per instructor.")
                current_only = st.toggle("Current streaks only", value=False)
                risks = streak_df.copy()
                if current_only and not (dom is None or dom.empty):
                    # latest available date per instructor within window
                    latest = dom.groupby("instructor", as_index=False)["date"].max().rename(columns={"date": "latest_date"})
                    risks = risks.merge(latest, on="instructor", how="left")
                    risks = risks[risks["end_date"] == risks["latest_date"]].drop(columns=["latest_date"])
                risks = risks.assign(days_since_end=(pd.to_datetime(str(date_to)) - pd.to_datetime(risks["end_date"]).dt.tz_localize(None)).dt.days)
                risks = risks.sort_values(["streak_len", "days_since_end"], ascending=[False, True])
                # Severity bucket
                def sev(n):
                    return "🔴 7+" if n >= 7 else ("🟠 5-6" if n >= 5 else ("🟡 3-4" if n >= 3 else "🟢 <3"))
                risks["severity"] = risks["streak_len"].apply(sev)
                st.dataframe(risks[["severity","instructor","level","age_band","streak_len","start_date","end_date","days_since_end"]], use_container_width=True)

            # Heatmap
            with tab2:
                if dom.empty:
                    st.info("No data for selected range.")
                else:
                    st.caption("Daily dominant (level + age band). Use aggregate mode to avoid long instructor lists.")
                    import altair as alt
                    aggregate_mode = st.toggle("Aggregate by level (no instructor axis)", value=True)
                    color_scale = alt.Scale(domain=[
                        '1st Time','Novice','Beginner','Intermediate','Advanced','Freestyle','Big Carpet','Little Carpet','Training','Meet & Greet','Fencing/Setup','Showed Up'
                    ], range=[
                        '#E3F2FD','#E8F5E9','#FFF3E0','#EDE7F6','#FFEBEE','#F3E5F5','#E0F7FA','#E0F2F1','#FFFDE7','#F1F8E9','#F5F5F5','#F9FBE7'
                    ])

                    if aggregate_mode:
                        agg = dom.groupby(['date','level'], as_index=False).size().rename(columns={'size':'days'})
                        chart = alt.Chart(agg).mark_rect().encode(
                            x=alt.X('date:T', title='Date'),
                            y=alt.Y('level:N', title='Level'),
                            color=alt.Color('level:N', scale=color_scale, title='Level'),
                            tooltip=[alt.Tooltip('date:T'), alt.Tooltip('level:N'), alt.Tooltip('days:Q', title='Num instructors')]
                        ).properties(height=300)
                        st.altair_chart(chart, use_container_width=True)
                    else:
                        # Instructor picker to limit rows
                        counts = dom['instructor'].value_counts().head(30)
                        default_picks = counts.index.tolist()
                        picks = st.multiselect("Instructors", sorted(dom['instructor'].unique()), default=default_picks)
                        sel = dom[dom['instructor'].isin(picks)] if picks else dom.iloc[0:0]
                        if sel.empty:
                            st.info("Select one or more instructors to view.")
                        else:
                            chart = alt.Chart(sel).mark_rect().encode(
                                x=alt.X('date:T', title='Date'),
                                y=alt.Y('instructor:N', sort='-x', title='Instructor'),
                                color=alt.Color('level:N', scale=color_scale, title='Level'),
                                tooltip=[
                                    alt.Tooltip('instructor:N'),
                                    alt.Tooltip('date:T'),
                                    alt.Tooltip('level:N'),
                                    alt.Tooltip('age_band:N')
                                ]
                            ).properties(height=400)
                            st.altair_chart(chart, use_container_width=True)

            # Weekly Matrix
            with tab3:
                if dom.empty:
                    st.info("No data for selected range.")
                else:
                    week_start = st.date_input("Week starting (Mon)", value=pd.to_datetime(date_from).to_period('W').start_time.date())
                    ws = pd.to_datetime(week_start).to_period('W').start_time.date()
                    we = (pd.to_datetime(ws) + pd.Timedelta(days=6)).date()
                    sel = dom[(dom["date"] >= ws) & (dom["date"] <= we)].copy()
                    if sel.empty:
                        st.info("No data in this selected week. Try a different week.")
                    else:
                        # Pick instructors to display (default top by presence in week)
                        vc = sel['instructor'].value_counts()
                        default_picks = vc.head(15).index.tolist()
                        picks = st.multiselect("Instructors", sorted(sel["instructor"].unique()), default=default_picks)
                        sel = sel[sel["instructor"].isin(picks)] if picks else sel.iloc[0:0]
                        if sel.empty:
                            st.info("Select at least one instructor with data in this week.")
                        else:
                            import altair as alt
                            chart = alt.Chart(sel).mark_rect().encode(
                                x=alt.X('date:T', title='Day'),
                                y=alt.Y('instructor:N', title='Instructor'),
                                color=alt.Color('level:N', title='Level'),
                                tooltip=[
                                    alt.Tooltip('instructor:N'),
                                    alt.Tooltip('date:T'),
                                    alt.Tooltip('level:N'),
                                    alt.Tooltip('age_band:N')
                                ]
                            ).properties(height=400)
                            st.altair_chart(chart, use_container_width=True)

            # Details
            with tab4:
                # Breakdown by level + age_band
                st.subheader("Breakdown by level and age band")
                brk = (streak_df.groupby(["level", "age_band"], as_index=False)
                       .agg(count=("streak_len", "size"), longest=("streak_len", "max"))
                       .sort_values(["count", "longest"], ascending=[False, False]))
                st.dataframe(brk, use_container_width=True)

                st.subheader("Streaks")
                st.dataframe(streak_df, use_container_width=True)

                # Optional details
                show_details = st.toggle("Show per-streak daily details", value=False)
                if show_details:
                    st.caption("Daily details show the dominant (level, age band) for each day in the streak.")
                    for idx, row in streak_df.iterrows():
                        with st.expander(f"{row['instructor']} — {row['level']} / {row['age_band']} — {int(row['streak_len'])} days ({row['start_date']} to {row['end_date']})"):
                            try:
                                details_query = """
                                WITH filtered AS (
                                    SELECT * FROM bookings 
                                    WHERE is_teaching = TRUE 
                                      AND task_category = 'Lesson'
                                      AND date IS NOT NULL 
                                      AND level NOT IN ('Private','Other')
                                      AND instructor = ?
                                      AND date BETWEEN ? AND ?
                                ),
                                daily_counts AS (
                                    SELECT instructor, date, level, age_band, COUNT(*) AS cnt
                                    FROM filtered
                                    GROUP BY instructor, date, level, age_band
                                ),
                                daily_dominant AS (
                                    SELECT instructor, date, level, age_band,
                                           ROW_NUMBER() OVER (PARTITION BY instructor, date ORDER BY cnt DESC, level, age_band) AS rn
                                    FROM daily_counts
                                )
                                SELECT date, level, age_band
                                FROM daily_dominant
                                WHERE rn = 1 AND level = ? AND age_band = ?
                                ORDER BY date
                                """
                                details = conn.execute(
                                    details_query,
                                    [row['instructor'], str(row['start_date']), str(row['end_date']), row['level'], row['age_band']]
                                ).df()
                                st.dataframe(details, use_container_width=True)
                            except Exception as de:
                                st.write(f"Failed to load details: {de}")

                # Download button
                csv_streaks = streak_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Streaks CSV",
                    data=csv_streaks,
                    file_name=f"streaks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
        else:
            st.info(f"No streaks of {streak_threshold}+ consecutive days found.")
            
    except Exception as e:
        st.error(f"Error calculating streaks: {str(e)}")


def data_browser_tab():
    """Raw data browser tab."""
    st.header("🔍 Data Browser")
    
    row_limit = st.slider("Number of rows to display", min_value=10, max_value=1000, value=100)
    
    conn = init_database()
    
    query = f"""
        SELECT * FROM bookings 
        ORDER BY date DESC, instructor 
        LIMIT {row_limit}
    """
    
    try:
        df = conn.execute(query).df()
        st.dataframe(df, use_container_width=True)
        
        # Download sample
        csv_sample = df.to_csv(index=False)
        st.download_button(
            label="📥 Download Sample CSV",
            data=csv_sample,
            file_name=f"sample_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
        
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")


def instructor_explorer_tab():
    """Interactive visualizations for a single instructor."""
    st.header("👤 Instructor Explorer")

    conn = init_database()

    # Instructor selector
    try:
        instructors = [r[0] for r in conn.execute(
            "SELECT DISTINCT instructor FROM bookings WHERE instructor IS NOT NULL ORDER BY instructor"
        ).fetchall()]
    except Exception as e:
        st.error(f"Failed to load instructors: {e}")
        return

    if not instructors:
        st.info("No instructors found. Upload data first.")
        return

    selected = st.selectbox("Instructor", instructors, key="ie_instructor")

    # Date range from data
    try:
        min_max = conn.execute(
            "SELECT MIN(date) AS min_d, MAX(date) AS max_d FROM bookings WHERE date IS NOT NULL"
        ).fetchone()
        min_d, max_d = min_max if min_max else (None, None)
    except Exception:
        min_d, max_d = None, None

    # View mode: Calendar or Charts
    view_mode = st.radio("View", ["Calendar", "Charts"], index=0, horizontal=True)
    # Task Category filter (default Lesson)
    try:
        tc_rows = conn.execute("SELECT DISTINCT task_category FROM bookings ORDER BY task_category").fetchall()
        tc_list = [r[0] for r in tc_rows if r[0] is not None]
    except Exception:
        tc_list = []
    tc_options = ["All", "Lesson", "Fencing/Setup", "Showed Up", "Meet & Greet", "Training", "Non Teaching", "Other"]
    for t in tc_list:
        if t not in tc_options:
            tc_options.append(t)
    task_category_filter_ie = st.selectbox("Task Category", tc_options, index=tc_options.index("All"), key="ie_task_category")
    include_non_teaching = st.toggle("Include Non Teaching (packup/setup, showed up)", value=False)

    st.markdown("---")

    # Calendar mode: pick a month
    if view_mode == "Calendar":
        # Default to max month if available
        default_month = max_d if max_d else datetime.today().date()
        month_picker = st.date_input("Month", value=default_month)
        if not month_picker:
            st.info("Select a month.")
            return
        first_day = pd.to_datetime(month_picker).to_period('M').start_time.date()
        last_day = pd.to_datetime(month_picker).to_period('M').end_time.date()

        try:
            df_cal = conn.execute(
                """
                SELECT 
                    date, instructor, level, is_teaching,
                    start_time, end_time,
                    date_diff('minute', start_time, end_time) AS minutes,
                    task_name, task_type, task_category
                FROM bookings
                WHERE instructor = ? AND date BETWEEN ? AND ?
                ORDER BY date
                """,
                [selected, str(first_day), str(last_day)]
            ).df()
        except Exception as e:
            st.error(f"Failed to load instructor data: {e}")
            return

        if df_cal.empty:
            st.info("No records for this instructor in the selected month.")
            return

        # Apply task_category filter
        if task_category_filter_ie and task_category_filter_ie != "All":
            df_cal = df_cal[df_cal["task_category"] == task_category_filter_ie]
        # We'll keep Non Teaching for status detection, but optionally exclude it from counts.
        df_counts_base = df_cal.copy()
        if not include_non_teaching:
            # For counts, show only group lessons (exclude privates and any non-lesson categories like Meet & Greet)
            df_counts_base = df_counts_base[
                (df_counts_base["is_teaching"] == True)
                & (df_counts_base["task_category"] == "Lesson")
                & (df_counts_base["level"] != "Private")
            ]

        # Build day -> counts by level, with hours
        df_cal["date"] = pd.to_datetime(df_cal["date"]).dt.date
        df_counts_base["date"] = pd.to_datetime(df_counts_base["date"]).dt.date
        # Minutes already computed in SQL; ensure non-negative and non-null
        if "minutes" not in df_counts_base.columns:
            df_counts_base["minutes"] = 0
        df_counts_base["minutes"] = pd.to_numeric(df_counts_base["minutes"], errors="coerce").fillna(0).clip(lower=0)
        # Weight sessions: count each group lesson session as 0.5, others as 1.0
        df_counts_base = df_counts_base.copy()
        df_counts_base["weight"] = 1.0
        mask_gl = (df_counts_base["task_category"] == "Lesson") & (df_counts_base["level"] != "Private")
        df_counts_base.loc[mask_gl, "weight"] = 0.5

        counts = (
            df_counts_base
            .groupby(["date", "level"], as_index=False)  # type: ignore
            .agg(count=("weight", "sum"), minutes=("minutes", "sum"))
        )

        # Build per-day teaching summary
        def day_summary(day_df: pd.DataFrame) -> list[str]:
            if day_df.empty:
                return []
            # Flags
            # Group teaching = is_teaching True and level != 'Private'
            grp = day_df[(day_df["is_teaching"] == True) & (day_df["level"] != "Private")]
            # Times
            def to_minutes(t):
                try:
                    return int(pd.to_datetime(t, format="%H:%M").hour) * 60 + int(pd.to_datetime(t, format="%H:%M").minute)
                except Exception:
                    try:
                        ts = pd.to_datetime(t)
                        return ts.hour * 60 + ts.minute
                    except Exception:
                        return None
            mins = grp["start_time"].apply(to_minutes) if not grp.empty else pd.Series(dtype=float)
            has_am = False
            has_pm = False
            late_1130 = False
            if not mins.empty:
                for m in mins.dropna():
                    if m is None:
                        continue
                    if m < 12*60:
                        has_am = True
                    else:
                        has_pm = True
                    if m == 11*60 + 30:
                        late_1130 = True
            lines: list[str] = []
            if has_am and has_pm:
                lines.append("Whole Day Group")
            elif has_am or has_pm:
                part = "AM" if has_am else "PM"
                if late_1130 and not has_am:
                    lines.append("PM (11:30 start)")
                else:
                    lines.append(f"Half Day Group ({part})")
            # Status from keywords when not group teaching dominant
            text = (day_df["task_name"].fillna("").astype(str) + " " + day_df["task_type"].fillna("").astype(str)).str.lower()
            if any(k in " ".join(text.tolist()) for k in ["injury", "injured", "sick", "medical", "workers comp", "wc"]):
                lines.append("Injured/Sick")
            if any(k in " ".join(text.tolist()) for k in ["day off", "rdo", "annual leave", "personal leave", "leave"]):
                lines.append("Day Off")
            # Showed up (no teaching, has 'Showed Up' or 'Available')
            level_text = " ".join(day_df["level"].astype(str).str.lower().tolist())
            if grp.empty and ("showed up" in level_text or "available" in level_text or "available" in " ".join(text.tolist())):
                lines.append("Showed Up (No Work)")
            return lines

        # Generate calendar grid (Mon-Sun)
        month_days = pd.date_range(first_day, last_day, freq='D')
        # Start from Monday of the first week
        start_week = (pd.to_datetime(first_day) - pd.offsets.Week(weekday=0)).date()
        end_week = (pd.to_datetime(last_day) + pd.offsets.Week(weekday=6)).date()
        grid_days = pd.date_range(start_week, end_week, freq='D')

        # Render weeks
        st.subheader(f"{pd.to_datetime(first_day).strftime('%B %Y')}")
        weekday_headers = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        cols = st.columns(7)
        for i, wd in enumerate(weekday_headers):
            cols[i].markdown(f"**{wd}**")

        week_rows = [grid_days[i:i+7] for i in range(0, len(grid_days), 7)]
        # Color palette for dominant level
        level_colors = {
            '1st Time': '#E3F2FD',
            'Novice': '#E8F5E9',
            'Beginner': '#FFF3E0',
            'Intermediate': '#EDE7F6',
            'Advanced': '#FFEBEE',
            'Freestyle': '#F3E5F5',
            'Big Carpet': '#E0F7FA',
            'Little Carpet': '#E0F2F1',
            'Fencing/Setup': '#F5F5F5',
            'Training': '#FFFDE7',
            'Meet & Greet': '#F1F8E9',
            'Showed Up': '#F9FBE7',
        }
        for week in week_rows:
            cols = st.columns(7)
            for i, day in enumerate(week):
                day_date = day.date()
                in_month = first_day <= day_date <= last_day
                if not in_month:
                    cols[i].markdown(" ")
                    continue
                # Fetch summary and counts for this day
                day_df = df_cal[df_cal["date"] == day_date]
                summary_lines = day_summary(day_df)
                day_counts = counts[counts["date"] == day_date]
                body_parts = []
                if summary_lines:
                    body_parts.append("<b>" + " | ".join(summary_lines) + "</b>")
                if not day_counts.empty:
                    # Use weighted count for sorting/dominant
                    day_counts = day_counts.sort_values(["count", "level"], ascending=[False, True])
                    count_lines = []
                    for _, row in day_counts.iterrows():
                        hrs = float(row.get("minutes", 0)) / 60.0
                        # Display count without trailing .0 (e.g., 1 instead of 1.0)
                        try:
                            cnt_disp = f"{row['count']:.2f}".rstrip('0').rstrip('.')
                        except Exception:
                            cnt_disp = str(row['count'])
                        count_lines.append(f"{row['level']}: {cnt_disp} ({hrs:.1f}h)")
                    body_parts.append("<small>" + ", ".join(count_lines) + "</small>")
                body_html = "<br>".join(body_parts) if body_parts else "_—_"
                # Determine dominant level for color
                if not day_counts.empty:
                    dominant_level = day_counts.iloc[0]["level"]
                else:
                    dominant_level = None
                bg = level_colors.get(dominant_level, "#FFFFFF") if dominant_level else "#FFFFFF"
                cell_html = f"""
                <div style='background:{bg}; border-radius:8px; padding:6px; min-height:72px;'>
                    <div style='font-weight:700'>{day_date.day}</div>
                    <div style='font-size:12px;'>{body_html}</div>
                </div>
                """
                cols[i].markdown(cell_html, unsafe_allow_html=True)

        st.caption("Non Teaching is hidden by default. Toggle above to include fencing/packup/setup work.")

        return

    # Charts mode (existing): Load instructor data for selected range
    c1, c2 = st.columns(2)
    with c1:
        date_from = st.date_input("From", value=min_d, min_value=min_d, max_value=max_d, key="ie_from")
    with c2:
        date_to = st.date_input("To", value=max_d, min_value=min_d, max_value=max_d, key="ie_to")

    if not (date_from and date_to):
        st.info("Set a valid date range.")
        return

    # Load instructor data
    try:
        # Parameterize to avoid quoting issues
        df = conn.execute(
            """
            SELECT 
                date, instructor, level, age_band, is_teaching, task_category,
                start_time, end_time,
                date_diff('minute', start_time, end_time) AS minutes
            FROM bookings
            WHERE instructor = ? AND date BETWEEN ? AND ?
            ORDER BY date
            """,
            [selected, str(date_from), str(date_to)]
        ).df()
    except Exception as e:
        st.error(f"Failed to load instructor data: {e}")
        return

    if df.empty:
        st.info("No records for this instructor in the selected range.")
        return

    # Apply task_category filter
    if task_category_filter_ie and task_category_filter_ie != "All":
        df = df[df["task_category"] == task_category_filter_ie]

    # Charts
    include_non_teaching_charts = st.toggle("Include Non Teaching (packup/setup, showed up) in charts", value=False)
    if include_non_teaching_charts:
        df_charts = df
    else:
        df_charts = df[~df["level"].isin(["Non Teaching", "Fencing/Setup", "Showed Up"])]

    st.subheader("Lesson mix by level")
    # Weight: 0.5 for group lessons (task_category='Lesson' and level!='Private'), else 1.0
    df_charts_w = df_charts.copy()
    df_charts_w["weight"] = 1.0
    df_charts_w.loc[(df_charts_w["task_category"] == "Lesson") & (df_charts_w["level"] != "Private"), "weight"] = 0.5
    level_counts = df_charts_w.groupby("level", as_index=False)["weight"].sum().rename(columns={"weight": "count"})
    chart_bar = alt.Chart(level_counts).mark_bar().encode(
        x=alt.X("count:Q", title="Count"),
        y=alt.Y("level:N", sort='-x', title="Level"),
        tooltip=["level", "count"]
    ).properties(height=300)
    st.altair_chart(chart_bar, use_container_width=True)

    # Hours by level
    df_charts = df_charts.copy()
    if "minutes" not in df_charts.columns:
        df_charts["minutes"] = 0
    df_charts["minutes"] = pd.to_numeric(df_charts["minutes"], errors="coerce").fillna(0).clip(lower=0)
    hours_by_level = df_charts.groupby("level", as_index=False)["minutes"].sum()
    hours_by_level["hours"] = hours_by_level["minutes"] / 60.0
    st.subheader("Hours by level")
    chart_hrs = alt.Chart(hours_by_level).mark_bar().encode(
        x=alt.X("hours:Q", title="Hours"),
        y=alt.Y("level:N", sort='-x', title="Level"),
        tooltip=["level", alt.Tooltip("hours:Q", format=".1f")]
    ).properties(height=300)
    st.altair_chart(chart_hrs, use_container_width=True)

    st.subheader("Trend over time (weekly)")
    df_week = df_charts_w.copy()
    df_week["date"] = pd.to_datetime(df_week["date"]).dt.to_period('W').dt.start_time
    trend = alt.Chart(df_week).mark_area(opacity=0.7).encode(
        x=alt.X("date:T", title="Week"),
        y=alt.Y("sum(weight):Q", title="Lessons (weighted)"),
        color=alt.Color("level:N", title="Level"),
        tooltip=[alt.Tooltip("date:T", title="Week"), alt.Tooltip("level:N"), alt.Tooltip("sum(weight):Q", title="Lessons (weighted)")]
    ).properties(height=320)
    st.altair_chart(trend, use_container_width=True)

    st.markdown("---")
    st.subheader("Records")
    st.dataframe(df, use_container_width=True)
    st.download_button(
        label="📥 Download Instructor CSV",
        data=df.to_csv(index=False),
        file_name=f"instructor_{selected.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )

def main():
    """Main Streamlit app."""
    st.set_page_config(
        page_title="Snowsports Analytics",
        page_icon="⛷️",
        layout="wide"
    )
    
    st.title("⛷️ Snowsports School Analytics")
    st.markdown("Daily Hill Sheet ingestion and analytics dashboard")
    
    # Password gate: if not authenticated, stop rendering the rest of the app
    if not _check_password():
        st.stop()
    
    # Initialize database
    init_database()
    
    # Tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📁 Upload", "📊 Dashboards", "🔥 Streak Flags", "🔍 Data Browser", "👤 Instructor Explorer"])
    
    with tab1:
        upload_tab()
    
    with tab2:
        dashboards_tab()
    
    with tab3:
        streak_flags_tab()
    
    with tab4:
        data_browser_tab()
    with tab5:
        instructor_explorer_tab()


if __name__ == "__main__":
    main()
