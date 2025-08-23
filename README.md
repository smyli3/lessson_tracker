# Snowsports School Analytics

Internal app for ingesting Daily Hill CSV files and maintaining a fast, queryable history with dashboards.

## Features

- **CSV Upload**: Daily ingestion with automatic deduplication
- **Analytics Dashboards**: Instructor × Age Band × Level summaries and pivot tables
- **Streak Detection**: Find instructors with consecutive days on same dominant level
- **Data Browser**: Raw data viewing and CSV downloads
- **Performance**: Handles 200k+ rows efficiently with DuckDB

## Tech Stack

- Python 3.11
- Streamlit (UI)
- DuckDB (storage & analytics)
- Polars (fast CSV processing)
- Pandas (data manipulation)

## Quick Start

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Run the Streamlit app**:
   ```bash
   streamlit run app.py
   ```

3. **Or use CLI ingestion**:
   ```bash
   python ingest.py path/to/dailyhill.csv
   ```

## Data Processing

### CSV Structure Expected
The app expects Daily Hill CSV files with these headers:
- `Date (YYYY/MM/DD)`
- `Staff First Name`, `Staff Last Name`
- `Task Name`, `Task Type`
- `Task Start (HH:MM)`, `Task End (HH:MM)`
- And other standard Flaik fields...

### Normalization Rules

**Age Band Classification**:
- **Kids**: Task names containing " KD ", "- KD", "Kids", "Youth", "Lowriders", "Skiwees"
- **Adults**: Everything else

**Level Classification** (in order of precedence):
1. Non Teaching (task_type contains "Non Teaching")
2. Private (task_type contains "Private") 
3. 1st Time (task_name contains "1st time" or "first time")
4. Novice (task_name contains "novice")
5. Beginner (task_name contains "beginner")
6. Intermediate (task_name contains "intermediate")
7. Advanced (task_name contains "advanced")
8. Freestyle (task_name contains "freestyle")
9. Meet & Greet (task_name contains "meet and greet")
10. Training (task_name contains "training")
11. Available (task_name contains "available")
12. Other (fallback)

### Deduplication
Records are deduplicated using a `booking_id`:
```
{date}|{staff_id}|{start_time}-{end_time}|{task_name}
```

## App Tabs

### 1. Upload
- File uploader for CSV files
- Automatic header detection (handles banner rows)
- Real-time ingestion feedback
- Database statistics

### 2. Dashboards
- **Filters**: Week, Age Band, Level, Teaching Only
- **Summary Table**: Instructor × Age Band × Level counts
- **Pivot Table**: Instructor × Level matrix
- CSV downloads for both views

### 3. Streak Flags
- Configurable streak threshold (default: 5 days)
- Finds consecutive days where instructor has same dominant level
- Uses window functions for efficient calculation
- CSV download of results

### 4. Data Browser
- View recent records (configurable limit)
- Raw data inspection
- Sample CSV downloads

## Database Schema

**Table**: `bookings`
- All original CSV fields (normalized names)
- Derived fields: `instructor`, `age_band`, `level`, `is_teaching`, `week`
- Unique constraint on `booking_id`

## Performance Notes

- All aggregations done in DuckDB SQL (not in-memory Python)
- Polars used for fast CSV reading
- File-backed DuckDB database (`flaik.duckdb`)
- Defensive parsing for dates/times
- Handles unknown extra columns gracefully

## Example Usage

```bash
# Initial setup
pip install -r requirements.txt

# Ingest historical data
python ingest.py All_Tasks_June.csv
python ingest.py All_Tasks_July.csv

# Start web interface
streamlit run app.py
```

## Troubleshooting

- **File not found**: Ensure CSV path is correct
- **Header detection fails**: Check for `Date (YYYY/MM/DD)` column
- **Database locked**: Close other connections to `flaik.duckdb`
- **Memory issues**: Reduce row limits in Data Browser

## Future Enhancements

- Configurable level/age band rules via YAML
- Health indicators (last ingest time, source filename)
- Advanced filtering and date range selection
- Export to Excel format
