# How to Use - Snowsports Analytics App

## 🚀 Getting Started

### Step 1: Install Python Packages
```bash
pip install streamlit duckdb polars pandas altair pyarrow
```

### Step 2: Start the App
```bash
streamlit run app.py
```

This opens your browser to `http://localhost:8501`

---

## 📋 Using the App

### Tab 1: 📁 Upload
**Purpose**: Import your Daily Hill CSV files

1. Click **"Choose a Daily Hill CSV file"**
2. Select your CSV (e.g., `All_Tasks_June.csv`)
3. Click **"Process Upload"**
4. See confirmation: "✅ Successfully inserted X new records!"

**Notes**:
- App automatically detects headers (handles banner rows)
- Duplicate records are skipped automatically
- View database stats at the top

### Tab 2: 📊 Dashboards
**Purpose**: View instructor analytics and summaries

**Filters** (top of page):
- **Week**: Select specific week or "All"
- **Age Band**: Kids/Adults/All
- **Level**: Specific skill level or "All" 
- **Teaching Only**: ✅ (recommended) - excludes breaks, admin tasks

**Two Tables**:
1. **Summary**: Instructor × Age Band × Level counts
2. **Pivot**: Instructor rows × Level columns (like a spreadsheet)

**Downloads**: Click "📥 Download CSV" under each table

### Tab 3: 🔥 Streak Flags
**Purpose**: Find instructors with consecutive days teaching same level

1. Adjust **"Minimum Streak Length"** slider (default: 5 days)
2. View results showing:
   - Instructor name
   - Level they taught consistently
   - Streak length (days)
   - Start and end dates
3. Download results as CSV

**Example**: "John Smith taught Advanced for 7 consecutive days"

### Tab 4: 🔍 Data Browser
**Purpose**: View raw data and export samples

1. Adjust **"Number of rows to display"** slider
2. Browse recent records (newest first)
3. Download sample data as CSV

---

## 🎯 Common Workflows

### Daily Routine
1. **Upload** → Upload today's CSV
2. **Dashboards** → Check weekly summaries with filters
3. **Download** → Export data for reports

### Weekly Analysis
1. **Dashboards** → Filter by specific week
2. **Streak Flags** → Identify consistent instructors
3. **Download** → Export pivot table for management

### Data Investigation
1. **Data Browser** → Search recent records
2. **Download** → Export raw data for Excel analysis

---

## 🔧 Troubleshooting

### "streamlit not recognized"
- Run: `pip install streamlit`
- Then: `streamlit run app.py`

### "No module named 'polars'"
- Run: `pip install polars duckdb`

### Upload fails
- Check CSV has `Date (YYYY/MM/DD)` column
- Ensure file isn't corrupted
- Try smaller file first

### No data showing
- Upload CSV files first in Upload tab
- Check filters aren't too restrictive
- Verify "Teaching Only" checkbox setting

### App runs slowly
- Reduce row limit in Data Browser
- Use more specific filters in Dashboards
- Close other browser tabs

---

## 📊 Understanding the Data

### Age Bands
- **Kids**: Tasks with "KD", "Lowriders", "Skiwees", "Kids", "Youth"
- **Adults**: Everything else

### Levels (in priority order)
1. **Non Teaching**: Breaks, admin, setup
2. **Private**: 1-on-1 lessons
3. **1st Time**: First-time students
4. **Beginner**: Basic skill building
5. **Novice**: Early intermediate
6. **Intermediate**: Standard progression
7. **Advanced**: High skill level
8. **Freestyle**: Park/tricks
9. **Meet & Greet**: Customer service
10. **Training**: Staff development
11. **Available**: Standby time
12. **Other**: Unclassified tasks

### Streak Detection
- Finds instructors teaching same level multiple consecutive days
- Only counts "teaching" tasks (excludes breaks, admin)
- Uses the instructor's "dominant" level each day (most hours)

---

## 💾 Data Storage

- Database file: `flaik.duckdb` (created automatically)
- Safe to delete - just re-upload your CSVs
- Handles 200k+ records efficiently
- No internet connection required
