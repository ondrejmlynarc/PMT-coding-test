# Parameta Solutions: Coding Test

This repo contains solutions for two data science tasks from the Parameta Solutions coding test.

## Setup

Clone the repo and install dependencies:

```bash
git clone [your-repo-url]
cd [your-repo-name]
python -m venv .venv
# Activate venv: source .venv/bin/activate (macOS/Linux) or .\.venv\Scripts\activate (Windows)
pip install -r requirements.txt
```

### Methodological Question - Why DuckDB?

I chose DuckDB as the main library for both tasks due to its superior analytical performance on local data compared to Pandas and Polars. Its vectorized time-based joins run about **5 times** faster than Pandas, which matches my previous experience and external benchmarks like the [prrao87/duckdb-study GitHub repository](https://github.com/prrao87/duckdb-study).



## Task 1: Currency Rate Adjustment

Implements a `FXRates` class to adjust timestamped prices using FX spot rates and currency conversion rules.

**Inputs:**

* `rates_price_data.parq`: Price data
* `rates_spot_rate_data.parq`: Spot FX rates
* `rates_ccy_data.csv`: Conversion factors and flags

**Process:**

1.  Load data into DuckDB.
2.  Match each price with the most recent spot rate (≤1 hour earlier).
3.  Apply conversion logic:
    * No conversion: keep original.
    * Conversion: `(price / factor) + mid-rate`.
    * Missing data: keep original, flag as insufficient.
4.  Output saved to CSV.

**Run from command line:**

Navigate to the `scripts` subfolder of the given project, then execute:

```bash
python main.py
```

To override default file paths, use:

```bash
python main.py --price-file <path> --spot-file <path> --ccy-file <path> --output-file <path>
```

**Output Columns:**

`ccy_pair`, `timestamp`, `price`, `new_price`, `conversion_applied`, `insufficient_data`, `error_message`

**Performance:** ~0.68s (Python 3, 16GB RAM)


## Testing

Make sure you are in the `scripts` subfolder of the given project, then execute:

```bash
pytest test_rates_calculation.py
```


## Task 2: Rolling Standard Deviation

Implements a `RollingStdev` class to compute hourly rolling standard deviations for `bid`, `mid`, and `ask` prices, ensuring time-contiguous windows.

**Inputs:**

- `stdev_price_data.parq`: Timestamped price data (hourly)

**Process:**

1. Load data into DuckDB with a 7-day lookback window.
2. For each `security_id` and hourly `snap_time` (from `2021-11-20 00:00:00` to `2021-11-23 09:00:00`), calculate the 20-hour rolling standard deviation.
3. Only compute if all 20 preceding hours are present (time-contiguous).
4. Output saved to CSV.

**Run from command line:**

Navigate to the `scripts` subfolder of the given project, then execute:

```bash
python main.py
```

To set up or override default parameters, run:

```bash
python main.py \
  --input-file <path> \
  --output-file <path> \
  --start-date <YYYY-MM-DD HH:MM:SS> \
  --end-date <YYYY-MM-DD HH:MM:SS> \
  --lookback-days <int> \
  --rolling-window <int>
  ```

**Output Columns:**

`security_id`, `snap_time`, `is_contiguous`, `bid_stdev`, `mid_stdev`, `ask_stdev`

**Performance:** ~0.01s (Python 3, 16GB RAM)

## Testing

Make sure you are in the `scripts` subfolder of the given project, then execute:

```bash
pytest test_rates_calculation.py
```

