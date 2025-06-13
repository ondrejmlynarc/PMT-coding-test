import duckdb
import time


class RollingStdev:
    """Calculates hourly rolling stdevs for bid, mid, and ask prices with time-contiguous checks"""
    def __init__(
        self,
        file_path='data/stdev_price_data.parq',
        start_output='2021-11-20 00:00:00',
        end_output='2021-11-23 09:00:00',
        lookback_days=7,
        output_file='rolling_stdev_results.csv',
        rolling_window=20,
    ):
        self.file_path = file_path
        self.start_output = start_output
        self.end_output = end_output
        self.lookback_days = lookback_days
        self.output_file = output_file
        self.rolling_window = rolling_window
        self.conn = duckdb.connect()

    def prepare_data(self):
        """ Includes lookback window to ensure we have enough data points for initial rolling windows """
        self.conn.execute(f"""
            CREATE TEMP TABLE trades AS
            SELECT * FROM read_parquet('{self.file_path}')
            WHERE snap_time BETWEEN TIMESTAMP '{self.start_output}' - INTERVAL '{self.lookback_days} days' 
                                AND TIMESTAMP '{self.end_output}'
            ORDER BY security_id, snap_time
        """)

    def run_and_save_query(self):
        n = self.rolling_window
        n_minus_1 = n - 1
        seconds_in_hour = 3600
        total_seconds = n_minus_1 * seconds_in_hour  # Enforces hourly intervals for contiguous window

        query = f"""
        COPY (
            WITH ordered_with_lag AS (
                SELECT 
                    security_id,
                    snap_time,
                    bid,
                    mid,
                    ask,
                    ROW_NUMBER() OVER (PARTITION BY security_id ORDER BY snap_time) AS rn,
                    LAG(snap_time, {n_minus_1}) OVER (PARTITION BY security_id ORDER BY snap_time) AS lag_snap_time
                FROM trades
            ),
            final_calc AS (
                SELECT 
                    security_id,
                    snap_time,
                    -- Only calculate if window is full and time-contiguous (e.g., no missing hours)
                    CASE 
                        WHEN rn >= {n} 
                        AND EXTRACT(EPOCH FROM (snap_time - lag_snap_time)) = {total_seconds}
                        THEN TRUE ELSE FALSE
                    END AS is_contiguous,
                    CASE 
                        WHEN rn >= {n} 
                        AND EXTRACT(EPOCH FROM (snap_time - lag_snap_time)) = {total_seconds}
                        THEN STDDEV(bid) OVER (
                            PARTITION BY security_id ORDER BY snap_time ROWS BETWEEN {n_minus_1} PRECEDING AND CURRENT ROW
                        )
                        ELSE NULL 
                    END AS bid_stdev,
                    CASE 
                        WHEN rn >= {n} 
                        AND EXTRACT(EPOCH FROM (snap_time - lag_snap_time)) = {total_seconds}
                        THEN STDDEV(mid) OVER (
                            PARTITION BY security_id ORDER BY snap_time ROWS BETWEEN {n_minus_1} PRECEDING AND CURRENT ROW
                        )
                        ELSE NULL 
                    END AS mid_stdev,
                    CASE 
                        WHEN rn >= {n} 
                        AND EXTRACT(EPOCH FROM (snap_time - lag_snap_time)) = {total_seconds}
                        THEN STDDEV(ask) OVER (
                            PARTITION BY security_id ORDER BY snap_time ROWS BETWEEN {n_minus_1} PRECEDING AND CURRENT ROW
                        )
                        ELSE NULL 
                    END AS ask_stdev
                FROM ordered_with_lag
            )
            SELECT 
                security_id,
                snap_time,
                is_contiguous,
                bid_stdev,
                mid_stdev,
                ask_stdev
            FROM final_calc
            WHERE snap_time BETWEEN TIMESTAMP '{self.start_output}' AND TIMESTAMP '{self.end_output}'
            ORDER BY security_id, snap_time
        ) TO '{self.output_file}' (HEADER, DELIMITER ';');
        """
        self.conn.execute(query)

    def run(self):
        start_time = time.time()
        print("Starting calculation with direct file loading...")

        try:
            self.prepare_data()
            self.run_and_save_query()
            elapsed = time.time() - start_time
            print(f"Saved to: '{self.output_file}'")
            print(f"Execution time: {elapsed:.3f} seconds")

        except Exception as e:
            elapsed = time.time() - start_time
            print(f"Error occurred after {elapsed:.3f} seconds: {e}")
            raise
        finally:
            self.conn.close()