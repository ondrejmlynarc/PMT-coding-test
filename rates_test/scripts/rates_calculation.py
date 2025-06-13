import duckdb
import time


class FXRates:
    """Computes adjusted FX rates using conversion rules and most recent spot mid rates within a 1-hour window"""
    def __init__(self, price_file, spot_file, ccy_file, output_file):
        self.price_file = price_file
        self.spot_file = spot_file
        self.ccy_file = ccy_file
        self.output_file = output_file
        self.con = duckdb.connect()

    def load_data(self):
        self.con.execute(f"""
            CREATE TABLE price AS 
            SELECT 
                ccy_pair, 
                CAST(timestamp AS TIMESTAMP) AS timestamp, 
                price 
            FROM read_parquet('{self.price_file}');
        """)

        self.con.execute(f"""
            CREATE TABLE spot AS 
            SELECT 
                ccy_pair, 
                CAST(timestamp AS TIMESTAMP) AS timestamp, 
                spot_mid_rate 
            FROM read_parquet('{self.spot_file}');
        """)

        self.con.execute(f"""
            CREATE TABLE ccy AS 
            SELECT * FROM read_csv_auto('{self.ccy_file}');
        """)

    def calculate_rates(self):
        query = """
        WITH matched_spot AS (
            SELECT 
                p.ccy_pair,
                p.timestamp,
                p.price,
                s.spot_mid_rate,
                ROW_NUMBER() OVER (
                    PARTITION BY p.ccy_pair, p.timestamp 
                    ORDER BY s.timestamp DESC
                ) AS rn
            FROM price p
            LEFT JOIN spot s 
              ON p.ccy_pair = s.ccy_pair 
             AND s.timestamp <= p.timestamp 
             AND s.timestamp > p.timestamp - INTERVAL '1 hour'
        ),
        final_result AS (
            SELECT 
                m.ccy_pair,
                m.timestamp,
                m.price,
                c.conversion_factor,
                c.convert_price,
                m.spot_mid_rate,
               CASE
                WHEN c.convert_price IS NOT TRUE THEN m.price
                WHEN c.convert_price IS TRUE 
                    AND c.conversion_factor IS NOT NULL 
                    AND m.spot_mid_rate IS NOT NULL
                    THEN ROUND( (m.price / c.conversion_factor) + m.spot_mid_rate, 2)
                ELSE NULL
                END AS new_price,
                -- Helps identify whether conversion logic was successfully applied
                (c.convert_price IS TRUE 
                 AND c.conversion_factor IS NOT NULL 
                 AND m.spot_mid_rate IS NOT NULL) AS conversion_applied,
                (c.convert_price IS TRUE 
                 AND (c.conversion_factor IS NULL OR m.spot_mid_rate IS NULL)) AS insufficient_data,
                c.conversion_factor
            FROM matched_spot m
            LEFT JOIN ccy c ON m.ccy_pair = c.ccy_pair
            WHERE m.rn = 1 OR m.rn IS NULL
        )
        SELECT 
            ccy_pair, 
            timestamp, 
            price, 
            new_price, 
            conversion_applied, 
            insufficient_data,
            CASE
                WHEN conversion_applied THEN ''
                WHEN insufficient_data AND conversion_factor IS NULL THEN 'No conversion factor'
                WHEN insufficient_data THEN 'No spot rate in window'
                ELSE ''
            END AS error_message
        FROM final_result
        """
        self.con.execute(f"COPY ({query}) TO '{self.output_file}' (HEADER, DELIMITER ';')")

    def run(self):
        start_time = time.time()
        print("Starting calculation with direct file loading...")

        try:
            self.load_data()
            self.calculate_rates()
            elapsed = time.time() - start_time
            print(f"Saved to: '{self.output_file}'")
            print(f"Execution time: {elapsed:.3f} seconds")

        except Exception as e:
            elapsed = time.time() - start_time
            print(f"Error occurred after {elapsed:.3f} seconds: {e}")
            raise
