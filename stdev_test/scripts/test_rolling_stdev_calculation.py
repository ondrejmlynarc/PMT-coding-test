import pytest
import pandas as pd
import os
import tempfile
import shutil
from datetime import datetime, timedelta
from rolling_stdev_calculation import RollingStdev

class TestRollingStdev:
    """ Unit tests including std logic with contiguous/non-contiguous data"""
    def setup_method(self):
        self.temp_dir = tempfile.mkdtemp()
        self.test_data_file = os.path.join(self.temp_dir, 'test_data.parq')
        self.output_file = os.path.join(self.temp_dir, 'output.csv')

    def teardown_method(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def create_contiguous_data(self, count=30):
        base_time = datetime(2021, 11, 1, 0, 0)
        data = []
        for i in range(count):
            data.append({
                'snap_time': base_time + timedelta(hours=i),
                'security_id': 'id_test',
                'bid': 100 + i,
                'mid': 200 + i,
                'ask': 300 + i
            })
        df = pd.DataFrame(data)
        df.to_parquet(self.test_data_file)

    def create_noncontiguous_data(self, count=30, missing_hour_index=15):
        base_time = datetime(2021, 11, 1, 0, 0)
        data = []
        for i in range(count):
            if i == missing_hour_index:
                continue  # skip one hour to break continuity
            data.append({
                'snap_time': base_time + timedelta(hours=i),
                'security_id': 'id_test',
                'bid': 100 + i,
                'mid': 200 + i,
                'ask': 300 + i
            })
        df = pd.DataFrame(data)
        df.to_parquet(self.test_data_file)

    def test_rolling_with_contiguous_data(self):
        self.create_contiguous_data()
        rolling_window = 20

        calculator = RollingStdev(
            file_path=self.test_data_file,
            start_output='2021-11-1 00:00:00',
            end_output='2021-11-2 23:00:00',
            output_file=self.output_file,
            rolling_window=rolling_window
        )
        calculator.run()

        df = pd.read_csv(self.output_file, delimiter=';')
        actual_non_null_rows = df['bid_stdev'].notnull().sum()
        expected = 30 - rolling_window + 1  # rolling window (n - w + 1) 
        assert actual_non_null_rows == expected, f"Expected {expected} non-null rows, got {actual_non_null_rows}"

    def test_rolling_with_noncontiguous_data(self):
        self.create_noncontiguous_data()
        rolling_window = 20

        calculator = RollingStdev(
            file_path=self.test_data_file,
            start_output='2021-11-1 00:00:00',
            end_output='2021-11-2 23:00:00',
            output_file=self.output_file,
            rolling_window=rolling_window
        )
        calculator.run()

        df = pd.read_csv(self.output_file, delimiter=';')
        actual_non_null_rows = df['bid_stdev'].notnull().sum()

        # Because the data is broken in the middle, it restricts a full 20-point window, so we expect 0 computed stdevs
        assert actual_non_null_rows == 0, f"Expected 0 non-null rows due to break, got {actual_non_null_rows}"


if __name__ == "__main__":
    import pytest
    pytest.main(["-v", __file__])
