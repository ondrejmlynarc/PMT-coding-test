import pytest
import pandas as pd
import tempfile
import os
from datetime import datetime
from unittest.mock import patch, MagicMock
from rates_calculation import FXRates


class TestFXRates:
    """ Unit and integration tests for FXRates class including data loading, conversion logic, and error handling."""
    def test_init(self):
        fx = FXRates("price.parquet", "spot.parquet", "ccy.csv", "output.csv")
        assert fx.price_file == "price.parquet"
        assert fx.output_file == "output.csv"
        assert fx.con is not None

    def test_load_data(self):
        with patch("duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            fx = FXRates("p.parquet", "s.parquet", "c.csv", "o.csv")
            fx.load_data()
            assert mock_con.execute.call_count == 3

    @pytest.mark.parametrize("price_data, spot_data, ccy_data, expected_price, test_case", [
        (
            pd.DataFrame({
                'timestamp': [datetime(2021, 12, 10, 7, 38, 7)],
                'security_id': ['id_023'],
                'price': [156.00],
                'ccy_pair': ['USDVND']
            }),
            pd.DataFrame({
                'timestamp': [datetime(2021, 12, 10, 7, 0, 0)],
                'ccy_pair': ['USDVND'],
                'spot_mid_rate': [0.002]
            }),
            pd.DataFrame({
                'ccy_pair': ['USDVND'],
                'conversion_factor': [1000.0],
                'convert_price': [True]
            }),
            0.158,
            "standard_conversion"
        ),
        (
            pd.DataFrame({
                'timestamp': [datetime(2021, 12, 10, 10, 0, 0)],
                'security_id': ['id_024'],
                'price': [200.0],
                'ccy_pair': ['EURUSD']
            }),
            pd.DataFrame({
                'timestamp': [datetime(2021, 12, 10, 9, 30, 0)],
                'ccy_pair': ['EURUSD'],
                'spot_mid_rate': [1.1]
            }),
            pd.DataFrame({
                'ccy_pair': ['EURUSD'],
                'conversion_factor': [99.0],
                'convert_price': [False]
            }),
            200.0,
            "no_conversion"
        ),
        (
            pd.DataFrame({
                'timestamp': [datetime(2021, 12, 11, 12, 0, 0)],
                'security_id': ['id_025'],
                'price': [99.0],
                'ccy_pair': ['GBPUSD']
            }),
            pd.DataFrame({
                'timestamp': [datetime(2021, 12, 11, 10, 30, 0)],
                'ccy_pair': ['GBPUSD'],
                'spot_mid_rate': [1.25]
            }),
            pd.DataFrame({
                'ccy_pair': ['GBPUSD'],
                'conversion_factor': [100.0],
                'convert_price': [True]
            }),
            None,
            "No spot rate in window"
        ),
        (
            pd.DataFrame({
                'timestamp': [datetime(2021, 12, 10, 12, 0, 0)],
                'security_id': ['id_026'],
                'price': [500.0],
                'ccy_pair': ['JPYUSD']
            }),
            pd.DataFrame({
                'timestamp': [datetime(2021, 12, 10, 11, 0, 1)],
                'ccy_pair': ['JPYUSD'],
                'spot_mid_rate': [0.0088]
            }),
            pd.DataFrame({
                'ccy_pair': ['JPYUSD'],
                'conversion_factor': [100.0],
                'convert_price': [True]
            }),
            5.0088,
            "hour_boundary"
        ),
        (
            pd.DataFrame({
                'timestamp': [datetime(2021, 12, 10, 15, 30, 0)],
                'security_id': ['id_027'],
                'price': [100.00],
                'ccy_pair': ['USDVND']
            }),
            pd.DataFrame({
                'timestamp': [datetime(2021, 12, 10, 15, 0, 0), datetime(2021, 12, 10, 15, 15, 0)],
                'ccy_pair': ['USDVND', 'USDVND'],
                'spot_mid_rate': [23150.0, 23151.0]
            }),
            pd.DataFrame({
                'ccy_pair': ['USDVND'],
                'conversion_factor': [1.0],
                'convert_price': [True]
            }),
            100.00 + 23151.0,
            "multiple_spots"
        )
    ])
    def test_fx_rates_integration(self, price_data, spot_data, ccy_data, expected_price, test_case):
        with tempfile.TemporaryDirectory() as temp_dir:
            price_file = os.path.join(temp_dir, "price.parquet")
            spot_file = os.path.join(temp_dir, "spot.parquet")
            ccy_file = os.path.join(temp_dir, "ccy.csv")
            output_file = os.path.join(temp_dir, "output.csv")

            price_data.to_parquet(price_file, index=False)
            spot_data.to_parquet(spot_file, index=False)
            ccy_data.to_csv(ccy_file, index=False)

            fx_rates = FXRates(price_file, spot_file, ccy_file, output_file)
            fx_rates.run()

            assert os.path.exists(output_file), f"Output file not created for {test_case}"
            results = pd.read_csv(output_file, delimiter=';')
            assert len(results) == 1, f"Expected 1 result for {test_case}"

            actual_price = results["new_price"].iloc[0]

            if expected_price is None:
                assert pd.isna(actual_price), f"Expected NULL for {test_case}, got {actual_price}"
            else:
                assert not pd.isna(actual_price), f"Expected value for {test_case}, got NULL"
                assert abs(actual_price - expected_price) < 0.005, \
                    f"{test_case}: Expected {expected_price}, got {actual_price}"

    def test_error_handling_missing_files(self):
        fx_rates = FXRates("missing.parquet", "missing.parquet", "missing.csv", "output.csv")
        with pytest.raises(Exception):
            fx_rates.run()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
