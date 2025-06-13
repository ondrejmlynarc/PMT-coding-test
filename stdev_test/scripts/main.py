from rolling_stdev_calculation import RollingStdev
from pathlib import Path
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate rolling standard deviation")

    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / "data"
    result_dir = script_dir.parent / "results"

    parser.add_argument('--input-file', default=data_dir / 'stdev_price_data.parq', type=Path)
    parser.add_argument('--output-file', default=result_dir / 'rolling_stdev_results.csv', type=Path)
    parser.add_argument('--start-date', default='2021-11-20 00:00:00')
    parser.add_argument('--end-date', default='2021-11-23 09:00:00')
    parser.add_argument('--lookback-days', default=7, type=int)
    parser.add_argument('--rolling-window', default=20, type=int)

    args = parser.parse_args()

    try: 

        calculation = RollingStdev(
            file_path=args.input_file,
            start_output=args.start_date,
            end_output=args.end_date,
            lookback_days=args.lookback_days,
            output_file=args.output_file,
            rolling_window=args.rolling_window
        )

        calculation.run()
        
    except FileNotFoundError as e:
        print(f"Error: File not found - {e}")
        exit(1)
    except ValueError as e:
        print(f"Error: Invalid parameter - {e}")
        exit(1)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        exit(1)