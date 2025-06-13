from rates_calculation import FXRates
from pathlib import Path
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate rates from price and spot data")
    
    # default paths
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / "data"
    result_dir = script_dir.parent / "results"
    
    parser.add_argument('--price-file', default=data_dir / 'rates_price_data.parq', type=Path)
    parser.add_argument('--spot-file', default=data_dir / 'rates_spot_rate_data.parq', type=Path)
    parser.add_argument('--ccy-file', default=data_dir / 'rates_ccy_data.csv', type=Path)
    parser.add_argument('--output-file', default=result_dir / 'rates_final_prices.csv', type=Path)
    
    args = parser.parse_args()
    
    try:
        calculation = FXRates(
            price_file=args.price_file,
            spot_file=args.spot_file,
            ccy_file=args.ccy_file,
            output_file=args.output_file
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