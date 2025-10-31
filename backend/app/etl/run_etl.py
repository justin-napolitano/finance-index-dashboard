from app.etl.fetch_data import fetch_prices
from app.etl.compute_signals import compute_all_signals
from app.etl.rebalance_indices import reconstitute_and_rebalance
from datetime import date

def main():
    fetch_prices()
    compute_all_signals()
    reconstitute_and_rebalance(asof=date.today())

if __name__ == "__main__":
    main()
