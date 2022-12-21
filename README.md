# Seshat
A python module for calling and wrangling crypto exchange data into a standardized format.

## Motivation
This module was developed to aid strategy development for perpetual futures - a product currently hosted only on crypto exchanges, so for now,  it focuses on funding rates and interest rates for borrowable assets on crypto exchanges.

Although other extensive market data aggregation libraries exist (namely [CCXT](https://github.com/ccxt/ccxt)), these options often have mismatching documentation and lack congruity between exchanges and types of market data.

As well, it is safer to understand all the code that your trading system relies on, so over the long term I think it is best to develop your own data aggregation modules.




## Available Data
**Exchanges**: Binance, Huobi, FTX, OKX, Bybit

**Data**:
- *Funding Rates*: predicted, current, and historical perpetual futures [funding rates](https://www.binance.com/en/blog/futures/a-beginners-guide-to-funding-rates-421499824684900382) per coin per exchange.
- *Borrow Rates*: current and historical interest rates for borrowing cryptocurrency, per coin, per exchange.
- *Market Prices*: currently only offers current and historical market (not BBO or order book depth) prices.
- Trading Pairs and Assets available for Borrowing


## Usage
#### Aggregated Data
Exchange data aggregators
```python
from seshat.report_funding_rates import ReportFundingRates
from seshat.report_borrow_rates import ReportBorrowRates
from seshat.report_prices import ReportPrices
```

See available trading pairs
```python
## Perpetual Future Trading Pairs
funding_pairs_df = ReportFundingRates().get_available_perps()
borrow_assets_df = ReportBorrowRates().get_borrowable_assets()

spot_pairs_df = ReportPrices.get_spot_pairs()
perp_pairs_df = ReportPrices.get_perp_pairs()
```

Get funding rates
```python
next_rates_df = ReportFundingRates(
    ).get_predicted_rates()  # returns all trading pairs if no symbols are requested

historical_df = ReportFundingRates(
    ).get_historical_rates(
        start=datetime(2022,1,1),
        end=datetime(2022,2,1),
        requested_symbols=['ADA', 'MATIC'])  # returns all pairs involving these symbols
```

Get borrow rates
```python
current_df = ReportBorrowRates(
    ).get_current_rates(requested_symbols=['BTC', 'ETH', 'LTC'])

historical_df = ReportBorrowRates(
    ).get_historical_rates(
        start=datetime(2022,5,1),
        end=datetime(2022,5,5),
        requested_symbols=['ETH', 'DOT'])
```

Get market prices
```python
historical_df = ReportPrices(
    ).get_historical_prices(
        interval='1h',  # format is {float}{[s,m,h,d]}  # Examples: 30s, 1.5m, 12h, 7d
        start=datetime(2021,12,1),
        end=datetime(2022,2,1),
        instrument='spot',  # 'spot' or 'perp'
        underlying=['BTC', 'ETH'],
        quote=['USD', 'USDT']
    )
```

#### Exchange-Specific Data

Classes
```python
from comms_binance import CommsBinance
from comms_huobi import CommsHuobi
# etc... same naming convention for other exchanges
```

Example functions
```python
hist_prices = CommsBinance(
        ).get_historical_price(
            pairs=[('BTC','USDT'), ('ETH', 'USD'), ('DOT', 'USD')],
            start=datetime(2022,4,1,10),
            end=datetime(2022,4,7,10),
            interval='1h',
            instrument='perp')

next_rates = CommsHuobi(
    ).get_next_funding_rate(
        pairs=(("BTC", "USDT"), ("LTC", "USDT"), ("ETH", "USDT")))

```