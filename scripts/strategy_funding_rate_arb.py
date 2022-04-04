import ccxt
from typing import List
from datetime import datetime
import pandas as pd




class StrategyFundingRateArb:
    '''
    Trade:
        - Funding rate on one side of the trade > cost to hedge the other side of the trade
        - Go long/short the perpetual giving the favorable funding rate
        - Go long/short hedge side, which could be:
            - another perpetual giving a cheaper funding rate, or
            - Margin borrowing USDT and buying the asset, or
            - Margin borrowing the asset and selling the asset
    Confounding variables:
        - Price divergence of perpetual future and hedge instrument could occur and maintain an intrument
          price loss, while the cost to hedge increases to the point where it exceeds the funding rate reward.
            - In that case, an unrealized loss may occur if the profits from the funding rate reward are
              smaller than the loss from price divergence.
              As well, in this case, one would have to decide on bearing a daily expense in order to maintain
              the price arbitrage, in the hopes that the true instrument's prices converge before the cost to
              hedge exceeds to the profit from instrument price convergence.
    '''

    def __init__(self):
        self.parsed_long_rates = None
        self.parsed_short_rates = None
        self.parsed_fiat_borrow_rates = None


    def parse_rates_into_long_and_short(self, funding_rates: dict, borrow_rates: dict):
        '''
        Takes Funding Rates and Borrow Rates and creates three dicts as class attributes,
        formatted as {asset, [list_of_fee_dicts]}, for the strategy function.

        The fiat_borrow_rates should be combined with the long_rates of each symbol.
        It is not combined and stored by this function to save space and reduce repeated data.
        '''
        self.parsed_long_rates = {}
        self.parsed_short_rates = {}
        self.parsed_fiat_borrow_rates = {}

        fiat_assets = ['USDC', 'USDT', 'USDP', 'TUSD', 'UST']
        # ## Find All Symbols
        # symbol_set = []
        # for exchange, exchange_funding_rates in funding_rates.items():
        #     symbol_set.append(exchange_funding_rates.keys())
        # for exchange, exchange_interest_rates in interest_rates.items():
        #     symbol_set.append(exchange_interest_rates.keys())

        ## Parse Funding Rates
        for exchange, exchange_funding_rates in funding_rates.items():
            for symbol, rate in exchange_funding_rates.items():
                long_rate_dict = {
                    'exchange': exchange,
                    'instrument': 'future',
                    'side': 'long',
                    'roi_1h': rate * -1 if rate >= 0 else rate   # positive fund fee rate means long pays short, so long roi for a positive rate is negative
                }
                short_rate_dict = {
                    'exchange': exchange,
                    'instrument': 'future',
                    'side': 'long',
                    'roi_1h': rate if rate >= 0 else rate * -1  # positive fund fee rate means long pays short, so short roi for a positive rate is positive
                }
                self.long_rates[symbol] = self.long_rates.get(symbol, []).append(long_rate_dict)
                self.short_rates[symbol] = self.short_rates.get(symbol, []).append(short_rate_dict)

        ## Parse Spot Borrow Rates
        for exchange, exchange_borrow_rates in borrow_rates.items():
            for symbol, rate in exchange_borrow_rates.items():

                ## Add to Fiat Borrow Rates (used as a long rate for every non-fiat asset)
                if symbol in fiat_assets:
                    rate_dict = {
                        'exchange': exchange,
                        'instrument': 'spot',
                        'side': 'long',
                        'roi_1h': rate * -1   # borrowing incur an expense
                    }
                    self.fiat_borrow_rates[symbol] = self.fiat_borrow_rates.get(symbol, []).append(rate_dict)

                ## Add to Short Rates
                else:
                    rate_dict = {
                        'exchange': exchange,
                        'instrument': 'spot',
                        'side': 'short',
                        'roi_1h': rate * -1   # borrowing incur an expense
                    }
                    self.short_rates[symbol] = self.short_rates.get(symbol, []).append(short_rate_dict)
        return


    def get_parsed_rates(self, symbol: str):
        '''
        For a given symbol, combine the fiat_borrow_rates with the symbol's long_rates,
        and then return the long_rates and short_rates for that symbol.
        '''
        symbol_long_rates = self.parsed_long_rates[symbol]
        symbol_long_rates.extend(self.parsed_fiat_borrow_rates)
        return symbol_long_rates, self.parsed_short_rates[symbol]


    def strategy(self, symbols: List[str], funding_rates: dict, interest_rates: dict):
        '''
        For each hedged trade in this strategy, we define the two sides of the trade as the:
            - Income Trade, which generates a periodic funding fee reward
            - Expense Trade, which hedges the Income Trade, and may generate an expense

        This function finds, for each asset, the two hedge instruments that together (income and expense) will return the largest ROI.
        '''
        best_return_template = {
            'underlying_symbol'  : None,
            'income_exchange'    : None,
            'income_instrument'  : None,   # spot, future
            'income_side'        : None,   # long, short
            'income_rate'        : None,   # per hour
            'expense_exchange'   : None,
            'expense_instrument' : None,
            'expense_side'       : None,
            'expense_rate'       : None,   # per hour
            'roi_1h'             : None,
        }
        intra_exchange_trades = []
        inter_exchange_trades = []

        ## Format Funding and Interest Rates
        self.parse_rates_into_long_and_short(funding_rates, interest_rates)

        ## Find Best Long and Short Instrument Pair for each Symbol
        for symbol in symbols:
            symbol_long_rates, symbol_short_rates = self.get_parsed_rates(symbol=symbol)

            #(1)# Find Best Inter-Exchange Return
            long_instrument = sorted(symbol_long_rates, key=lambda d: d['roi_1h'], reverse=True)[0]
            short_instrument = sorted(symbol_long_rates, key=lambda d: d['roi_1h'], reverse=True)[0]
            best_inter_exchange_pair = {
                'underlying_symbol'  : symbol,
                'income_exchange'    : long_instrument['exchange'],
                'income_instrument'  : long_instrument['instrument'],
                'income_side'        : long_instrument['side'],
                'income_rate'        : long_instrument['roi_1h'],
                'expense_exchange'   : short_instrument['exchange'],
                'expense_instrument' : short_instrument['instrument'],
                'expense_side'       : short_instrument['side'],
                'expense_rate'       : short_instrument['roi_1h'],
                'roi_1h'             : long_instrument['roi_1h'] + short_instrument['roi_1h']
            }
            inter_exchange_trades.append(best_inter_exchange_pair)

            #(2)# Find Best Intra-Exchange Return
            temp_trades = []
            exchanges = 1
            for exchange in exchanges:
                ## For each exchange, find the best futures and spot pair
                exchange_long_rates = [d for d in symbol_long_rates if d['exchange'] == exchange]
                exchange_short_rates = [d for d in symbol_short_rates if d['exchange'] == exchange]

                long_instrument = sorted(exchange_long_rates,key=lambda d: d['roi_1h'], reverse=True)[0]
                short_instrument = sorted(exchange_short_rates, key=lambda d: d['roi_1h'], reverse=True)[0]

                temp_trades.append({
                    'underlying_symbol'  : symbol,
                    'income_exchange'    : long_instrument['exchange'],
                    'income_instrument'  : long_instrument['instrument'],
                    'income_side'        : long_instrument['side'],
                    'income_rate'        : long_instrument['roi_1h'],
                    'expense_exchange'   : short_instrument['exchange'],
                    'expense_instrument' : short_instrument['instrument'],
                    'expense_side'       : short_instrument['side'],
                    'expense_rate'       : short_instrument['roi_1h'],
                    'roi_1h'             : long_instrument['roi_1h'] + short_instrument['roi_1h']
                })

            ## For each symbol, find the best return for a long instrument and short instrument from the same exchange
            best_intra_exchange_pair = sorted(temp_trades, key=lambda d: d['roi_1h'], reverse=True)[0]  # highest return intra-exchange pair
            intra_exchange_trades.append(best_intra_exchange_pair)

        ## Sort Symbol Trades by ROI
        inter_exchange_trades = sorted(inter_exchange_trades, key=lambda d: d['roi_1h'])
        intra_exchange_trades = sorted(intra_exchange_trades, key=lambda d: d['roi_1h'])

        return inter_exchange_trades, intra_exchange_trades



    def back_test(self):
        funding_datetimes = []
        for dt in funding_datetimes:
            pass

    def forward_test(self):
        pass


if __name__ == "__main__":
    StrategyFundingRateArb().strategy()