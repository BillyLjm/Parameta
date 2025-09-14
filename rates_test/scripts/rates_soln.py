from pathlib import Path
import pandas as pd

class rates_soln:
    """
    Class to calculate adjusted currency pair prices by merging raw prices, 
    conversion specifications, and spot mid rates.

    Attributes
    ----------
    state : pd.DataFrame
        Stores the merged and processed data, including calculated 'final_price'.
    """
    
    def __init__(self):
        """
        Initialize the RateSoln class with an empty state DataFrame.
        """
        self.state = pd.DataFrame()

    def calculate(self, price_fname, conv_fname, spot_fname):
        """
        Computes the final prices from given price data, conversion 
        specifications and spot mid rates.

        Parameters
        ----------
        price_fname : str
            Path to the parquet file containing raw price data.
        conv_fname : str
            Path to the CSV file containing conversion specifications.
        spot_fname : str
            Path to the parquet file containing spot mid-rate data.
        """
        # read price data
        df_price = pd.read_parquet(price_fname)
        df_price['timestamp'] = pd.to_datetime(df_price['timestamp'])

        # merge conversion specs
        df_pairs = pd.read_csv(conv_fname, dtype={'convert_prices':'boolean'})
        df = df_price.merge(df_pairs, on='ccy_pair', how='left')

        # merge spot mid rates
        df_spot = pd.read_parquet(spot_fname)
        df_spot['timestamp'] = pd.to_datetime(df_spot['timestamp'])
        df = pd.merge_asof(
            df.sort_values('timestamp'), df_spot.sort_values('timestamp'), 
            on='timestamp', by='ccy_pair', direction='backward', 
            tolerance=pd.Timedelta('1hr')
        )

        # calculate new prices
        df['final_price'] = df['price'].where(df['convert_price'] == False, 
            df['price'] / df['conversion_factor'] + df['spot_mid_rate'])
        self.state = df.sort_values(by=['timestamp', 'security_id', 'final_price'])

    def get_state(self):
        """
        Return the current internal state DataFrame.

        Returns
        -------
        pd.DataFrame
            The merged and processed DataFrame including 'final_price'.
        """
        return self.state

    def output(self, fname=None):
        """
        Returns the calculated 'final_price' and optionally write to CSV.

        Parameters
        ----------
        fname : str, optional
            Path to save the output CSV file. If None, no file is written.

        Returns
        -------
        pd.DataFrame
            DataFrame of ['timestamp', 'security_id', 'price', 'ccy_pair', 
            'final_price']. Entries where the price could not be calculated are 
            filled with the string 'insufficient data'.
        """
        out = self.state[['timestamp', 'security_id', 'price' ,'ccy_pair', 
                          'final_price']].copy()
        out['final_price'] = out['final_price'].fillna('insufficient data')
        if fname != None: out.to_csv(fname, index=False)
        return out

if __name__ == '__main__':
    dirr = Path(__file__).parent.parent
    soln = rates_soln()
    soln.calculate(
        price_fname = dirr/'data'/'rates_price_data.parq.gzip',
        conv_fname = dirr/'data'/'rates_ccy_data.csv', 
        spot_fname = dirr/'data'/'rates_spot_rate_data.parq.gzip'
    )
    soln.output(dirr/'results'/'rates_output.csv')
