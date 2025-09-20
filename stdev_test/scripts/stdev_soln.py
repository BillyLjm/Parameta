from pathlib import Path
import pandas as pd

class stdev_soln:
    """
    Class to compute rolling standard deviation of price data and track continuity.

    Attributes
    ----------
    window : int
        Rolling window size for standard deviation calculations (default 20).
    timestep : pd.Timedelta
        Expected time difference between consecutive snapshots (default 1 hour).
    state : pd.DataFrame
        Stores both raw_price and rolling_stdev data with a MultiIndex on columns: 
        level 0 = 'raw_price' or 'rolling_stdev', level 1 = original column names.
    """
    
    def __init__(self, window=20, timestep=pd.Timedelta('1hr')):
        """
        Initialize the rolling window and timestep for contiguity.

        Parameters
        ----------
        window : int, optional
            Rolling window size for standard deviation (default is 20).
        timestep : pd.Timedelta, optional
            Expected interval between snapshots for continuity check (default is 1 hour).
        """
        self.window = window
        self.timestep = timestep
        self.state = pd.DataFrame()

    def calculate(self, fname):
        """
        Read price data, compute rolling standard deviation, and check time-contiguity.

        Parameters
        ----------
        fname : str
            Path to the parquet file containing price data.
        """
        # read time-series data
        df = pd.read_parquet(fname)
        df['snap_time'] = pd.to_datetime(df['snap_time'])

        # check if time-series contiguous
        df = df.sort_values(by=['security_id', 'snap_time']).reset_index(drop=True)
        df['cont'] = (df.groupby('security_id')['snap_time'].diff() == self.timestep)

        # rolling_stdev
        df_agg = pd.concat([
            # index
            df[['snap_time', 'security_id']],
            # rolling_stdev
            df.groupby('security_id')[['bid', 'mid', 'ask']]\
                .rolling(self.window).std().reset_index(drop=True),
            # check if contiguous
            df.groupby('security_id')[['cont']].rolling(self.window-1)\
                .min().astype('boolean').reset_index(drop=True),
        ], axis=1, ignore_index=False)

        # save both raw and rolling as state
        df = df.set_index(['security_id', 'snap_time'])
        df.columns = pd.MultiIndex.from_product([['raw_price'], df.columns])
        df_agg = df_agg.set_index(['security_id', 'snap_time'])
        df_agg.columns = pd.MultiIndex.from_product([['rolling_stdev'], df_agg.columns])
        self.state = df.join(df_agg, how='outer')

    def get_state(self):
        """
        Return the current stored state of raw_price and rolling_stdev data.

        Returns
        -------
        pd.DataFrame
            MultiIndexed DataFrame containing 'raw_price' and 'rolling_stdev' 
            data for each security and timestamp.
        """
        return self.state

    def output(self, start=None, end=None, fname=None):
        """
        Filter rolling data by optional time range and/or write to CSV.

        Parameters
        ----------
        fname : str, optional
            Path to save CSV file (if None, file is not written).
        start : pd.Timestamp or str, optional
            Start timestamp for filtering rows (inclusive). If None, no lower bound.
        end : pd.Timestamp or str, optional
            End timestamp for filtering rows (inclusive). If None, no upper bound.

        Returns
        -------
        pd.DataFrame
            Filtered rolling_stdev DataFrame with columns ['bid', 'mid', 'ask'].
        """
        out = self.state['rolling_stdev']
        out = out[out['cont'] == True][['bid', 'mid', 'ask']]
        if start != None: out = out[out.index.get_level_values('snap_time') >= start]
        if end != None: out = out[out.index.get_level_values('snap_time') <= end]
        if fname != None: out.to_csv(fname)
        return out

if __name__ == '__main__':
    dirr = Path(__file__).parent.parent
    soln = stdev_soln()
    soln.calculate(dirr/'data'/'stdev_price_data.parq.gzip')
    soln.output(start='2021-11-20 00:00:00', end='2021-11-23 09:00:00',
                fname=dirr/'results'/'stdev_output.csv')