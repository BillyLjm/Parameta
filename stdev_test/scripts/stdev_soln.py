from pathlib import Path
import pandas as pd

class stdev_soln:
    """
    Class for calculating rolling standard deviations of price data for multiple
    securities, considering only contiguous time windows.

    Attributes
    ----------
    window : int
        Rolling window size for standard deviation calculations (default 20).
    timestep : pd.Timedelta
        Expected time difference between consecutive snapshots (default 1 hour).
    state : pd.DataFrame
        Stores original price data and calculated rolling standard deviations.
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

    def rolling_stdev(self, df):
        """
        Calculates the rolling standard deviation for each security in the 
        DataFrame, only for windows where the `snap_time` values are contiguous.
    
        Parameters
        ----------
        df : pd.DataFrame
            Price data indexed by ('security_id', 'snap_time').
    
        Returns
        -------
        pd.DataFrame
            Rolling standard deviations for contiguous windows
        """
        # check if time-series contiguous
        df = df.dropna().sort_index()
        df['cont'] = df.index.get_level_values(1).to_series().groupby(
            df.index.get_level_values(0)).diff().eq(self.timestep).values
        
        # rolling_stdev
        df_agg = pd.concat([
            # rolling_stdev
            df.groupby('security_id')[df.columns[:-1]]\
                .rolling(self.window).std(),
            # check if contiguous
            df.groupby('security_id')[['cont']]\
                .rolling(self.window-1).min().astype('boolean'),
        ], axis=1, ignore_index=False)

        # only return contiguous windows
        df_agg = df_agg[df_agg['cont'] == True].drop(columns='cont') 
        df_agg = df_agg.droplevel(0).add_suffix('_std')
        return df_agg

    def calculate(self, fname):
        """
        Load price data from a file and compute rolling standard deviations.

        Parameters
        ----------
        fname : str
            Path to the parquet file containing price data.
        """
        # read time-series data
        df = pd.read_parquet(fname)
        df['snap_time'] = pd.to_datetime(df['snap_time'])
        df = df.set_index(['security_id', 'snap_time'])

        # calculate for each column
        for i in df.columns:
            df = df.join(self.rolling_stdev(df[[i]].dropna()), how='outer')

        self.state = df

    def get_state(self):
        """
        Return the current stored state of raw prices and calculated rolling 
        standard deviations.

        Returns
        -------
        pd.DataFrame
            DataFrame containing raw prices and rolling standard deviations
        """
        return self.state

    def output(self, start=None, end=None, security_id=None, fname=None):
        """
        Calculates the most recent rolling standard deviations for the specified
        securities and time range, and optionally saving to CSV.

        Parameters
        ----------
        start : pd.Timestamp or str, optional
            Start timestamp for constructing the time index. If None, uses the 
            earliest timestamp in the data.
        end : pd.Timestamp or str, optional
            End timestamp for constructing the time index. If None, uses the 
            earliest timestamp in the data.
        security_id : list of str, optional
            Security ID(s) to filter. If None, includes all securities.
        fname : str, optional
            Path to save CSV file. If None, file is not written.

        Returns
        -------
        pd.DataFrame
            DataFrame of most recent rolling standard deviations.
        """
        # default arguments
        start = start or self.state.index.get_level_values('snap_time').min()
        end = end or self.state.index.get_level_values('snap_time').max()
        security_id = security_id or self.state.index.get_level_values('security_id').unique()

        # create every possible snap_time
        out = pd.DataFrame(index=pd.MultiIndex.from_product(
            [security_id, pd.date_range(start=start, end=end, freq=self.timestep)], 
        names=['security_id', 'snap_time']))
        
        # get stdev
        out = out.join(self.state.filter(regex='_std$'), how='left')
        out = out.ffill() # if stdev not available, use last most recent value
        if fname != None: out.to_csv(fname)
        return out

if __name__ == '__main__':
    dirr = Path(__file__).parent.parent
    soln = stdev_soln()
    soln.calculate(dirr/'data'/'stdev_price_data.parq.gzip')
    soln.output(start='2021-11-20 00:00:00', end='2021-11-23 09:00:00',
                fname=dirr/'results'/'stdev_output.csv')