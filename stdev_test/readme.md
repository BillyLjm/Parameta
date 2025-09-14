# Problem 2 – the standard deviation problem

## Files

You have a single gzip compressed parquet file called `stdev_price_data.parq.gzip`

It consists of timestamped `bid`, `mid` and `ask` prices for security IDs. These prices are generated in hourly snaps. For each `security_id` at all possible hourly snap times in the interval defined below, we need to know the rolling standard deviation for bids, for mids and for asks. In other words, for each snap hour, generate the rolling standard deviation for each of bids, mids and asks for each ID.

## Goal

To generate a standard deviation for a security id at a given hourly snap time, you need the most recent set of 20 contiguous hourly snap values for the security id. By contiguous we mean there are no gaps in the set of hourly snaps.

Please generate the standard deviation of price for each price type for each security for every possible hourly snap from `2021-11-20 00:00:00` to `2021-11-23 09:00:00`. You can capture this as an output data file – csv is preferable

Some thoughts – in this problem, consider yourself at snap point in time. Look back at previous data and work out the stdev. An hour later, you are at the next snap time, and you calculate the new stdev. You can think of this as either:

1. **Updating based on previous data**
   You could consider storing stdev calculation state data at each snap time and using that at the next snap time.

2. **A completely new calculation**.
   You ignore calculation data from previous steps and do the calculation afresh each time.

Although you only need to calculate from 20th Nov for a few days, in reality we run this calculation every hour and so your solution should be able to handle a request to show the result on any given hour. You are not required to support this but it should help guide your solution.

Although you are working with files, in reality we would be pulling this data from a database and caching any intermediate or state data to local (file) or remote (database) storage so the idea of having state data that can be accessed in the future is not unusual. For example, if someone queries our result we need to be able to easily regenerate it.

## Benchmark

For reference, the benchmark calculation time is again < 1 second using python 3 with 16GB ram