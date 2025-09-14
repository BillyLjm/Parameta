# Problem 1 – the rates problem

## Files

You have three data files, `rates_ccy_data.csv`, `rates_price_data.parq.gzip` and `rates_spot_rate_data.parq.gzip`

1. `rates_ccy_data` is a currency pair reference file. It tells us the currency pairs (ccy_pair) in scope, whether we need to convert the price, and the conversion factor. Price in this example relates to the FX rate for the ccy_pair.
2. `rates_price_data` is a set of timestamped prices for ccy_pairs.
3. `rates_spot_rate_data` is a timestamped set of fx rates for ccy_pairs. The FX rates are in a column called `spot_mid_rate`.

## Goal

Generate a new price for each row in rates_price_data. The new price depends on whether that ccy_pair is supported, needs to be converted and has sufficient data to convert:

- If conversion is not required, then the new price is simply the `existing price`
- If conversion is required, then the new price is: (`existing price`/ `conversion factor`) + `spot_mid_rate`
- If there is insufficient data to create a new price then capture this fact in some way

For each row in rates_price_data, if conversion is required then we need to find an appropriate `spot_mid_rate`. This is defined as the most recently timestamped rate for that specific ccy_pair within the hour that precedes the timestamped price.

Please generate the final price for each row in price_data. You can capture this as an output data file – csv is preferable.

## Benchmark

For reference, the benchmark calculation time is < 1 second using python 3 with 16GB ram