
# Parameta Solutions - Coding Test

This is my solution for the **Data Science coding test** at [Parameta Solutions](https://www.parametasolutions.com).

There are two main problems, each contained in its own folder:

1. `rates_test/` for a currency conversion problem
2. `stdev_test/` for a rolling standard deviation problem

## Setup

First, create a virtual environment and install the required packages:

```bash
python -m venv venv
source venv/bin/activate    # On Windows use `venv\Scripts\activate.bat`
pip install -r requirements.txt
```

## Running the Solutions

The solutions can be run via the python files in the `scripts` subfolders.

### 1. Rates Problem

The solution is implemented in [`rates_soln.py`](rates_test/scripts/rates_soln.py) and can be run via

```bash
python rates_test/scripts/rates_soln.py
```

This will output the results at [`rates_test/results/rates_output.csv`](rates_test/results/rates_output.csv)


### 2. Standard Deviation Problem

The solution is implemented in [`stdev_soln.py`](stdev_test/scripts/stdev_soln.py) and can be run via

```bash
python stdev_test/scripts/stdev_soln.py
```

This will output the results at [`stdev_test/results/stdev_output.csv`](stdev_test/results/stdev_output.csv)