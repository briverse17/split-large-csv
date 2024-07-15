# Split Large CSV by value

Split one large CSV file into multiple files based on value combinations of specific columns

## NOTE

### This code was well tested on Ubuntu 23.10. Please make appropriate changes if you are on other platforms

## Build

0. Prerequisite: `Python >= 3.11.6`
1. Create virtual environment

    ```shell
    python3 -m venv .venv
    ```

    Replace `.venv` with any name of your choice

2. Activate the virtual environment

    ```shell
    source .venv/bin/activate
    ```

3. Install dependencies

    ```shell
    python3 -m pip install -r requirements.txt
    ```

4. Run the program

    ```shell
    python3 app/split.py
    ```

    Expected results

    ```shell
    $ python3 app/split.py
    Splitting done in: 1.93 seconds
    Total rows in original file (excluding header): 16045
    Total rows across output files: 16045
    Row count mismatch between original and output files.
    Number of unique combinations found: 4214
    Number of output files created: 4214
    ```
