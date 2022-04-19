# Case Study Project

Case Study for crash dataset analysis is done and solutions have been added for 8 given use cases.

## ETL Project Structure

The basic project structure is as follows:

```bash
root/
 |-- configs/
 |   |-- params.json
 |-- utils/
 |   |-- app_logging.py
 |   |-- spark.py
 |-- docs/
 |   |-- Data Dictionary.xlsx
 |   |-- Analysis.txt
 |   |-- Exec_Results.html
 |-- src/
 |   |-- process_data.py
 |-- tests/
 |   |-- test_data/
 |   |-- | -- Damages_use.csv
 |   |-- test_process_data.py
```


## Running the job

Assuming that the `$SPARK_HOME` environment variable points to your local Spark installation folder, then the processing job can be run from the project's root directory using the following command from the terminal,

```bash
$SPARK_HOME/bin/spark-submit \
--master local[*] \
--files configs/params.json \
jobs/process_data.py
```
