# lopatar
A simple json-nd data pumper to DataSet aka Scalyr; WITH backpressure support.

```
usage: lopatar.py [-h] [--token DATASET_TOKEN] [--ts-field TS_FIELD] [--alt-ts-field ALT_TS_FIELD] [--debug]
                  [--api API] [--threads THREADS] [--plot] [--progress-bar]
                  FILE

Shovel data into dataset/scalyr

positional arguments:
  FILE                  File path of s3 URI for file to import

optional arguments:
  -h, --help            show this help message and exit
  --token DATASET_TOKEN
                        Dataset/Scalyr API token
  --ts-field TS_FIELD   Timestamp field name, will be converted to "ts"
  --alt-ts-field ALT_TS_FIELD
                        Field name which will additionally be converted to epoch time (not to be confused with ts
                        field)
  --debug               Enable debugging mode
  --api API             Scalyr/DataSet API endpoint
  --threads THREADS, -t THREADS
                        Set the number of worker threads
  --plot                Plot a nice performance chart!
  --progress-bar        Display a progress bar
  ```


# Examples

## Upload some data 
``` 
python .\lopatar.py --token xxxx --alt-ts-field datetime "D:\50k.json" --threads 20 --progress-bar --plot 
```
