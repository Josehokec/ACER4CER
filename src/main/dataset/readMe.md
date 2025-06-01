# Datasets

Git limits the size of uploaded files, so we split the two datasets.
```shell
./file_split.sh nasdaq.csv 2
./file_split.sh crimes.csv 2
```


**Run the following script to restore the original dataset file:**
```shell
./file_recovery.sh nasdaq
./file_recovery.sh crimes
gunzip job.csv.zip
```
