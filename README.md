# movie-score-data-pipeline
Test program for a data pipeline and score

# Setup

## Install dependencies

```bash
pip3 install -r requirements.txt
```

# Run

```bash
python3 main.py
```

# Test

```bash
pytest tests/test_pipeline.py
pytest tests/test_readers.py
```

# Structure explanation

The program has a models.py file that defines the data models for the pipeline.

The readers folder contains the data readers for each data source.

Teh data folder constains the original data to be processed and data tests for the testings.

