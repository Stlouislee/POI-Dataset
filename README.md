# Preprocessed POI Recommendation Datasets

This repository contains preprocessed Point-Of-Interest (POI) check-in records for three regions:
- **NYC**: New York City (Foursquare check-ins from April 2012 to February 2013)
- **TKY**: Tokyo (Foursquare check-ins from April 2012 to February 2013)
- **CA**: California / Nevada (Gowalla check-ins from Feb 2009 to Oct 2010)

The datasets are structured specifically for Point-Of-Interest recommendation models.

## Preprocessing Protocol

The original raw data underwent preprocessing to align with standard POI recommendation evaluation protocols. The preprocessing script (`preprocess.py`) supports configurable parameters to generate different variations of the datasets.

Key steps in the pipeline:
1. **N-core Filtering (POIs & Users)**: Configurable threshold to filter out inactive users and unpopular POIs. Supports both `iterative` (filter until no drops) and `one-pass` filtering strategies.
2. **Trajectory Splitting**: Divides continuous user check-in records into independent trajectories based on a configurable time gap (e.g., 24 hours), excluding short trajectories (e.g., length < 2).
3. **Chronological Splitting**: Check-in records within valid trajectories are ordered chronologically and split into Train (80%), Validation (10%), and Test (10%) sets.
4. **Validation/Test Entity Filtering**: Ensures validation and test sets only contain POIs and users present in the training set.
5. **Evaluation Target Marking**: The last check-in record of each trajectory in the validation and test sets is marked with `is_eval_target = True` for evaluation.

## Directory Structure

The preprocessed data is organized dynamically based on the filtering parameters used during the generation process. For example:
- `preprocessed_data/filter-iterative_minpoi-10_minuser-10_mintrajlen-2_gap-24.0h/`
- `preprocessed_data/filter-iterative_minpoi-0_minuser-10_mintrajlen-2_gap-24.0h/`

Each directory contains a `datacard.md` with detailed statistics for that specific dataset build, and subfolders for each city (`NYC`, `TKY`, `CA`) containing:
- `train.jsonl`
- `val.jsonl`
- `test.jsonl`

## File Format

Files are stored in JSON Lines (`.jsonl`) format. Each line represents a check-in event:

```json
{
  "user": "470",
  "poi": "4bf58dd8d48988d127951735",
  "lat": 40.719810375488535,
  "lon": -74.00258103213994,
  "time": 1333476009,
  "traj_id": "470_0"
}
```

Validation and test records contain an additional boolean field:
```json
{
  ...
  "is_eval_target": true
}
```

## Reproducibility

The source script `preprocess.py` is included. You can reproduce the datasets or generate new variations by running:

```bash
python3 preprocess.py --filter-method iterative --min-poi 10 --min-user 10 --min-traj-len 2 --traj-gap-hours 24.0
```

## Large File Storage (LFS)

The `.jsonl` data files in this repository are managed using Git LFS. Ensure you have Git LFS installed to fetch the actual file contents.