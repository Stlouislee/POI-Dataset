# Data Card: Preprocessed POI Recommendation Datasets

## Preprocessing Parameters
- **Filter Method**: iterative
- **Min POI Checkins**: 0
- **Min User Checkins**: 10
- **Min Trajectory Length**: 2
- **Trajectory Gap (Hours)**: 24.0

## Dataset Statistics
| City | Original Events | Post-Filter Events | Valid Users | Train Set Size | Val Set Size | Test Set Size |
|------|-----------------|--------------------|-------------|----------------|--------------|---------------|
| NYC | 227428 | 227428 | 1083 | 170396 | 7908 | 8218 |
| TKY | 573703 | 573703 | 2293 | 435242 | 26915 | 29748 |
| CA | 636512 | 608942 | 8568 | 433302 | 19096 | 31643 |

## Data Format
The processed data is saved in JSONL format. Each line is a JSON object with the following fields:
- `user`: User ID (String)
- `poi`: POI ID (String)
- `cat_id`: POI Category ID (String)
- `cat_name`: POI Category Name (String)
- `lat`: Latitude (Float)
- `lon`: Longitude (Float)
- `time`: UTC Unix Timestamp (Integer)
- `traj_id`: Unique identifier for the trajectory, structured as `{user_id}_{trajectory_index}` (String)
- `is_eval_target`: (Only in Val/Test sets) Boolean indicating if this is the final check-in of a trajectory, used as the prediction target.
