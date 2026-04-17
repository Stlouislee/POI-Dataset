# Data Card: Preprocessed POI Recommendation Datasets

## Preprocessing Parameters
- **Filter Method**: one-pass
- **Min POI Checkins**: 10
- **Min User Checkins**: 10
- **Min Trajectory Length**: 2
- **Trajectory Gap (Hours)**: 24.0

## Dataset Statistics
| City | Original Events | Post-Filter Events | Valid Users | Train Set Size | Val Set Size | Test Set Size |
|------|-----------------|--------------------|-------------|----------------|--------------|---------------|
| NYC | 227428 | 147938 | 1082 | 99326 | 12342 | 12821 |
| TKY | 573703 | 447570 | 2293 | 323090 | 40358 | 41453 |
| CA | 636512 | 363983 | 8025 | 229743 | 28587 | 32775 |

## Data Format
The processed data is saved in JSONL format. Each line is a JSON object with the following fields:
- `user`: User ID (String)
- `poi`: POI ID (String)
- `lat`: Latitude (Float)
- `lon`: Longitude (Float)
- `time`: UTC Unix Timestamp (Integer)
- `traj_id`: Unique identifier for the trajectory, structured as `{user_id}_{trajectory_index}` (String)
- `is_eval_target`: (Only in Val/Test sets) Boolean indicating if this is the final check-in of a trajectory, used as the prediction target.
