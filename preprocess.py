import os
import csv
import json
import math
import argparse
import ast
from collections import Counter, defaultdict
import datetime as dt

def parse_time(time_str, fmt="tsmc"):
    if fmt == "tsmc":
        return dt.datetime.strptime(time_str, "%a %b %d %H:%M:%S %z %Y")
    else:
        return dt.datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)

def parse_gowalla_category(cat_str):
    cat_id, cat_name = 'unknown', 'unknown'
    if not cat_str:
        return cat_id, cat_name
    try:
        # String format: "[{'url': '/categories/45', 'name': 'Airport'}]"
        cats = ast.literal_eval(cat_str)
        if cats and isinstance(cats, list) and len(cats) > 0:
            cat_url = cats[0].get('url', '')
            cat_id = cat_url.split('/')[-1] if cat_url else 'unknown'
            cat_name = cats[0].get('name', 'unknown')
    except Exception:
        pass
    return str(cat_id), str(cat_name)

def read_data(file_path, city):
    events = []
    if city in ['NYC', 'TKY']:
        with open(file_path, 'r', encoding='latin-1') as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) < 8:
                    continue
                uid = parts[0]
                poi = parts[1]
                cat_id = parts[2]
                cat_name = parts[3]
                lat, lon = parts[4], parts[5]
                ts = parse_time(parts[7], "tsmc")
                events.append({
                    'user': uid, 'poi': poi, 
                    'cat_id': cat_id, 'cat_name': cat_name,
                    'lat': float(lat), 'lon': float(lon), 
                    'time': int(ts.timestamp())
                })
    else:
        with open(file_path, 'r', encoding='latin-1') as f:
            reader = csv.DictReader(f)
            for row in reader:
                uid = row['UserId']
                poi = row['PoiId']
                
                cat_str = row.get('PoiCategoryId', '')
                cat_id, cat_name = parse_gowalla_category(cat_str)
                
                lat, lon = row['Latitude'], row['Longitude']
                ts = parse_time(row['UTCTime'], "gowalla")
                events.append({
                    'user': uid, 'poi': poi, 
                    'cat_id': cat_id, 'cat_name': cat_name,
                    'lat': float(lat), 'lon': float(lon), 
                    'time': int(ts.timestamp())
                })
    return events

def preprocess(city, source_file, out_dir, filter_method, min_poi, min_user, min_traj_len, traj_gap_hours):
    print(f"Processing {city} with {filter_method} filtering...")
    events = read_data(source_file, city)
    original_events_count = len(events)
    print(f"  Raw events: {original_events_count}")
    
    # 1 & 2: Filter POIs and users
    if filter_method == 'iterative':
        while True:
            poi_counts = Counter(e['poi'] for e in events)
            user_counts = Counter(e['user'] for e in events)
            
            filtered = [e for e in events if poi_counts[e['poi']] >= min_poi and user_counts[e['user']] >= min_user]
            if len(filtered) == len(events):
                break
            events = filtered
    elif filter_method == 'one-pass':
        poi_counts = Counter(e['poi'] for e in events)
        user_counts = Counter(e['user'] for e in events)
        events = [e for e in events if poi_counts[e['poi']] >= min_poi and user_counts[e['user']] >= min_user]
    else:
        raise ValueError("Invalid filter method")
    
    post_filter_events_count = len(events)
    print(f"  Events after filtering: {post_filter_events_count}")
    
    # 3: Divide into trajectories
    user_events = defaultdict(list)
    for e in events:
        user_events[e['user']].append(e)
        
    for u in user_events:
        user_events[u].sort(key=lambda x: x['time'])
        
    trajectories = defaultdict(list)
    gap_seconds = int(traj_gap_hours * 3600)
    for u, evs in user_events.items():
        if not evs: continue
        curr_traj = [evs[0]]
        for e in evs[1:]:
            if e['time'] - curr_traj[-1]['time'] > gap_seconds:
                if len(curr_traj) >= min_traj_len:
                    trajectories[u].append(curr_traj)
                curr_traj = [e]
            else:
                curr_traj.append(e)
        if len(curr_traj) >= min_traj_len:
            trajectories[u].append(curr_traj)
            
    valid_users_count = len(trajectories)
    print(f"  Users with valid trajectories: {valid_users_count}")
    
    # 4: Sequence and split 80/10/10
    train_data, val_data, test_data = [], [], []
    train_pois, train_users = set(), set()
    
    for u, trajs in trajectories.items():
        flat_evs = []
        for t_idx, t in enumerate(trajs):
            for e in t:
                e_copy = dict(e)
                e_copy['traj_id'] = f"{u}_{t_idx}"
                flat_evs.append(e_copy)
                
        n = len(flat_evs)
        if n == 0: continue
        train_end = int(math.floor(n * 0.8))
        val_end = int(math.floor(n * 0.9))
        
        u_train = flat_evs[:train_end]
        u_val = flat_evs[train_end:val_end]
        u_test = flat_evs[val_end:]
        
        train_data.extend(u_train)
        val_data.extend(u_val)
        test_data.extend(u_test)
        
        if u_train:
            train_users.add(u)
            for e in u_train:
                train_pois.add(e['poi'])
                
    # 5: The validation/test set has to contain all the users and POIs in the training set.
    def filter_split(split_data):
        return [e for e in split_data if e['user'] in train_users and e['poi'] in train_pois]
        
    val_data = filter_split(val_data)
    test_data = filter_split(test_data)
    
    # 6: Only evaluate the last check-in record of each trajectory in val/test.
    def mark_last_in_traj(split_data):
        traj_groups = defaultdict(list)
        for e in split_data:
            traj_groups[e['traj_id']].append(e)
        
        marked = []
        for t_id, t_evs in traj_groups.items():
            t_evs.sort(key=lambda x: x['time'])
            for i, e in enumerate(t_evs):
                e['is_eval_target'] = (i == len(t_evs) - 1)
                marked.append(e)
        return marked
        
    val_data = mark_last_in_traj(val_data)
    test_data = mark_last_in_traj(test_data)
    
    train_size = len(train_data)
    val_size = len(val_data)
    test_size = len(test_data)
    print(f"  Final Train: {train_size}, Val: {val_size}, Test: {test_size}\n")
    
    os.makedirs(os.path.join(out_dir, city), exist_ok=True)
    
    def save_jsonl(data, path):
        with open(path, 'w') as f:
            for d in data:
                f.write(json.dumps(d) + '\n')
                
    save_jsonl(train_data, os.path.join(out_dir, city, 'train.jsonl'))
    save_jsonl(val_data, os.path.join(out_dir, city, 'val.jsonl'))
    save_jsonl(test_data, os.path.join(out_dir, city, 'test.jsonl'))

    return {
        'city': city,
        'original_events': original_events_count,
        'post_filter_events': post_filter_events_count,
        'valid_users': valid_users_count,
        'train_size': train_size,
        'val_size': val_size,
        'test_size': test_size
    }

def create_datacard(out_dir, params, stats_list):
    md_content = f"# Data Card: Preprocessed POI Recommendation Datasets\n\n"
    md_content += "## Preprocessing Parameters\n"
    md_content += f"- **Filter Method**: {params['filter_method']}\n"
    md_content += f"- **Min POI Checkins**: {params['min_poi']}\n"
    md_content += f"- **Min User Checkins**: {params['min_user']}\n"
    md_content += f"- **Min Trajectory Length**: {params['min_traj_len']}\n"
    md_content += f"- **Trajectory Gap (Hours)**: {params['traj_gap_hours']}\n\n"
    
    md_content += "## Dataset Statistics\n"
    md_content += "| City | Original Events | Post-Filter Events | Valid Users | Train Set Size | Val Set Size | Test Set Size |\n"
    md_content += "|------|-----------------|--------------------|-------------|----------------|--------------|---------------|\n"
    
    for s in stats_list:
        md_content += f"| {s['city']} | {s['original_events']} | {s['post_filter_events']} | {s['valid_users']} | {s['train_size']} | {s['val_size']} | {s['test_size']} |\n"
        
    md_content += "\n## Data Format\n"
    md_content += "The processed data is saved in JSONL format. Each line is a JSON object with the following fields:\n"
    md_content += "- `user`: User ID (String)\n"
    md_content += "- `poi`: POI ID (String)\n"
    md_content += "- `cat_id`: POI Category ID (String)\n"
    md_content += "- `cat_name`: POI Category Name (String)\n"
    md_content += "- `lat`: Latitude (Float)\n"
    md_content += "- `lon`: Longitude (Float)\n"
    md_content += "- `time`: UTC Unix Timestamp (Integer)\n"
    md_content += "- `traj_id`: Unique identifier for the trajectory, structured as `{user_id}_{trajectory_index}` (String)\n"
    md_content += "- `is_eval_target`: (Only in Val/Test sets) Boolean indicating if this is the final check-in of a trajectory, used as the prediction target.\n"
    
    with open(os.path.join(out_dir, "datacard.md"), "w") as f:
        f.write(md_content)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--filter-method', type=str, choices=['iterative', 'one-pass'], default='iterative')
    parser.add_argument('--min-poi', type=int, default=10)
    parser.add_argument('--min-user', type=int, default=10)
    parser.add_argument('--min-traj-len', type=int, default=2)
    parser.add_argument('--traj-gap-hours', type=float, default=24.0)
    
    args = parser.parse_args()

    dir_name = f"filter-{args.filter_method}_minpoi-{args.min_poi}_minuser-{args.min_user}_mintrajlen-{args.min_traj_len}_gap-{args.traj_gap_hours}h"
    out_dir = os.path.join("/home/ubuntu/poi_baseline/data_preprocessing/preprocessed_data", dir_name)
    os.makedirs(out_dir, exist_ok=True)
    
    datasets = [
        ("NYC", "/home/ubuntu/poi_baseline/source_data/dataset_TSMC2014_NYC.txt"),
        ("TKY", "/home/ubuntu/poi_baseline/source_data/dataset_TSMC2014_TKY.txt"),
        ("CA", "/home/ubuntu/poi_baseline/source_data/dataset_gowalla_ca_ne.csv")
    ]

    stats_list = []
    for city, path in datasets:
        stats = preprocess(city, path, out_dir, args.filter_method, args.min_poi, args.min_user, args.min_traj_len, args.traj_gap_hours)
        stats_list.append(stats)
        
    create_datacard(out_dir, vars(args), stats_list)
    print(f"All done! Data and datacard saved in {out_dir}")