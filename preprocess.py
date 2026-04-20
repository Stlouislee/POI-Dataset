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
        return dt.datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=dt.timezone.utc
        )


def parse_gowalla_category(cat_str):
    cat_id, cat_name = "unknown", "unknown"
    if not cat_str:
        return cat_id, cat_name
    try:
        # String format: "[{'url': '/categories/45', 'name': 'Airport'}]"
        cats = ast.literal_eval(cat_str)
        if cats and isinstance(cats, list) and len(cats) > 0:
            cat_url = cats[0].get("url", "")
            cat_id = cat_url.split("/")[-1] if cat_url else "unknown"
            cat_name = cats[0].get("name", "unknown")
    except Exception:
        pass
    return str(cat_id), str(cat_name)


def read_data(file_path, city):
    events = []
    if city in ["NYC", "TKY"]:
        with open(file_path, "r", encoding="latin-1") as f:
            for line in f:
                parts = line.strip().split("\t")
                if len(parts) < 8:
                    continue
                uid = parts[0]
                poi = parts[1]
                cat_id = parts[2]
                cat_name = parts[3]
                lat, lon = parts[4], parts[5]
                ts = parse_time(parts[7], "tsmc")
                events.append(
                    {
                        "user": uid,
                        "poi": poi,
                        "cat_id": cat_id,
                        "cat_name": cat_name,
                        "lat": float(lat),
                        "lon": float(lon),
                        "time": int(ts.timestamp()),
                    }
                )
    else:
        with open(file_path, "r", encoding="latin-1") as f:
            reader = csv.DictReader(f)
            for row in reader:
                uid = row["UserId"]
                poi = row["PoiId"]

                cat_str = row.get("PoiCategoryId", "")
                cat_id, cat_name = parse_gowalla_category(cat_str)

                lat, lon = row["Latitude"], row["Longitude"]
                ts = parse_time(row["UTCTime"], "gowalla")
                events.append(
                    {
                        "user": uid,
                        "poi": poi,
                        "cat_id": cat_id,
                        "cat_name": cat_name,
                        "lat": float(lat),
                        "lon": float(lon),
                        "time": int(ts.timestamp()),
                    }
                )
    return events


def preprocess(
    city,
    source_file,
    out_dir,
    filter_method,
    min_poi,
    min_user,
    min_traj_len,
    traj_gap_hours,
):
    print(f"Processing {city} with {filter_method} filtering...")
    events = read_data(source_file, city)
    original_events_count = len(events)
    print(f"  Raw events: {original_events_count}")

    # 1 & 2: Filter POIs and users
    if filter_method == "iterative":
        while True:
            poi_counts = Counter(e["poi"] for e in events)
            user_counts = Counter(e["user"] for e in events)

            filtered = [
                e
                for e in events
                if poi_counts[e["poi"]] >= min_poi
                and user_counts[e["user"]] >= min_user
            ]
            if len(filtered) == len(events):
                break
            events = filtered
    elif filter_method == "one-pass":
        poi_counts = Counter(e["poi"] for e in events)
        user_counts = Counter(e["user"] for e in events)
        events = [
            e
            for e in events
            if poi_counts[e["poi"]] >= min_poi and user_counts[e["user"]] >= min_user
        ]
    else:
        raise ValueError("Invalid filter method")

    post_filter_events_count = len(events)
    print(f"  Events after filtering: {post_filter_events_count}")

    # 3: Divide into trajectories
    user_events = defaultdict(list)
    for e in events:
        user_events[e["user"]].append(e)

    for u in user_events:
        user_events[u].sort(key=lambda x: x["time"])

    trajectories = defaultdict(list)
    gap_seconds = int(traj_gap_hours * 3600)
    for u, evs in user_events.items():
        if not evs:
            continue
        curr_traj = [evs[0]]
        for e in evs[1:]:
            if e["time"] - curr_traj[-1]["time"] > gap_seconds:
                if len(curr_traj) >= min_traj_len:
                    trajectories[u].append(curr_traj)
                curr_traj = [e]
            else:
                curr_traj.append(e)
        if len(curr_traj) >= min_traj_len:
            trajectories[u].append(curr_traj)

    valid_users_count = len(trajectories)
    print(f"  Users with valid trajectories: {valid_users_count}")

    # 4: Per-user chronological trajectory-level split 80/10/10
    #    Each trajectory stays intact in exactly one split.
    train_data, val_data, test_data = [], [], []
    train_pois, train_users = set(), set()

    for u, trajs in trajectories.items():
        n_trajs = len(trajs)
        if n_trajs == 0:
            continue

        # Assign traj_ids (chronological order preserved from step 3)
        labeled_trajs = []
        for t_idx, traj in enumerate(trajs):
            labeled = [dict(e, traj_id=f"{u}_{t_idx}") for e in traj]
            labeled_trajs.append(labeled)

        # Split at trajectory level — never break a trajectory across splits
        train_end = max(int(math.floor(n_trajs * 0.8)), 1)  # at least 1 train traj
        val_end = int(math.floor(n_trajs * 0.9))
        val_end = max(val_end, train_end)  # ensure val_end >= train_end

        for traj in labeled_trajs[:train_end]:
            train_data.extend(traj)
        for traj in labeled_trajs[train_end:val_end]:
            val_data.extend(traj)
        for traj in labeled_trajs[val_end:]:
            test_data.extend(traj)

        # Build train vocabulary
        train_users.add(u)
        for traj in labeled_trajs[:train_end]:
            for e in traj:
                train_pois.add(e["poi"])

    # 5: Entity filtering for val/test — drop entire trajectories with unknown entities.
    #    Dropping individual checkins would break trajectory continuity and could
    #    create fragments shorter than min_traj_len.
    def filter_eval_split(split_data, split_name):
        traj_groups = defaultdict(list)
        for e in split_data:
            traj_groups[e["traj_id"]].append(e)

        result = []
        n_dropped = 0
        for t_id, t_evs in sorted(traj_groups.items()):
            has_unknown = any(
                e["user"] not in train_users or e["poi"] not in train_pois
                for e in t_evs
            )
            if has_unknown:
                n_dropped += 1
                continue
            result.extend(t_evs)

        if n_dropped:
            print(
                f"  {split_name}: dropped {n_dropped} trajectories with entities not in train"
            )
        return result

    val_data = filter_eval_split(val_data, "Val")
    test_data = filter_eval_split(test_data, "Test")

    # 6: Only evaluate the last check-in record of each trajectory in val/test.
    def mark_last_in_traj(split_data):
        traj_groups = defaultdict(list)
        for e in split_data:
            traj_groups[e["traj_id"]].append(e)

        marked = []
        for t_id, t_evs in traj_groups.items():
            t_evs.sort(key=lambda x: x["time"])
            for i, e in enumerate(t_evs):
                e["is_eval_target"] = i == len(t_evs) - 1
                marked.append(e)
        return marked

    val_data = mark_last_in_traj(val_data)
    test_data = mark_last_in_traj(test_data)

    # 7: Post-split statistics and validation
    def get_split_stats(data):
        if not data:
            return {
                "checkins": 0,
                "trajs": 0,
                "users": 0,
                "pois": 0,
                "cats": 0,
                "min_traj_len": 0,
                "targets": 0,
            }
        traj_counts = Counter(e["traj_id"] for e in data)
        return {
            "checkins": len(data),
            "trajs": len(traj_counts),
            "users": len(set(e["user"] for e in data)),
            "pois": len(set(e["poi"] for e in data)),
            "cats": len(set(e["cat_id"] for e in data)),
            "min_traj_len": min(traj_counts.values()),
            "targets": sum(1 for e in data if e.get("is_eval_target", False)),
        }

    tr = get_split_stats(train_data)
    va = get_split_stats(val_data)
    te = get_split_stats(test_data)

    header = f"  {'':8s} {'Checkins':>10s} {'Trajs':>8s} {'Users':>8s} {'POIs':>8s} {'Cats':>8s} {'MinLen':>8s} {'Targets':>8s}"
    print(f"\n{header}")
    print(
        f"  {'Train':8s} {tr['checkins']:>10,d} {tr['trajs']:>8,d} {tr['users']:>8,d} {tr['pois']:>8,d} {tr['cats']:>8,d} {tr['min_traj_len']:>8d} {'—':>8s}"
    )
    if va["trajs"] > 0:
        print(
            f"  {'Val':8s} {va['checkins']:>10,d} {va['trajs']:>8,d} {va['users']:>8,d} {va['pois']:>8,d} {va['cats']:>8,d} {va['min_traj_len']:>8d} {va['targets']:>8,d}"
        )
    if te["trajs"] > 0:
        print(
            f"  {'Test':8s} {te['checkins']:>10,d} {te['trajs']:>8,d} {te['users']:>8,d} {te['pois']:>8,d} {te['cats']:>8,d} {te['min_traj_len']:>8d} {te['targets']:>8,d}"
        )

    # Validation checks
    issues = []
    for name, data in [("Train", train_data), ("Val", val_data), ("Test", test_data)]:
        if not data:
            continue
        tc = Counter(e["traj_id"] for e in data)
        short = [t for t, c in tc.items() if c < min_traj_len]
        if short:
            issues.append(
                f"{name}: {len(short)} trajectories with < {min_traj_len} checkins"
            )
    for name, data in [("Val", val_data), ("Test", test_data)]:
        unk_u = sum(1 for e in data if e["user"] not in train_users)
        unk_p = sum(1 for e in data if e["poi"] not in train_pois)
        if unk_u:
            issues.append(f"{name}: {unk_u} checkins with unknown users")
        if unk_p:
            issues.append(f"{name}: {unk_p} checkins with unknown POIs")
    # Check disjoint traj_ids
    train_tids = set(e["traj_id"] for e in train_data)
    val_tids = set(e["traj_id"] for e in val_data)
    test_tids = set(e["traj_id"] for e in test_data)
    for na, sa, nb, sb in [
        ("Train", train_tids, "Val", val_tids),
        ("Train", train_tids, "Test", test_tids),
        ("Val", val_tids, "Test", test_tids),
    ]:
        overlap = sa & sb
        if overlap:
            issues.append(f"{na} & {nb}: {len(overlap)} overlapping traj_ids")

    if issues:
        print(f"\n  Validation FAILED:")
        for issue in issues:
            print(f"    - {issue}")
    else:
        print(
            f"\n  Validation PASSED: all trajs >= {min_traj_len} checkins, "
            f"all val/test entities in train, disjoint traj_ids"
        )
    print()

    os.makedirs(os.path.join(out_dir, city), exist_ok=True)

    def save_jsonl(data, path):
        with open(path, "w") as f:
            for d in data:
                f.write(json.dumps(d) + "\n")

    save_jsonl(train_data, os.path.join(out_dir, city, "train.jsonl"))
    save_jsonl(val_data, os.path.join(out_dir, city, "val.jsonl"))
    save_jsonl(test_data, os.path.join(out_dir, city, "test.jsonl"))

    return {
        "city": city,
        "original_events": original_events_count,
        "post_filter_events": post_filter_events_count,
        "valid_users": valid_users_count,
        "train_size": tr["checkins"],
        "val_size": va["checkins"],
        "test_size": te["checkins"],
        "train_trajs": tr["trajs"],
        "val_trajs": va["trajs"],
        "test_trajs": te["trajs"],
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
    parser.add_argument(
        "--filter-method",
        type=str,
        choices=["iterative", "one-pass"],
        default="iterative",
    )
    parser.add_argument("--min-poi", type=int, default=10)
    parser.add_argument("--min-user", type=int, default=10)
    parser.add_argument("--min-traj-len", type=int, default=2)
    parser.add_argument("--traj-gap-hours", type=float, default=24.0)

    args = parser.parse_args()

    dir_name = f"filter-{args.filter_method}_minpoi-{args.min_poi}_minuser-{args.min_user}_mintrajlen-{args.min_traj_len}_gap-{args.traj_gap_hours}h"
    script_dir = os.path.dirname(os.path.abspath(__file__))
    out_dir = os.path.join(script_dir, "preprocessed_data", dir_name)
    os.makedirs(out_dir, exist_ok=True)

    source_dir = os.path.join(script_dir, "source_data")
    datasets = [
        ("NYC", os.path.join(source_dir, "dataset_TSMC2014_NYC.txt")),
        ("TKY", os.path.join(source_dir, "dataset_TSMC2014_TKY.txt")),
        ("CA", os.path.join(source_dir, "dataset_gowalla_ca_ne.csv")),
    ]

    stats_list = []
    for city, path in datasets:
        stats = preprocess(
            city,
            path,
            out_dir,
            args.filter_method,
            args.min_poi,
            args.min_user,
            args.min_traj_len,
            args.traj_gap_hours,
        )
        stats_list.append(stats)

    create_datacard(out_dir, vars(args), stats_list)
    print(f"All done! Data and datacard saved in {out_dir}")
