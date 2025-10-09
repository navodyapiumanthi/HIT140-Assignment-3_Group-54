import pandas as pd

df = pd.read_csv("merged_dataset.csv")


#drop duplicate rows
print("Duplicates before drop:", df.duplicated(keep="first").sum())
df = df.drop_duplicates(keep="first").reset_index(drop=True)

##Drop columns
#time column to have one main time column

df = df.drop(columns=["time"], errors="ignore")
df = df.rename(columns={"start_time": "time"})
df["time_dt"] = pd.to_datetime(df["time"], errors="coerce", dayfirst=True)

#habit_raw
df = df.drop(columns=["habit_raw"], errors="ignore")


#Fix out of range dates / typos 
def flip_date(ts: pd.Timestamp) -> pd.Timestamp:
    """Flip day and month if month > 5"""
    if pd.isna(ts):
        return ts
    if ts.month <= 5 or ts.day >12:
        return ts
    try:
        return ts.replace(month=int(ts.day), day=int(ts.month))
    except ValueError:
        return ts

df["time_dt"] = df["time_dt"].apply(flip_date)

map_to_scheme = {12:0, 1:1, 2:2, 3:3, 4:4, 5:5,}
df["month"] = df["time_dt"].dt.month.map(map_to_scheme).astype("Int64")
df["season"] = df["month"].map({0:0, 1:0, 2:0, 3:1, 4:1, 5:1}).astype("Int64")

# Sorting
# Sort by season, month and then time
df["sort_dt"] = df["time_dt"]
df = df.sort_values(["season", "month", "sort_dt"], na_position="last").reset_index(drop=True)

df["time"] = df["time_dt"]
df_out = df.drop(columns=["time_dt", "sort_dt"], errors="ignore")

# save merged with datetime formatting
df_out.to_csv("merged_dataset_sorted.csv", index=False, date_format="%d/%m/%Y %H:%M:%S")
print("\nSaved: merged_dataset_sorted.csv")


#Spit by season
winter = df_out[df_out["season"] == 0].reset_index(drop=True)
spring = df_out[df_out["season"] == 1].reset_index(drop=True)

#save train and test data
winter.to_csv("winter_dataset.csv", index=False, date_format="%d/%m/%Y %H:%M:%S")
spring.to_csv("spring_dataset.csv", index=False, date_format="%d/%m/%Y %H:%M:%S")


a = pd.to_datetime(df.iloc[:, 0], errors="coerce", dayfirst=True)
ok = a.dropna().is_monotonic_increasing
viol = int(a.dropna().diff().lt(pd.Timedelta(0)).sum())
print(f"Column A chronological (non-decreasing): {ok} | out-of-order count: {viol}")
