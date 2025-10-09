import pandas as pd

#file loading
df1 = pd.read_csv("Dataset1_clean.csv")
df2 = pd.read_csv("Dataset2_clean.csv")


df1["dataset"] = "dataset1"
df2["dataset"] = "dataset2"

#merging
combined = pd.concat([df1, df2], ignore_index=True)

#sort by month
combined = combined.sort_values(by="month").reset_index(drop=True)

#add season to missing rows
def assign_season(m):
    if m in [0, 1, 2]:
        return 0
    elif m in [3, 4, 5]:
        return 1
    else:
        return None

if "season" in combined.columns:
    combined["season"] = combined["month"].apply(assign_season)
else:
    combined["season"] = combined["season"].where(
        combined["season"].notna(),
        combined["month"].apply(assign_season)
    )

#season column to be integer    
combined["season"] = combined["season"].astype("Int64")

#drop month 6 as season is not defined
before = len(combined)
combined = combined[combined["month"] != 6].reset_index(drop=True)
after = len(combined)


# save combined data
combined.to_csv("merged_dataset.csv", index=False)

