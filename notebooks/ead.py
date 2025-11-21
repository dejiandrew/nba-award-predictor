import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Load the dataset
data = Path("data/player-statistics.csv")
df = pd.read_csv(data, low_memory=False)

# Clean column names

df.columns = df.columns.str.strip().str.lower()
#print(df.columns)
df['game_date'] = pd.to_datetime(df['gamedate'], utc=True, errors='coerce').dt.tz_convert(None)
df = df.dropna(subset=['game_date']).copy()

# Add season column
df["season"] = np.where(
    df["game_date"].dt.month >= 7,
    df["game_date"].dt.year.astype(str) + "-" + (df["game_date"].dt.year + 1).astype(str),
    (df["game_date"].dt.year - 1).astype(str) + "-" + df["game_date"].dt.year.astype(str),
)

# Add ISO week
df["week"] = df["game_date"].dt.isocalendar().week.astype(int)

# Aggregate to weekly level
weekly = (
    df.groupby(["player_id", "season", "week"], as_index=False)
      .agg(
          gms=("gameid", "nunique"),
          pts=("points", "sum"),
          ast=("assists", "sum"),
          blk=("blocks", "sum"),
          stl=("steals", "sum"),
          pm=("plusminuspoints", "sum"),
          minutes=("numminutes", "sum")
      )
)

#print("Weekly dataset shape:", weekly.shape)
#print(weekly.head())

# Graphing setup
# Weekly average points
plt.figure(figsize=(12, 6))
weekly_mean = weekly.groupby(["season", "week"], as_index=False)["pts"].mean()
plt.plot(weekly_mean["week"], weekly_mean["pts"])
plt.title("League Average Weekly Points")
plt.xlabel("Week")
plt.ylabel("Mean Points")
plt.show()

#Weekly points distribution
plt.figure(figsize=(12, 6))
sns.histplot(weekly["pts"], kde=True, bins=40)
plt.title("Distribution of Weekly Total Points (Player-week)")
plt.xlabel("Weekly Points")
plt.show()

# Top 20 scorers for a sample week
'''sample_week = 20
sample_season = "2022-2023"

sample = weekly[(weekly["season"] == sample_season) & (weekly["week"] == sample_week)]
'''

#print(weekly["season"].unique()[:20])
#print(weekly["week"].unique()[:20])
#print(top20.head())
#print(top20.shape)

latest_season = weekly["season"].max()
latest_week = weekly[weekly["season"] == latest_season]["week"].max()

#print("Latest season:", latest_season)
#print("Latest week:", latest_week)

top_week = weekly[(weekly["season"] == latest_season) & (weekly["week"] == latest_week)]
top20 = top_week.nlargest(20, "pts")

print(top_week.head())
print(top_week.shape)

plt.figure(figsize=(12, 6))
sns.barplot(y=top20["player_id"], x=top20["pts"])
plt.title(f"Top 20 Scorers - Week {latest_week} of {latest_season}")
plt.xlabel("Points")
plt.ylabel("Player ID")
plt.show()

# Weekly plus-minus distribution
plt.figure(figsize=(12, 6))
sns.boxplot(x=weekly["pm"])
plt.title("Distribution of Weekly Plus-Minus")
plt.xlabel("Plus-Minus")
plt.show()

# Weekly Z-score breakout heatmap
weekly = weekly.sort_values(["player_id", "season", "week"])

grp = weekly.groupby(["player_id", "season"])["pts"]

weekly["pts_mean_prev"] = grp.expanding().mean().shift().reset_index(level=[0,1], drop=True)
weekly["pts_std_prev"]  = grp.expanding().std().shift().reset_index(level=[0,1], drop=True)


weekly["z_pts"] = (weekly["pts"] - weekly["pts_mean_prev"]) / weekly["pts_std_prev"]
weekly["z_pts"] = weekly["z_pts"].replace([np.inf, -np.inf], 0).fillna(0)

# Player for visualization
player_example = top20.iloc[0]["player_id"]
sub = weekly[weekly["player_id"] == player_example]

plt.figure(figsize=(14, 3))
sns.heatmap(
    sub[["z_pts"]].T,
    cmap="coolwarm",
    cbar=True
)
plt.title(f"Breakout Week Heatmap - Player {player_example}")
plt.xlabel("Week Index")
plt.ylabel("Z-score")
plt.show()
