import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Load the dataset
data = Path("data/features-overall-weekly.csv")
df = pd.read_csv(data, low_memory=False)

# Clean column names

df.columns = df.columns.str.strip().str.lower()
#print(df.columns)

# Graphing setup
# Weekly average points
plt.figure(figsize=(12, 6))
weekly_mean = (
    df.groupby(["season","week"])["points"]
      .mean()
      .reset_index()
)

plt.plot(weekly_mean["week"], weekly_mean["points"])
plt.title("League Average Weekly Points")
plt.xlabel("Week")
plt.ylabel("Mean Points")
plt.show()

#Weekly points distribution
plt.figure(figsize=(12, 6))
sns.histplot(df["points"])
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


#print("Latest season:", latest_season)
#print("Latest week:", latest_week)
latest_season = df["season"].max()
latest_week = df[df["season"] == latest_season]["week"].max()
top_week = df[(df["season"] == latest_season) & (df["week"] == latest_week)]
top20 = top_week.nlargest(20, "points")

print(top_week.head())
print(top_week.shape)

plt.figure(figsize=(12, 6))
sns.barplot(data=top20, x="points", y="player_id")
plt.title(f"Top 20 Scorers - Week {latest_week} of {latest_season}")
plt.xlabel("Points")
plt.ylabel("Player ID")
plt.show()

# Weekly plus-minus distribution
plt.figure(figsize=(12, 6))
sns.boxplot(x=df["plusminuspoints"])
plt.title("Distribution of Weekly Plus-Minus")
plt.xlabel("Plus-Minus")
plt.show()

# Player for visualization
player_example = top20.iloc[0]["player_id"]
sub = df[df["player_id"] == player_example].sort_values(["season","week"])
plt.figure(figsize=(12, 6))
sns.heatmap([sub["breakout_score"].values], cmap="coolwarm")
plt.title(f"Breakout Week Heatmap - Player {player_example}")
plt.xlabel("Week Index")
plt.ylabel("Z-score")
plt.show()


# Top 20 breakout scores
plt.figure(figsize=(12, 6))
top_breakout = df.nlargest(20, "breakout_score")
sns.barplot(data=top_breakout, x="breakout_score", y="player_id", palette="viridis")
plt.title(f"Top 20 Breakout Scores - Week {latest_week} of {latest_season}")
plt.xlabel("Breakout Score")
plt.ylabel("Player ID")
plt.show()

# Correlation heatmap of numeric features
plt.figure(figsize=(12, 6))
numeric_df = df.select_dtypes(include=[np.number])
corr = numeric_df.corr()
sns.heatmap(corr, cmap="coolwarm", center=0)
plt.title("Feature Correlation Heatmap")
plt.show()

# Week-over-week improvement distribution
df_improvement = df.sort_values(["player_id", "season", "week"])
df_improvement["pts_change"] = df_improvement.groupby("player_id")["points"].diff()
plt.figure(figsize=(12, 6))
sns.histplot(df_improvement["pts_change"].dropna(), bins=40, kde=True)
plt.title("Distribution of Week-over-Week Points Change")
plt.xlabel("Points Change (This week - Last week)")
plt.show()

pow_col = None

# Different name in datasets
if "pow" in df.columns:
    pow_col = "pow"
elif "won_player_of_the_week" in df.columns:
    pow_col = "won_player_of_the_week"
if pow_col is None:
    print("No POW column found in dataset.")
else:
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=df, x=pow_col, y="points")
    plt.title("Points Distribution: POW vs Non-POW")
    plt.xlabel("POW (1 = Player of the Week)")
    plt.ylabel("Weekly Points")
    plt.show()