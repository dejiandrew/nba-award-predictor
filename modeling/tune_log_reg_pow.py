import pandas as pd
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.metrics import roc_auc_score, classification_report
import joblib

df = pd.read_csv("data/features-overall-weekly.csv")
#print(df.columns.tolist())
label_col = "won_player_of_the_week"
y = df[label_col]

drop_cols = [
    "player_id", "full_name", "team",
    "pow_player_id", "player_of_the_week",
    "week_start", "conference", "pow_conference",
    label_col
]

X = df.drop(columns=drop_cols, errors="ignore")

#print("X shape:", X.shape)
#print("y value counts:", y.value_counts())
#print(X.select_dtypes(include=['object']).columns.tolist())

# Define pipeline
pipe = Pipeline([
    ("scaler", StandardScaler()),
    ("pca", PCA(n_components=30)),
    ("model", LogisticRegression(
        max_iter=1000,
        class_weight="balanced",
        n_jobs=-1
    ))
])

# Split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# Test fit
print("Fitting pipeline...waitttt")
pipe.fit(X_train, y_train)
print("Pipeline fit succeeded!")



'''
# Testing smaller grid
param_grid_small = {
    "pca__n_components": [20, 30],
    "model__C": [0.1, 1],
}
grid = GridSearchCV(
    estimator=pipe,
    param_grid=param_grid_small,
    cv=3,
    scoring="roc_auc",
    n_jobs=-1,
    verbose=2
)
grid.fit(X_train, y_train)
print("GridSearch small test completed!")
print("Best params:", grid.best_params_)
print("Best ROC AUC (CV):", grid.best_score_)
'''


print("Running GridSearchCV...")
param_grid_full = {
    "pca__n_components": [10, 20, 30, 40, 50, 60],
    "model__C": [0.01, 0.1, 1, 10, 100],
    "model__max_iter": [500, 1000],
    "model__solver": ["lbfgs", "liblinear"],
    "model__class_weight": ["balanced"]
}

grid_full = GridSearchCV(
    estimator=pipe,
    param_grid=param_grid_full,
    cv=5,
    scoring="roc_auc",
    n_jobs=-1,
    verbose=2
)

grid_full.fit(X_train, y_train)

print("FULL GridSearch Completed!")
print("Best params:", grid_full.best_params_)
print("Best CV ROC AUC:", grid_full.best_score_)

# Evaluate on testing data
y_pred = grid_full.predict(X_test)
y_proba = grid_full.predict_proba(X_test)[:, 1]

print("Test Set Performance:")
print(classification_report(y_test, y_pred))
print("Test ROC AUC:", roc_auc_score(y_test, y_proba))

# Save the best model
joblib.dump(grid_full.best_estimator_, "modeling/best_pow_log_reg.pkl")
print("Saved best model to: modeling/best_pow_log_reg.pkl")