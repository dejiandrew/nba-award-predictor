import pandas as pd
import joblib
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import (
    roc_curve,
    auc,
    precision_recall_curve,
    confusion_matrix,
    classification_report
)
from sklearn.model_selection import train_test_split

# Load Data
print("Loading data...")
df = pd.read_csv("data/features-overall-weekly.csv")

label_col = "won_player_of_the_week"

drop_cols = [
    "player_id", "full_name", "team",
    "pow_player_id", "player_of_the_week",
    "week_start", "conference", "pow_conference",
    label_col
]

X = df.drop(columns=drop_cols, errors="ignore")
y = df[label_col]

# Split same way as training
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# Load Trained Model
print("Loading model...")
model = joblib.load("modeling/best_pow_log_reg.pkl")

# Predict
y_pred = model.predict(X_test)
y_proba = model.predict_proba(X_test)[:, 1]

print("Classification Report:")
print(classification_report(y_test, y_pred))

# ROC Curve
fpr, tpr, _ = roc_curve(y_test, y_proba)
roc_auc = auc(fpr, tpr)

plt.figure(figsize=(7, 6))
plt.plot(fpr, tpr, label=f"AUC = {roc_auc:.4f}")
plt.plot([0, 1], [0, 1], linestyle="--", color="gray")
plt.xlabel("False Positive Rate")
plt.ylabel("True Positive Rate")
plt.title("ROC Curve - POW Logistic Regression")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()

#Precision-Recall Curve
precision, recall, _ = precision_recall_curve(y_test, y_proba)

plt.figure(figsize=(7, 6))
plt.plot(recall, precision)
plt.xlabel("Recall")
plt.ylabel("Precision")
plt.title("Precision-Recall Curve - POW Logistic Regression")
plt.grid(True)
plt.tight_layout()
plt.show()

# Confusion Matrix (Heatmap)
cm = confusion_matrix(y_test, y_pred)

plt.figure(figsize=(6, 5))
sns.heatmap(cm, annot=True, fmt="d", cmap="Blues")
plt.xlabel("Predicted Label")
plt.ylabel("True Label")
plt.title("Confusion Matrix - POW Logistic Regression")
plt.tight_layout()
plt.show()

# Probability Distribution Plot
plt.figure(figsize=(8, 6))
sns.histplot(y_proba[y_test == 0], color="blue", label="Negative (0)", kde=True, bins=30, stat="density")
sns.histplot(y_proba[y_test == 1], color="red", label="Positive (1)", kde=True, bins=30, stat="density")

plt.xlabel("Predicted Probability")
plt.ylabel("Density")
plt.title("Probability Distribution of Predictions - POW Logistic Regression")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()

