import pandas as pd
import joblib
import os
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from backend.app.model import extract_features, model

# 📁 Dosya yolları
DATA_FILE = "FinalProject_LogAnalyzer/data/raw/combined_log_data.csv"
LABEL_FILE = "FinalProject_LogAnalyzer/data/raw/anomaly_label.csv"

# 1. Verileri oku
df_logs = pd.read_csv(DATA_FILE)
df_labels = pd.read_csv(LABEL_FILE)

# 2. Trace ID'leri eşleştir
df_labels['BlockId'] = df_labels['BlockId'].astype(str)
df_logs['trace_id'] = df_logs['trace_id'].astype(str)
label_map = dict(zip(df_labels['BlockId'], df_labels['Label']))

# 3. Her satırı tek tek işle
true_labels = []
pred_labels = []

for i, row in df_logs.iterrows():
    # Gerçek etiket
    tid = row['trace_id']
    short_id = tid.split("_")[-1]
    true_label = label_map.get(short_id, "Normal")
    true_labels.append(1 if true_label == "Anomaly" else 0)

    # Özellik çıkarımı
    log_input = {
        "EventTemplate": row.get("EventTemplate", ""),
        "timestamp": row.get("timestamp", ""),
        "log_level": row.get("log_level", ""),
        "component": row.get("component", "")
    }
    X = extract_features(log_input)

    # Model tahmini
    pred = model.predict(X)[0]
    pred_labels.append(1 if pred == -1 else 0)  # -1 = anomaly

# 4. Metrik hesapla
acc = accuracy_score(true_labels, pred_labels)
prec = precision_score(true_labels, pred_labels, zero_division=0)
rec = recall_score(true_labels, pred_labels, zero_division=0)
f1 = f1_score(true_labels, pred_labels, zero_division=0)

# 5. Yazdır
print("\n📊 MODEL PERFORMANSI")
print(f"✅ Accuracy : {acc*100:.2f}%")
print(f"✅ Precision: {prec*100:.2f}%")
print(f"✅ Recall   : {rec*100:.2f}%")
print(f"✅ F1-Score : {f1*100:.2f}%")
