import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
import pickle
import os

# Eğitim için veri dosyaları
data_paths = [
    "D:/coding/FinalProject_LogAnalyzer/data/raw/Spark_2k.log_structured.csv",
    "D:/coding/FinalProject_LogAnalyzer/data/raw/BGL_2k.log_structured.csv",
    "D:/coding/FinalProject_LogAnalyzer/data/raw/OpenStack_2k.log_structured.csv",
    "D:/coding/FinalProject_LogAnalyzer/data/raw/Android_2k.log_structured.csv",
    "D:/coding/FinalProject_LogAnalyzer/data/raw/Zookeeper_2k.log_structured.csv",
]

event_values = []
for path in data_paths:
    try:
        df = pd.read_csv(path)
        events = df["EventId"].dropna().astype(str)
        for e in events:
            try:
                val = float(e.strip("E"))
                event_values.append(val)
            except:
                continue
    except Exception as e:
        print(f"Dosya okunamadı: {path} -> {e}")

# Liste numpy dizisine çevrilir
X = np.array(event_values).reshape(-1, 1)

# Model tanımlanır ve eğitilir
model = IsolationForest(
    n_estimators=100,
    contamination=0.05,  # anomali oranı
    random_state=42
)
model.fit(X)

# Kaydetme klasörü
model_path = os.path.join("backend", "app", "model.pkl")
os.makedirs(os.path.dirname(model_path), exist_ok=True)

# Model dosyası kaydedilir
with open(model_path, "wb") as f:
    pickle.dump(model, f)

print("\u2705 Model başarıyla eğitildi ve kaydedildi: backend/app/model.pkl")
