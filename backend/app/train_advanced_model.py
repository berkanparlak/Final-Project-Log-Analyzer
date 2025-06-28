import pandas as pd
import numpy as np
import os
import joblib
import re

from sklearn.ensemble import IsolationForest
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import OneHotEncoder

# Veri yolu
DATA_PATH = "D:/coding/FinalProject_LogAnalyzer/data/raw/combined_log_data.csv"
MODEL_DIR = "D:/coding/FinalProject_LogAnalyzer/backend/app/models"

# Veriyi oku
df = pd.read_csv(DATA_PATH)

# Özellik çıkarımı
def extract_features(df):
    features = pd.DataFrame()

    # 1. Template uzunluğu
    features["template_length"] = df["EventTemplate"].fillna("").apply(len)

    # 2. Saat bilgisi
    def extract_hour(ts):
        try:
            return int(str(ts).split()[1].split(":")[0])
        except:
            return -1
    features["hour"] = df["timestamp"].apply(extract_hour)

    # 3. Log seviyesi
    features["log_level"] = df["log_level"].fillna("UNKNOWN").str.upper()

    # 4. Component bilgisi
    features["component"] = df["component"].fillna("unknown").str.lower()

    # 5. TF-IDF vectorizer
    tfidf = TfidfVectorizer(max_features=50)
    tfidf_matrix = tfidf.fit_transform(df["EventTemplate"].fillna(""))
    tfidf_df = pd.DataFrame(tfidf_matrix.toarray(), columns=[f"tfidf_{i}" for i in range(tfidf_matrix.shape[1])])


    # 6. One-hot encode: log_level ve component
    enc = OneHotEncoder(sparse_output=False, handle_unknown="ignore")
    cat_features = enc.fit_transform(features[["log_level", "component"]])
    cat_df = pd.DataFrame(cat_features, columns=enc.get_feature_names_out(["log_level", "component"]))

    # Tüm özellikleri birleştir
    final_df = pd.concat([features[["template_length", "hour"]], cat_df, tfidf_df], axis=1)

    # Model dosyaları için dizin oluştur
    os.makedirs(MODEL_DIR, exist_ok=True)
    joblib.dump(enc, os.path.join(MODEL_DIR, "encoder.pkl"))
    joblib.dump(tfidf, os.path.join(MODEL_DIR, "tfidf_vectorizer.pkl"))

    return final_df

# Özellikleri çıkar
X = extract_features(df)

#Modeli eğit
model = IsolationForest(n_estimators=100, contamination=0.01, random_state=42)

model.fit(X)

#Modeli kaydet
joblib.dump(model, os.path.join(MODEL_DIR, "isolation_forest_model.pkl"))

print("Model eğitildi ve kaydedildi.")

