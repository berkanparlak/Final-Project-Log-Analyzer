import os
import joblib
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import OneHotEncoder

MODEL_DIR = os.path.join(os.path.dirname(__file__), "models")
MODEL_PATH = os.path.join(MODEL_DIR, "isolation_forest_model.pkl")
ENCODER_PATH = os.path.join(MODEL_DIR, "encoder.pkl")
TFIDF_PATH = os.path.join(MODEL_DIR, "tfidf_vectorizer.pkl")

model = joblib.load(MODEL_PATH)
encoder = joblib.load(ENCODER_PATH)
tfidf = joblib.load(TFIDF_PATH)

def extract_features(log):
    df = pd.DataFrame([log])

    df["template_length"] = df["EventTemplate"].fillna("").apply(len)

    def extract_hour(ts):
        try:
            return int(str(ts).split()[1].split(":")[0])
        except:
            return -1

    df["hour"] = df["timestamp"].apply(extract_hour)
    df["log_level"] = df["log_level"].fillna("UNKNOWN").str.upper()
    df["component"] = df["component"].fillna("unknown").str.lower()

    tfidf_matrix = tfidf.transform(df["EventTemplate"].fillna(""))
    tfidf_df = pd.DataFrame(tfidf_matrix.toarray(), columns=[f"tfidf_{i}" for i in range(tfidf_matrix.shape[1])])

    cat_features = encoder.transform(df[["log_level", "component"]])
    cat_df = pd.DataFrame(cat_features, columns=encoder.get_feature_names_out())

    final_df = pd.concat([df[["template_length", "hour"]], cat_df, tfidf_df], axis=1)
    return final_df

def detect_anomaly_from_log(log_dict):
    X = extract_features(log_dict)
    pred = model.predict(X)[0]
    score = model.decision_function(X)[0]
    return pred == -1, float(score)

def get_recommendation(score):
    if score < 0.05:
        return "High anomaly score  investigate urgently"
    elif score < 0.07:
        return "Moderate anomaly  review system logs"
    elif score < 0.09:
        return "Slight anomaly monitor system"
    else:
        return "Normal activity"
