import pandas as pd
import os

raw_path = "D:/coding/FinalProject_LogAnalyzer/data/raw"
template_path = "D:/coding/FinalProject_LogAnalyzer/backend/app/templates"

log_sources = ["android", "bgl", "spark", "openstack", "zookeeper"]
all_data = []

for source in log_sources:
    print(f"🔍 İşleniyor: {source}")
    
    log_file = f"{raw_path}/{source.capitalize()}_2k.log_structured.csv"
    label_file = f"{template_path}/{source}_templates_with_labels.csv"

    if not os.path.exists(log_file):
        print(f"Log dosyası bulunamadı: {log_file}")
        continue
    if not os.path.exists(label_file):
        print(f"Template dosyası bulunamadı: {label_file}")
        continue

    logs_df = pd.read_csv(log_file)
    labels_df = pd.read_csv(label_file)

    # Timestamp oluştur
    logs_df["timestamp"] = logs_df["Date"].astype(str) + " " + logs_df["Time"].astype(str)

    # Gerekli sütunları seç
    logs_df = logs_df[["LineId", "timestamp", "EventId", "EventTemplate", "Level", "Component"]]

    # Merge işlemi
    merged = logs_df.merge(labels_df, on=["EventId", "EventTemplate"], how="left")

    # source bilgisini ekle
    merged["source"] = source

    # Kolonları yeniden adlandır
    merged = merged.rename(columns={
        "LineId": "trace_id",
        "Level": "log_level",
        "Component": "component"
    })

    # Sıralı hale getir
    merged = merged[["trace_id", "timestamp", "EventId", "EventTemplate", "type", "rec", "log_level", "component", "source"]]
    all_data.append(merged)

# Sonuçları birleştir
if all_data:
    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df.dropna(subset=["EventTemplate"], inplace=True)

    output_path = os.path.join(raw_path, "combined_log_data.csv")
    combined_df.to_csv(output_path, index=False)
    print(f"combined_log_data.csv başarıyla kaydedildi: {output_path}")
else:
    print("Hiçbir veri işlenemedi.")
