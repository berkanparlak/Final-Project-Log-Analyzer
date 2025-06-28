import os
import pandas as pd

TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), "templates")

_loaded_templates = {}

def load_template(log_type):
    fname = f"{log_type}_templates_with_labels.csv"
    fpath = os.path.join(TEMPLATE_PATH, fname)
    if not os.path.exists(fpath):
        return {}

    df = pd.read_csv(fpath)
    mapping = {}
    for _, row in df.iterrows():
        mapping[row['EventId']] = {
            'type': row['type'],
            'reason': row['rec']
        }
    return mapping

def get_event_info(log_type, event_id):
    if log_type not in _loaded_templates:
        _loaded_templates[log_type] = load_template(log_type)
    return _loaded_templates[log_type].get(event_id, {'type': 'Unknown', 'reason': 'No info available'})
