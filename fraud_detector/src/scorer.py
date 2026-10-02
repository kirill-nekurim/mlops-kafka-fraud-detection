import json
import logging

import pandas as pd
from catboost import CatBoostClassifier

logger = logging.getLogger(__name__)

# загружаем модель один раз при старте
model = CatBoostClassifier()
model.load_model('./models/model.cbm')

# порог подобран на валидации при обучении (см. train/train_model.py)
with open('./models/threshold.json') as f:
    THRESHOLD = json.load(f)['threshold']

logger.info('Model loaded, threshold = %.3f', THRESHOLD)


def make_pred(df):
    scores = model.predict_proba(df)[:, 1]
    return pd.DataFrame({
        'score': scores,
        'fraud_flag': (scores > THRESHOLD).astype(int),
    })
