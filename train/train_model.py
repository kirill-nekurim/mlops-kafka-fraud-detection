"""
Обучение стартовой модели (версия initial), которая зашита в образ fraud_detector.

Обучение идет тем же кодом, что и в DAG retrain_fraud_model, на стартовом окне:
январь–март 2019 — train, апрель 2019 — test. Данных из более поздних месяцев модель
не видит, поэтому ее честно можно сравнивать с моделями, которые обучит DAG.

    pip install catboost==1.2.8 pandas scikit-learn pyarrow python-dateutil
    python train/train_model.py

Сохраняет fraud_detector/models/model.cbm и model.json (порог и метрики).
"""
import json
import os
import sys

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, 'fraud_detector', 'src'))
from pipeline.retrain.clean_window import clean_frame  # noqa: E402
from pipeline.retrain.train_challenger import fit_model  # noqa: E402
from preprocessing import run_preproc  # noqa: E402

TRAIN_MONTHS = ['2019-01', '2019-02', '2019-03']
TEST_MONTH = '2019-04'

df = pd.read_csv(os.path.join(ROOT, 'data', 'train.csv'))
df, stats = clean_frame(df)
print('Очистка:', stats)
months = df['transaction_time'].dt.to_period('M').astype(str)
train_raw = df[months.isin(TRAIN_MONTHS)].sort_values('transaction_time')
test_raw = df[months == TEST_MONTH]

train = run_preproc(train_raw).assign(target=train_raw['target'].to_numpy())
model, threshold, val_metrics = fit_model(train)

from sklearn.metrics import average_precision_score, roc_auc_score  # noqa: E402
proba = model.predict_proba(run_preproc(test_raw))[:, 1]
test_metrics = {
    'pr_auc': float(average_precision_score(test_raw['target'], proba)),
    'roc_auc': float(roc_auc_score(test_raw['target'], proba)),
}
print('Порог:', threshold, 'валидация:', val_metrics, 'test', TEST_MONTH, test_metrics)

out = os.path.join(ROOT, 'fraud_detector', 'models')
model.save_model(os.path.join(out, 'model.cbm'))
with open(os.path.join(out, 'model.json'), 'w') as f:
    json.dump({'version': 'initial', 'threshold': threshold, 'train_months': TRAIN_MONTHS,
               'test_month': TEST_MONTH, 'train_rows': int(len(train)), **val_metrics,
               'test_metrics': test_metrics}, f, ensure_ascii=False, indent=2)
print('Model saved')
