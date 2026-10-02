"""
Обучение легкой модели CatBoost (запускается локально один раз, в контейнере только inference).

    pip install catboost==1.2.8 pandas==2.2.3 scikit-learn
    python train/train_model.py

Сохраняет fraud_detector/models/model.cbm и threshold.json
"""
import json
import os
import sys

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier
from sklearn.metrics import f1_score, roc_auc_score
from sklearn.model_selection import train_test_split

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(ROOT, 'fraud_detector', 'src'))
from preprocessing import CAT_FEATURES, run_preproc

train = pd.read_csv(os.path.join(ROOT, 'data', 'train.csv'))
X = run_preproc(train)
y = train['target']

X_tr, X_val, y_tr, y_val = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)

model = CatBoostClassifier(
    iterations=500,
    depth=6,
    learning_rate=0.05,
    cat_features=CAT_FEATURES,
    auto_class_weights='Balanced',
    random_seed=42,
    verbose=100,
)
model.fit(X_tr, y_tr, eval_set=(X_val, y_val))

# подбираем порог по f1 на валидации
val_scores = model.predict_proba(X_val)[:, 1]
thresholds = np.arange(0.05, 1.0, 0.01)
f1s = [f1_score(y_val, val_scores > t) for t in thresholds]
best_th = float(thresholds[int(np.argmax(f1s))])
print(f'ROC-AUC: {roc_auc_score(y_val, val_scores):.4f}, best F1: {max(f1s):.4f} at threshold {best_th:.2f}')

model.save_model(os.path.join(ROOT, 'fraud_detector', 'models', 'model.cbm'))
with open(os.path.join(ROOT, 'fraud_detector', 'models', 'threshold.json'), 'w') as f:
    json.dump({'threshold': round(best_th, 2)}, f)
print('Model saved')
