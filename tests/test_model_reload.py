"""Проверяем, что сломанная новая модель не выключает работающий скоринг."""

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

src = Path('/app/src')
if not src.exists():
    src = Path(__file__).resolve().parents[1] / 'fraud_detector' / 'src'
sys.path.insert(0, str(src))
import scorer
from metrics import ACTIVE, LOAD_ERRORS, RELOADS


class FakeModel:
    feature_names_ = ['amount']

    def load_model(self, path):
        if 'broken' in path:
            raise ValueError('Incomplete model file')

    def predict_proba(self, frame):
        import numpy as np
        return np.array([[0.8, 0.2]] * len(frame))


class ModelReloadTest(unittest.TestCase):
    def test_good_reload_and_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pointer = root / 'latest.txt'
            pointer.write_text('test_v1')
            (root / 'model_test_v1.json').write_text(json.dumps({'threshold': .1}))
            (root / 'model_test_v2.json').write_text(json.dumps({'threshold': .5}))
            scorer._model = scorer._version = scorer._threshold = None
            with patch.object(scorer, 'MODELS_DIR', root), patch.object(scorer, 'LATEST', pointer), \
                    patch.object(scorer, 'CatBoostClassifier', FakeModel):
                frame = pd.DataFrame({'amount': [10]})
                first, version = scorer.make_pred(frame)
                self.assertEqual((version, int(first.iloc[0]['fraud_flag'])), ('test_v1', 1))
                pointer.write_text('test_v2')
                second, version = scorer.make_pred(frame)
                self.assertEqual((version, int(second.iloc[0]['fraud_flag'])), ('test_v2', 0))
                self.assertEqual(ACTIVE.labels('test_v1')._value.get(), 0)
                self.assertEqual(RELOADS.labels('test_v2')._value.get(), 1)
                previous_errors = LOAD_ERRORS._value.get()
                pointer.write_text('broken')
                with self.assertLogs('scorer', level='ERROR'):
                    third, version = scorer.make_pred(frame)
                self.assertEqual(version, 'test_v2')
                self.assertEqual(int(third.iloc[0]['fraud_flag']), 0)
                self.assertEqual(LOAD_ERRORS._value.get(), previous_errors + 1)
                self.assertEqual(ACTIVE.labels('test_v2')._value.get(), 1)


if __name__ == '__main__':
    unittest.main()
