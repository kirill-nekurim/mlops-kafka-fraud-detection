"""Проверка живых targets, PromQL и всех SQL-панелей через API Grafana."""

import base64
import json
import math
import os
import time
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

GRAFANA = os.getenv('GRAFANA_URL', 'http://localhost:3000')
PROMETHEUS = os.getenv('PROMETHEUS_URL', 'http://localhost:9090')
AUTH = base64.b64encode(b'admin:admin').decode()


def get(url, data=None):
    headers = {'Authorization': 'Basic ' + AUTH}
    if data is not None:
        headers['Content-Type'] = 'application/json'
    request = Request(url, headers=headers, data=json.dumps(data).encode() if data is not None else None)
    with urlopen(request, timeout=30) as response:
        return json.load(response)


def query(sql):
    return get(GRAFANA + '/api/ds/query', {
        'from': str(int((time.time() - 900) * 1000)), 'to': str(int(time.time() * 1000)),
        'queries': [{'refId': 'A', 'datasource': {'uid': 'postgres', 'type': 'postgres'},
                     'rawSql': sql, 'format': 'table', 'rawQuery': True}],
    })['results']['A']


def main():
    report = {'checked_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
              'targets': [], 'dashboards': [], 'panels': [], 'sql_filters': []}
    targets = get(PROMETHEUS + '/api/v1/targets')['data']['activeTargets']
    assert len(targets) == 3, targets
    for target in targets:
        assert target['health'] == 'up', target
        report['targets'].append(target['labels']['job'])
    dashboards = get(GRAFANA + '/api/search?type=dash-db')
    assert len(dashboards) == 3, dashboards
    for item in dashboards:
        dashboard = get(GRAFANA + '/api/dashboards/uid/' + item['uid'])['dashboard']
        report['dashboards'].append(item['title'])
        all_values = {v['name']: v['allValue'] for v in dashboard['templating']['list']}
        for value in all_values.values():
            assert value == "'__all'", 'Custom All value must include SQL quotes'
        for panel in dashboard['panels']:
            for target in panel['targets']:
                if 'rawSql' in target:
                    sql = target['rawSql'].replace('${state:sqlstring}', all_values['state']).replace('${merchant:sqlstring}', all_values['merchant'])
                    result = query(sql)
                    assert not result.get('error'), result
                    assert result.get('frames'), (panel['title'], result)
                    assert any(frame.get('data', {}).get('values') and frame['data']['values'][0]
                               for frame in result['frames']), (panel['title'], result)
                else:
                    result = get(PROMETHEUS + '/api/v1/query?' + urlencode({'query': target['expr']}))
                    assert result['status'] == 'success', result
                    assert result['data']['result'], (panel['title'], result)
                    assert any(math.isfinite(float(series['value'][1]))
                               for series in result['data']['result']), (panel['title'], result)
            report['panels'].append(panel['title'])
    totals = query('SELECT count(*) AS scores, count(DISTINCT model_version) AS versions, '
                   "count(*) FILTER (WHERE scored_at IS NOT NULL AND (us_state='UNKNOWN' OR merch='UNKNOWN' OR cat_id='UNKNOWN')) AS missing FROM scores")
    values = totals['frames'][0]['data']['values']
    assert values[0][0] > 20 and values[2][0] == 0, totals
    report['totals'] = {field['name']: val[0] for field, val in zip(totals['frames'][0]['schema']['fields'], values)}
    # Одна и та же панель должна работать и с реальными выбранными значениями.
    row = query('SELECT us_state, merch FROM scores ORDER BY id LIMIT 1')['frames'][0]['data']['values']
    quote = lambda value: "'" + value.replace("'", "''") + "'"
    d = get(GRAFANA + '/api/dashboards/uid/fraud-scoring')['dashboard']
    for panel in d['panels']:
        for target in panel['targets']:
            if 'rawSql' in target:
                sql = target['rawSql'].replace('${state:sqlstring}', quote(row[0][0])).replace('${merchant:sqlstring}', quote(row[1][0]))
                result = query(sql)
                assert not result.get('error'), result
                assert result.get('frames'), (panel['title'], result)
                report['sql_filters'].append(panel['title'])
    rules = get(GRAFANA + '/api/v1/provisioning/alert-rules')
    assert {r['uid'] for r in rules} == {'flow-stopped', 'high-fraud'}, rules
    report['alert_rules'] = [r['title'] for r in rules]
    path = Path(__file__).resolve().parents[1] / 'docs' / 'monitoring-check.json'
    path.parent.mkdir(exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
