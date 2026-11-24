from datetime import datetime, timezone
import json

import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import RobustScaler

from src.utils.db import clean, fetch_all, fetch_one, transactional

RANDOM_STATE = 1069
CONTAMINATION = 0.22

SAMPLE_SELECT = '''
    SELECT s.*, c.target_temp_min, c.target_temp_max, c.target_rh_min, c.target_rh_max
      FROM environment_samples s
      JOIN display_cases c ON c.id = s.case_id'''


class RunCancelled(Exception):
    """值守已撤销任务：停止计算且不发布任何告警。"""

    def __init__(self, run_id: int):
        super().__init__(f'算法任务 {run_id} 已被值守撤销')
        self.run_id = run_id


def _load_run_inputs(run_id: int):
    """按点分析时冻结的采样快照取数；旧任务（无快照）回退为实时全量取数。

    恢复或重试同一 run 时始终读到同一份采样，不会混入分析之后新录入的点。
    """
    run = fetch_one('SELECT status, input_sample_ids FROM algorithm_runs WHERE id=%s', (run_id,))
    if not run:
        raise ValueError(f'算法任务 {run_id} 不存在')
    snapshot = run.get('input_sample_ids') or []
    if not snapshot:
        case = fetch_one('SELECT case_id FROM algorithm_runs WHERE id=%s', (run_id,))
        samples = fetch_all(
            SAMPLE_SELECT + " WHERE s.case_id=%s AND s.state='accepted' ORDER BY s.sampled_at",
            (case['case_id'],),
        )
        return samples, False

    sample_ids = [int(item['sample_id']) for item in snapshot]
    samples = fetch_all(
        SAMPLE_SELECT + ' WHERE s.id = ANY(%s) ORDER BY s.sampled_at',
        (sample_ids,),
    )
    return samples, True


def assert_run_active(run_id: int):
    """发布前确认任务未被撤销。撤销发生在计算途中时直接终止。"""
    run = fetch_one('SELECT status FROM algorithm_runs WHERE id=%s', (run_id,))
    if not run or run['status'] == 'cancelled':
        raise RunCancelled(run_id)


def _publish_results(run_id: int, case_id: int, findings: list, metrics: dict, result: dict):
    """整批发布：所有异常事件与任务终态在一个事务里提交或一起回滚。

    告警等整批算齐才出现；uq_anomaly_events_sample + ON CONFLICT 保证
    重试只更新同一条事件，不会重复发出告警。冲突时不覆盖事件处置状态。
    """
    with transactional() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT status FROM algorithm_runs WHERE id=%s FOR UPDATE', (run_id,))
            row = cur.fetchone()
            if not row or row[0] == 'cancelled':
                raise RunCancelled(run_id)

            published_events = []
            for item in findings:
                cur.execute(
                    '''
                    INSERT INTO anomaly_events
                      (case_id, sample_id, algorithm_run_id, score, threshold, anomaly_type, severity,
                       status, door_context, consecutive_count, escalated_to_curator, reason, created_by)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL)
                    ON CONFLICT (sample_id) DO UPDATE SET
                      algorithm_run_id=EXCLUDED.algorithm_run_id,
                      score=EXCLUDED.score,
                      threshold=EXCLUDED.threshold,
                      anomaly_type=EXCLUDED.anomaly_type,
                      severity=EXCLUDED.severity,
                      door_context=EXCLUDED.door_context,
                      consecutive_count=EXCLUDED.consecutive_count,
                      escalated_to_curator=EXCLUDED.escalated_to_curator,
                      reason=EXCLUDED.reason
                    RETURNING id
                    ''',
                    (case_id, item['sample_id'], run_id, item['score'], item['threshold'],
                     item['type'], item['severity'], item['status'], item['door_context'],
                     item['consecutive_count'], item['escalated'], item['reason']),
                )
                event_id = cur.fetchone()[0]
                item['event_id'] = event_id
                published_events.append({
                    'event_id': event_id,
                    'sample_id': item['sample_id'],
                    'status': item['status'],
                    'type': item['type'],
                    'score': item['score'],
                })

            result['events'] = published_events
            cur.execute(
                "UPDATE algorithm_runs SET status='succeeded', metrics=%s::jsonb, result=%s::jsonb, updated_at=NOW() WHERE id=%s",
                (json.dumps(clean(metrics), ensure_ascii=False),
                 json.dumps(clean(result), ensure_ascii=False), run_id),
            )


def run_case_analysis(case_id: int, run_id: int):
    samples, snapshot_mode = _load_run_inputs(run_id)
    assert_run_active(run_id)

    if len(samples) < 6:
        raise ValueError('至少需要 6 条有效环境采样才能建立异常基线')

    features = np.array([[float(row['temperature']), float(row['humidity']), float(row['co2_ppm'])] for row in samples])
    scaler = RobustScaler()
    scaled = scaler.fit_transform(features)
    model = IsolationForest(contamination=CONTAMINATION, random_state=RANDOM_STATE, n_estimators=160)
    labels = model.fit_predict(scaled)
    scores = model.decision_function(scaled)
    threshold = float(np.quantile(scores, CONTAMINATION))
    previous_flagged = 0
    findings = []

    for row, label, score in zip(samples, labels, scores):
        abnormal = label == -1
        if abnormal:
            previous_flagged += 1
        else:
            previous_flagged = 0
        if not abnormal:
            continue

        door_context = bool(row['door_open'])
        continuous = previous_flagged >= 3
        anomaly_type = 'door_interference' if door_context else ('continuous_drift' if continuous else 'environment_outlier')
        severity = 'watch' if door_context else ('critical' if continuous else 'high')
        escalated = continuous and not door_context
        status = 'escalated' if escalated else 'open'
        reason = (
            '门磁处于开启段，模型异常只作为开门干扰观察，不判定为设备故障'
            if door_context else
            ('连续异常达到 3 个采样点，已升级馆长查看' if continuous else '环境向量偏离展柜稳态基线')
        )
        findings.append({
            'sample_id': row['id'],
            'score': float(score),
            'threshold': threshold,
            'type': anomaly_type,
            'severity': severity,
            'status': status,
            'door_context': door_context,
            'consecutive_count': previous_flagged,
            'escalated': escalated,
            'reason': reason,
        })

    metrics = {
        'sample_count': len(samples),
        'outlier_count': len(findings),
        'outlier_rate': round(len(findings) / len(samples), 4),
        'threshold': round(threshold, 5),
        'door_interference_count': sum(1 for item in findings if item['type'] == 'door_interference'),
        'continuous_upgrade_count': sum(1 for item in findings if item['status'] == 'escalated'),
        'snapshot_mode': snapshot_mode,
    }

    # 整批算齐后再发布；计算途中被撤销则什么都不写
    assert_run_active(run_id)
    result = {
        'events': [],
        'feature_order': ['temperature_c', 'relative_humidity_pct', 'co2_ppm'],
        'scaler': 'RobustScaler(median/IQR)',
        'detector': 'IsolationForest(n_estimators=160, contamination=0.22, random_state=1069)',
        'snapshot_sample_ids': [row['id'] for row in samples],
        'completed_at': datetime.now(timezone.utc).isoformat(),
    }
    _publish_results(run_id, case_id, findings, metrics, result)
    return metrics, result
