CASE_STATUSES = ['active', 'maintenance', 'retired']
ANOMALY_STATUSES = ['open', 'triaged', 'escalated', 'resolved', 'closed']
ANOMALY_TYPES = ['environment_outlier', 'door_interference', 'continuous_drift']
DUTY_ACTION_TYPES = ['acknowledge', 'verify_door_sealed', 'adjust_climate', 'inspect_sensor', 'call_curator']
CHECK_RESULTS = ['pending', 'pass', 'fail']
# 算法任务生命周期：cancelled 为值守撤销；queued/running 为可撤销、可重启恢复的状态
ALGORITHM_RUN_STATUSES = ['queued', 'running', 'succeeded', 'failed', 'cancelled']
ALGORITHM_RUN_CANCELLABLE = ['queued', 'running']
