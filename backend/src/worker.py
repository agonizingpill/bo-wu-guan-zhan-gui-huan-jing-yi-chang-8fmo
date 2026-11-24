import json
import os
from datetime import datetime, timezone

import redis

from src.config.settings import REDIS_URL
from src.services.algorithm import RunCancelled, run_case_analysis
from src.services.job_queue import QUEUE, ensure_job_schema, recover_stale_jobs
from src.utils.db import execute, fetch_one, wait_for_db

HEARTBEAT = '/shared/worker-heartbeat'


def heartbeat():
    os.makedirs('/shared', exist_ok=True)
    with open(HEARTBEAT, 'w', encoding='utf-8') as handle:
        handle.write(datetime.now(timezone.utc).isoformat())


def main():
    wait_for_db()
    ensure_job_schema()
    recovered = recover_stale_jobs()
    for job in recovered:
        print(f"[worker] 已恢复挂起的算法任务 run_id={job['id']} case_id={job['case_id']}", flush=True)

    client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    while True:
        heartbeat()
        item = client.blpop(QUEUE, timeout=5)
        if not item:
            continue
        _, payload = item
        job = json.loads(payload)
        run_id = int(job['run_id'])
        case_id = int(job['case_id'])

        # 撤销/已完成的残留消息直接丢弃；只领取仍在排队的任务
        run = fetch_one('SELECT status FROM algorithm_runs WHERE id=%s', (run_id,))
        if not run:
            print(f'[worker] 丢弃不存在的任务消息 run_id={run_id}', flush=True)
            continue
        if run['status'] != 'queued':
            print(f"[worker] 跳过状态为 {run['status']} 的任务 run_id={run_id}", flush=True)
            continue

        # CAS：同一 run 被重复投递时只有一个 worker 能抢到
        changed = execute(
            "UPDATE algorithm_runs SET status='running', updated_at=NOW() WHERE id=%s AND status='queued'",
            (run_id,),
        )
        if not changed:
            print(f'[worker] 任务已被其他进程领取 run_id={run_id}', flush=True)
            continue

        try:
            run_case_analysis(case_id, run_id)
        except RunCancelled:
            # 撤销在计算途中生效：状态由撤销接口负责，这里不再改写、不发告警
            print(f'[worker] 任务已被值守撤销，停止发布 run_id={run_id}', flush=True)
        except Exception as exc:
            execute("UPDATE algorithm_runs SET status='failed', error=%s, updated_at=NOW() WHERE id=%s", (str(exc), run_id))


if __name__ == '__main__':
    main()
