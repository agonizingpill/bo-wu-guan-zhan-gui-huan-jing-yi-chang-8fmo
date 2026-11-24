"""算法任务队列：重启恢复、幂等 schema 保障与状态机常量。

后台或 worker 重启后，留在 queued/running 的任务可能一直挂着：
Redis 里的消息会随容器销毁丢失，running 任务也随旧 worker 进程消失。
两个进程启动时都调用 recover_stale_jobs，把挂起任务重新投递回队列。
"""

import json

import redis

from src.config.settings import REDIS_URL
from src.utils.db import execute, fetch_all

QUEUE = 'museum_case_anomaly_jobs'

RUN_STATUSES = ('queued', 'running', 'succeeded', 'failed', 'cancelled')
# 值守可以撤销的状态：还在排队或正在计算（尚未发布告警）
CANCELLABLE_STATUSES = ('queued', 'running')


def ensure_job_schema():
    """对已有数据卷做幂等升级，效果与 migrations/002_job_lifecycle.sql 一致。

    docker-entrypoint-initdb.d 只在空卷首次初始化时执行，已存在的卷不会
    自动跑新迁移，因此启动时再做一次 IF NOT EXISTS 式补齐。
    """
    execute(
        """
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname='algorithm_runs_status_check')
             AND NOT EXISTS (
               SELECT 1 FROM pg_constraint
               WHERE conname='algorithm_runs_status_check'
                 AND pg_get_constraintdef(oid) LIKE '%%cancelled%%'
             ) THEN
            ALTER TABLE algorithm_runs DROP CONSTRAINT algorithm_runs_status_check;
          END IF;
          IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='algorithm_runs_status_check') THEN
            ALTER TABLE algorithm_runs ADD CONSTRAINT algorithm_runs_status_check
              CHECK (status IN ('queued','running','succeeded','failed','cancelled'));
          END IF;
        END $$;
        """
    )
    execute("ALTER TABLE algorithm_runs ADD COLUMN IF NOT EXISTS input_sample_ids JSONB NOT NULL DEFAULT '[]'::jsonb")
    execute("ALTER TABLE algorithm_runs ADD COLUMN IF NOT EXISTS recovery_count INTEGER NOT NULL DEFAULT 0")
    execute("ALTER TABLE algorithm_runs ADD COLUMN IF NOT EXISTS cancelled_by INTEGER REFERENCES users(id)")
    execute("ALTER TABLE algorithm_runs ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMPTZ")
    # 一条采样最多对应一条异常事件，保证重试时告警 upsert 而不是重复插入
    execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_anomaly_events_sample ON anomaly_events(sample_id)"
    )


def recover_stale_jobs():
    """把挂起的算法任务重新投递到 Redis 队列，返回恢复的任务列表。

    - queued：消息可能随 Redis 重启丢失，按未投递处理重新入队
    - running：旧 worker 已随重启消失，任务不可能再完成，重新入队并计恢复次数
    已经在队列里的消息不重复投递；succeeded/failed/cancelled 不动。
    """
    client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    queued_payloads = client.lrange(QUEUE, 0, -1)
    queued_run_ids = set()
    for payload in queued_payloads:
        try:
            queued_run_ids.add(int(json.loads(payload)['run_id']))
        except (ValueError, KeyError, TypeError):
            continue

    stale = fetch_all(
        "SELECT id, case_id FROM algorithm_runs WHERE status IN ('queued','running') ORDER BY id"
    )
    recovered = []
    for job in stale:
        if job['id'] in queued_run_ids:
            continue
        # 原子标记：running 计入恢复次数，queued 保持 0 次
        execute(
            """
            UPDATE algorithm_runs
               SET status='queued',
                   recovery_count = CASE WHEN status='running' THEN recovery_count + 1 ELSE recovery_count END,
                   updated_at=NOW()
             WHERE id=%s AND status IN ('queued','running')
            """,
            (job['id'],),
        )
        client.rpush(QUEUE, json.dumps({'run_id': job['id'], 'case_id': job['case_id']}))
        recovered.append(job)
    return recovered
