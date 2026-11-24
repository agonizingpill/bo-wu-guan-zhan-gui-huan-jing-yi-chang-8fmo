-- 算法任务生命周期：取消态、采样快照、重启恢复
-- 001_init.sql 的旧约束不含 cancelled，需要先删除再重建
ALTER TABLE algorithm_runs DROP CONSTRAINT IF EXISTS algorithm_runs_status_check;
ALTER TABLE algorithm_runs ADD CONSTRAINT algorithm_runs_status_check
  CHECK (status IN ('queued','running','succeeded','failed','cancelled'));

-- 点分析时冻结的采样集合（仅保存 sample id 与采样时间），恢复/重试用同一份采样
ALTER TABLE algorithm_runs ADD COLUMN IF NOT EXISTS input_sample_ids JSONB NOT NULL DEFAULT '[]'::jsonb;
-- 重启恢复重新入队的次数
ALTER TABLE algorithm_runs ADD COLUMN IF NOT EXISTS recovery_count INTEGER NOT NULL DEFAULT 0;
-- 撤销任务的值守人与时间，页面可查询
ALTER TABLE algorithm_runs ADD COLUMN IF NOT EXISTS cancelled_by INTEGER REFERENCES users(id);
ALTER TABLE algorithm_runs ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMPTZ;

-- 告警幂等：一条采样最多对应一条异常事件，重试不重复发告警
CREATE UNIQUE INDEX IF NOT EXISTS uq_anomaly_events_sample
  ON anomaly_events(sample_id);
