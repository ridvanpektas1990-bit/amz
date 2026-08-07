-- etl_runs writers (GitHub Actions / Python) use status "success" or "ok".
-- The original check rejected "success", so nightly logs never landed and Monitoring stayed grey.
alter table public.etl_runs drop constraint if exists etl_runs_status_check;

alter table public.etl_runs
  add constraint etl_runs_status_check
  check (
    status is null
    or lower(status) in (
      'ok',
      'success',
      'error',
      'failed',
      'failure',
      'running',
      'queued',
      'skipped'
    )
  );
