Table: airflow_monitor.task_runs		
		
Column Name	Type	Description
run_id	STRING	DAG run ID
dag_id	STRING	DAG name
task_id	STRING	Task ID
execution_date	TIMESTAMP	Scheduled DAG execution
start_time	TIMESTAMP	Task start time
end_time	TIMESTAMP	Task end time
duration_seconds	FLOAT	Task runtime
status	STRING	running / success / failed / skipped / upstream_failed
try_number	INT	Current attempt number
retries	INT	Max retries for task
owner	STRING	DAG/task owner
environment	STRING	dev/prod
upstream_task_ids	STRING	JSON list of upstream task_ids
downstream_task_ids	STRING	JSON list of downstream task_ids
failure_reason	STRING	Exception message or failure reason
root_cause_task_id	STRING	If failed due to upstream, the parent/root failing task
inputs	STRING	JSON list of input datasets/files
outputs	STRING	JSON list of output datasets/files
inserted_at	TIMESTAMP	Metadata insert timestamp
		
Key points:		
		
root_cause_task_id allows failure traceability. If a task fails because an upstream task failed, you can store the failing parent task.		
		
failure_reason gives user-readable error message.		
		
Upstream/downstream lists allow reconstructing DAG dependencies.		
