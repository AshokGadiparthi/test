// HARD-CODED sample JSON for testing
  private graphData: any = {
    nodes: [
      { id: 'dag1', type: 'dag', meta: { task_id: 'DAG 1', label: 'DAG 1' } },
      { id: 'task1', type: 'task', meta: { task_id: 'Task 1', label: 'Task 1' } },
      { id: 'task2', type: 'task', meta: { task_id: 'Task 2', label: 'Task 2' } },
      { id: 'tableA', type: 'bigquery', meta: { canonical_name: 'Table A', label: 'Table A' } },
      { id: 'tableB', type: 'bigquery', meta: { canonical_name: 'Table B', label: 'Table B' } },
    ],
    edges: [
      { source: 'dag1', target: 'task1', type: 'dag_contains' },
      { source: 'task1', target: 'tableA', type: 'task_writes' },
      { source: 'tableA', target: 'task2', type: 'task_reads' },
      { source: 'task2', target: 'tableB', type: 'task_writes' },
    ],
    column_mappings: [
      { source_dataset: 'tableA', source_column: 'col1', target_dataset: 'tableB', target_column: 'colX', confidence: 0.9 },
    ],
  };
