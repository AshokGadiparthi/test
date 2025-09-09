import { Component, OnInit } from '@angular/core';
import cytoscape from 'cytoscape';
import dagre from 'cytoscape-dagre';

cytoscape.use(dagre);

@Component({
  selector: 'app-lineage-graph',
  template: '<div id="lineageGraph" style="width:100%; height:90vh; border:1px solid #ccc;"></div>',
})
export class LineageGraphComponent implements OnInit {
  private cy: any;

  // Minimal sample data
  private graphData = {
    nodes: [
      { id: 'dag1', type: 'dag', label: 'DAG 1' },
      { id: 'task1', type: 'task', label: 'Task 1' },
      { id: 'task2', type: 'task', label: 'Task 2' },
      { id: 'tableA', type: 'bigquery', label: 'Table A' },
      { id: 'tableB', type: 'bigquery', label: 'Table B' },
    ],
    edges: [
      { source: 'dag1', target: 'task1', type: 'dag_contains' },
      { source: 'task1', target: 'tableA', type: 'task_writes' },
      { source: 'tableA', target: 'task2', type: 'task_reads' },
      { source: 'task2', target: 'tableB', type: 'task_writes' },
    ],
  };

  ngOnInit() {
    this.initializeGraph();
  }

  private initializeGraph() {
    // Destroy previous instance
    if (this.cy) this.cy.destroy();

    const elements = [];

    // Map types to colors/shapes
    const nodeStyles: any = {
      dag: { color: '#1f77b4', shape: 'round-rectangle' },
      task: { color: '#ff7f0e', shape: 'rectangle' },
      bigquery: { color: '#2ca02c', shape: 'ellipse' },
    };

    // Nodes
    this.graphData.nodes.forEach((n: any) => {
      const style = nodeStyles[n.type] || { color: '#888', shape: 'rectangle' };
      elements.push({
        data: { id: n.id, label: n.label, color: style.color, shape: style.shape },
      });
    });

    // Edges
    this.graphData.edges.forEach((e: any) => {
      let color = '#888';
      if (e.type === 'dag_contains') color = '#1f77b4';
      if (e.type === 'task_reads') color = '#2ca02c';
      if (e.type === 'task_writes') color = '#d62728';

      elements.push({
        data: { source: e.source, target: e.target, label: e.type, color },
      });
    });

    // Initialize Cytoscape
    this.cy = cytoscape({
      container: document.getElementById('lineageGraph'),
      elements,
      style: [
        {
          selector: 'node',
          style: {
            label: 'data(label)',
            'background-color': 'data(color)',
            shape: 'data(shape)',
            width: 50,
            height: 50,
            'text-valign': 'center',
            'text-halign': 'center',
            'font-size': 12,
          },
        },
        {
          selector: 'edge',
          style: {
            width: 2,
            'line-color': 'data(color)',
            'target-arrow-color': 'data(color)',
            'target-arrow-shape': 'triangle',
            'curve-style': 'bezier',
            label: 'data(label)',
            'font-size': 10,
            'text-rotation': 'autorotate',
          },
        },
      ],
      layout: {
        name: 'dagre',
        rankDir: 'TB',
        nodeSep: 50,
        edgeSep: 10,
      } as any,
    });
  }
}
