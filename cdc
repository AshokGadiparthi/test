import { Component, OnInit } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import cytoscape from 'cytoscape';
import dagre from 'cytoscape-dagre';

cytoscape.use(dagre);

@Component({
  selector: 'app-lineage-graph',
  template: '<div id="lineageGraph" style="width:100%; height:90vh;"></div>',
  styles: []
})
export class LineageGraphComponent implements OnInit {
  private cy: any;
  private graphData: any;

  constructor(private http: HttpClient) {}

  ngOnInit() {
    // Load JSON from assets
    this.http.get('/assets/full_lineage_graph.json').subscribe(
      (data) => {
        this.graphData = data;
        this.initializeGraph();
      },
      (err) => console.error('Failed to load JSON', err)
    );
  }

  private initializeGraph() {
    if (!this.graphData) return;
    if (this.cy) this.cy.destroy();

    const elements = this.buildElements(this.graphData);

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
            'text-valign': 'center',
            'text-halign': 'center',
            width: 'data(size)',
            height: 'data(size)',
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
        { selector: '.hidden', style: { display: 'none' } },
      ],
      layout: {
        name: 'dagre',
        rankDir: 'TB',
        nodeSep: 50,
        edgeSep: 10,
      } as any,
      userZoomingEnabled: true,
      wheelSensitivity: 0.2,
      selectionType: 'single',
    });

    this.cy.nodes().grabify();
  }

  private buildElements(data: any) {
    const elements: any[] = [];
    const nodeConfigs: any = {
      dag: { color: '#1f77b4', shape: 'round-rectangle', size: 60 },
      task: { color: '#ff7f0e', shape: 'rectangle', size: 50 },
      bigquery: { color: '#2ca02c', shape: 'ellipse', size: 40 },
      spanner: { color: '#d62728', shape: 'ellipse', size: 40 },
      redis: { color: '#9467bd', shape: 'diamond', size: 35 },
      kafka: { color: '#8c564b', shape: 'diamond', size: 35 },
      file: { color: '#e377c2', shape: 'rectangle', size: 35 },
      unknown: { color: '#7f7f7f', shape: 'rectangle', size: 35 },
    };

    // Nodes
    data.nodes.forEach((n: any) => {
      const cfg = nodeConfigs[n.type] || nodeConfigs['unknown'];
      const label = n.meta?.label || n.meta?.task_id || n.meta?.canonical_name || n.id;
      elements.push({ data: { id: n.id, label, color: cfg.color, shape: cfg.shape, size: cfg.size, meta: n.meta } });
    });

    // Edges
    data.edges.forEach((e: any) => {
      let color = '#888888';
      if (e.type === 'task_to_task') color = '#ff9900';
      if (e.type === 'task_reads') color = '#2ca02c';
      if (e.type === 'task_writes') color = '#d62728';
      if (e.type === 'dataset_to_dataset') color = '#1f77b4';

      elements.push({ data: { source: e.source, target: e.target, label: e.type, color } });
    });

    return elements;
  }
}
