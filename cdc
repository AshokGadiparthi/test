import { Component, Input, OnInit, OnChanges, SimpleChanges } from '@angular/core';
import cytoscape from 'cytoscape';
import dagre from 'cytoscape-dagre';
import tippy from 'tippy.js';
import 'tippy.js/dist/tippy.css';

cytoscape.use(dagre);

@Component({
  selector: 'app-lineage-graph',
  template: '<div id="lineageGraph" style="width:100%; height:90vh;"></div>',
  styles: []
})
export class LineageGraphComponent implements OnInit, OnChanges {
  @Input() graphData: any;
  private cy: any;

  constructor() {}

  ngOnInit() {
    if (this.graphData) this.initializeGraph();
  }

  ngOnChanges(changes: SimpleChanges) {
    if (changes.graphData && !changes.graphData.firstChange) {
      this.initializeGraph();
    }
  }

  private initializeGraph() {
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
        rankDir: 'TB', // top-to-bottom
        nodeSep: 50,
        edgeSep: 10,
      } as any, // cast as any to avoid TypeScript errors
      userZoomingEnabled: true,
      wheelSensitivity: 0.2,
      selectionType: 'single',
    });

    this.setupInteractions();
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

    const columnEdges: any[] = [];
    const bundledColumnEdges = new Map<string, any>();

    // Nodes
    data.nodes.forEach((n: any) => {
      const cfg = nodeConfigs[n.type] || nodeConfigs['unknown'];
      const label = n.meta.label || n.meta.task_id || n.meta.canonical_name || n.id;

      elements.push({
        data: { id: n.id, label, color: cfg.color, shape: cfg.shape, size: cfg.size, meta: n.meta },
      });
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

  private setupInteractions() {
    // Hover tooltips
    this.cy.nodes().forEach((node: any) => {
      const ref = node.popperRef();
      const tip = tippy(document.createElement('div'), {
        content: this.renderTooltip(node),
        trigger: 'manual',
        placement: 'bottom',
        hideOnClick: true,
        interactive: true,
      });
      node.on('mouseover', () => tip.show());
      node.on('mouseout', () => tip.hide());
    });

    // Drag & expand/collapse
    this.cy.on('tap', 'node', (evt: any) => {
      const node = evt.target;
      const children = node.children();
      if (children && children.length > 0) {
        children.toggleClass('hidden');
        this.cy.layout({ name: 'dagre', rankDir: 'TB', nodeSep: 50, edgeSep: 10 } as any).run();
      }
    });

    this.cy.nodes().grabify();
  }

  private renderTooltip(node: any) {
    const meta = node.data('meta');
    return `<b>${node.data('label')}</b><br/><pre style="max-height:200px; overflow:auto;">${JSON.stringify(
      meta,
      null,
      2
    )}</pre>`;
  }
}
