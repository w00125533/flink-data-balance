import { Graph, Shape } from '@antv/x6';
import { Alert, Card, Empty } from 'antd';
import { useEffect, useRef, useState } from 'react';
import type { MutableRefObject } from 'react';
import type { StageStatus, StageStatusValue } from '../types/observability';
import { resolveFlowEdges } from './flowEdges';

const NODE_WIDTH = 300;
const NODE_HEIGHT = 190;
const CANVAS_HEIGHT = 790;

const statusStyle: Record<StageStatusValue, { stroke: string; fill: string; text: string }> = {
  healthy: { stroke: '#16a34a', fill: '#dcfce7', text: '#166534' },
  warning: { stroke: '#f59e0b', fill: '#fef3c7', text: '#92400e' },
  critical: { stroke: '#dc2626', fill: '#fee2e2', text: '#991b1b' },
  idle: { stroke: '#94a3b8', fill: '#f1f5f9', text: '#475569' },
  unknown: { stroke: '#64748b', fill: '#e2e8f0', text: '#334155' }
};

const nodePositions: Record<string, { x: number; y: number; group: string }> = {
  'chr-source': { x: 32, y: 36, group: 'Source' },
  'pm-source': { x: 32, y: 300, group: 'Source' },
  'cfg-source': { x: 32, y: 564, group: 'Source' },
  kafka: { x: 410, y: 300, group: 'Messaging' },
  assigner: { x: 788, y: 300, group: 'Balance' },
  enrichment: { x: 1166, y: 128, group: 'Process' },
  'load-coordinator': { x: 1166, y: 472, group: 'Balance' },
  'kafka-kpi-1m': { x: 1544, y: 36, group: 'Sink' },
  'starrocks-kpi-1m': { x: 1544, y: 220, group: 'Sink' },
  'hive-kpi-1m': { x: 1544, y: 404, group: 'Sink' },
  'iceberg-kpi-1m': { x: 1544, y: 588, group: 'Sink' },
  'kafka-kpi-5m': { x: 1544, y: 772, group: 'Sink' },
  'starrocks-kpi-5m': { x: 1544, y: 956, group: 'Sink' },
  'hive-kpi-5m': { x: 1544, y: 1140, group: 'Sink' },
  'iceberg-kpi-5m': { x: 1544, y: 1324, group: 'Sink' },
  'kafka-cell-anomaly': { x: 1922, y: 36, group: 'Sink' },
  'kafka-user-anomaly': { x: 1922, y: 220, group: 'Sink' },
  'kafka-grid-anomaly': { x: 1922, y: 404, group: 'Sink' },
  'starrocks-cell-anomaly': { x: 1922, y: 588, group: 'Sink' },
  'starrocks-user-anomaly': { x: 1922, y: 772, group: 'Sink' },
  'starrocks-grid-anomaly': { x: 1922, y: 956, group: 'Sink' },
  'hive-cell-anomaly': { x: 1922, y: 1140, group: 'Sink' },
  'hive-user-anomaly': { x: 1922, y: 1324, group: 'Sink' },
  'hive-grid-anomaly': { x: 1922, y: 1508, group: 'Sink' },
  'iceberg-cell-anomaly': { x: 1922, y: 1692, group: 'Sink' },
  'iceberg-user-anomaly': { x: 1922, y: 1876, group: 'Sink' },
  'iceberg-grid-anomaly': { x: 1922, y: 2060, group: 'Sink' }
};

Shape.HTML.register({
  shape: 'stage-card',
  width: NODE_WIDTH,
  height: NODE_HEIGHT,
  effect: ['data'],
  html(cell) {
    return cell.getData<{ html: string }>()?.html ?? '';
  }
});

export default function StreamingFlowGraph({ stages }: { stages: StageStatus[] }) {
  const containerRef = useRef<HTMLDivElement>(null);
  const graphRef = useRef<Graph | null>(null);
  const resizeObserverRef = useRef<ResizeObserver | null>(null);
  const [renderError, setRenderError] = useState<string>();

  useEffect(() => {
    if (stages.length === 0) {
      destroyGraph(graphRef, resizeObserverRef);
      return;
    }
    if (!containerRef.current) {
      return;
    }

    try {
      if (!graphRef.current) {
        graphRef.current = createGraph(containerRef.current);
        resizeObserverRef.current = new ResizeObserver(() => {
          if (!containerRef.current || !graphRef.current) {
            return;
          }
          graphRef.current.resize(containerRef.current.clientWidth, CANVAS_HEIGHT);
          fitGraph(graphRef.current);
        });
        resizeObserverRef.current.observe(containerRef.current);
      }

      renderGraph(graphRef.current, stages);
      setRenderError(undefined);
    } catch (err) {
      setRenderError(err instanceof Error ? err.message : String(err));
      destroyGraph(graphRef, resizeObserverRef);
    }
  }, [stages]);

  useEffect(() => {
    return () => {
      destroyGraph(graphRef, resizeObserverRef);
    };
  }, []);

  return (
    <Card size="small" title="流处理阶段流程图">
      {renderError ? <Alert type="warning" showIcon message="流程图渲染失败" description={renderError} style={{ marginBottom: 12 }} /> : null}
      {stages.length === 0 ? <Empty description="暂无阶段状态" /> : <div ref={containerRef} style={{ width: '100%', height: CANVAS_HEIGHT }} />}
    </Card>
  );
}

function createGraph(container: HTMLDivElement) {
  return new Graph({
    container,
    width: container.clientWidth,
    height: CANVAS_HEIGHT,
    background: {
      color: '#f8fafc'
    },
    grid: {
      visible: true,
      type: 'mesh',
      args: {
        color: '#e2e8f0',
        thickness: 1
      }
    },
    panning: {
      enabled: true
    },
    mousewheel: {
      enabled: true,
      modifiers: ['ctrl', 'meta'],
      minScale: 0.45,
      maxScale: 1.25
    },
    interacting: {
      nodeMovable: false,
      edgeMovable: false,
      magnetConnectable: false
    }
  });
}

function renderGraph(graph: Graph, stages: StageStatus[]) {
  const stageById = new Map(stages.map((stage) => [stage.stageId, stage]));
  graph.clearCells();

  stages.forEach((stage) => {
    const position = nodePositions[stage.stageId] ?? fallbackPosition(stage.stageId);
    const colors = statusStyle[stage.status] ?? statusStyle.unknown;
    graph.addNode({
      id: stage.stageId,
      shape: 'stage-card',
      x: position.x,
      y: position.y,
      width: NODE_WIDTH,
      height: NODE_HEIGHT,
      data: {
        html: stageCardHtml(stage, position.group, colors)
      }
    });
  });

  resolveFlowEdges(stageById.keys())
    .filter(([source, target]) => stageById.has(source) && stageById.has(target))
    .forEach(([source, target]) => {
      graph.addEdge({
        source,
        target,
        attrs: {
          line: {
            stroke: '#64748b',
            strokeWidth: 1.8,
            targetMarker: {
              name: 'block',
              width: 8,
              height: 6
            }
          }
        },
        connector: {
          name: 'smooth'
        },
        zIndex: -1
      });
    });

  fitGraph(graph);
}

function stageCardHtml(
  stage: StageStatus,
  group: string,
  colors: { stroke: string; fill: string; text: string }
) {
  return `
    <div style="
      width: ${NODE_WIDTH}px;
      height: ${NODE_HEIGHT}px;
      box-sizing: border-box;
      overflow: hidden;
      padding: 14px 16px;
      border: 1.5px solid ${colors.stroke};
      border-radius: 8px;
      background: #ffffff;
      box-shadow: 0 2px 8px rgba(100, 116, 139, 0.12);
      font-family: Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      color: #0f172a;
    ">
      <div style="display: flex; align-items: center; justify-content: space-between; gap: 8px;">
        <div style="font-size: 10px; font-weight: 700; color: #64748b; text-transform: uppercase; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">${escapeHtml(group)}</div>
        <div style="flex: 0 0 auto; max-width: 92px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; padding: 3px 10px; border-radius: 999px; border: 1px solid ${colors.stroke}; background: ${colors.fill}; color: ${colors.text}; font-size: 11px; font-weight: 700;">${escapeHtml(stage.status)}</div>
      </div>
      <div style="margin-top: 7px; font-size: 15px; line-height: 20px; font-weight: 700; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">${escapeHtml(stage.label)}</div>
      <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 6px 10px; margin-top: 10px; font-size: 12px; line-height: 17px;">
        <div style="min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"><span style="color: #64748b;">In</span> ${escapeHtml(formatNumber(stage.inEps))}</div>
        <div style="min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"><span style="color: #64748b;">Out</span> ${escapeHtml(formatNumber(stage.outEps))}</div>
        <div style="min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"><span style="color: #64748b;">P50</span> ${escapeHtml(formatInteger(stage.latencyP50Ms))}ms</div>
        <div style="min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"><span style="color: #64748b;">P95</span> ${escapeHtml(formatInteger(stage.latencyP95Ms))}ms</div>
        <div style="min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"><span style="color: #64748b;">WM</span> ${escapeHtml(formatInteger(stage.watermarkLagMs))}ms</div>
        <div style="min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"><span style="color: #64748b;">DLQ</span> ${escapeHtml(String(stage.dlqCount))}</div>
      </div>
      <div style="height: 32px; margin-top: 9px; overflow: hidden; color: #475569; font-size: 11px; line-height: 16px;">${escapeHtml(stage.summary)}</div>
      <div style="margin-top: 5px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: #94a3b8; font-size: 10px; line-height: 13px;">Updated ${escapeHtml(formatTime(stage.updatedAt))}</div>
    </div>
  `;
}

function fitGraph(graph: Graph) {
  graph.zoomToFit({
    padding: 28,
    maxScale: 0.92
  });
  graph.centerContent();
}

function fallbackPosition(stageId: string) {
  const hash = Array.from(stageId).reduce((sum, char) => sum + char.charCodeAt(0), 0);
  return {
    x: 788 + (hash % 3) * 378,
    y: 36 + (hash % 3) * 264,
    group: 'Stage'
  };
}

function destroyGraph(
  graphRef: MutableRefObject<Graph | null>,
  resizeObserverRef: MutableRefObject<ResizeObserver | null>
) {
  resizeObserverRef.current?.disconnect();
  resizeObserverRef.current = null;
  graphRef.current?.dispose();
  graphRef.current = null;
}

function formatNumber(value: number) {
  if (value >= 1000) {
    return value.toLocaleString(undefined, { maximumFractionDigits: 0 });
  }
  return value.toLocaleString(undefined, { maximumFractionDigits: 1 });
}

function formatInteger(value: number) {
  return value.toLocaleString(undefined, { maximumFractionDigits: 0 });
}

function formatTime(value: string) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleTimeString(undefined, {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  });
}

function escapeHtml(value: string) {
  return value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}
