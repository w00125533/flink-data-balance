import '@testing-library/jest-dom/vitest';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import type { ReactNode } from 'react';
import { beforeEach, expect, test, vi } from 'vitest';
import App from './App';
import { kpiRowKey } from './pages/KpiResults';
import { anomalyRowKey } from './pages/CellAnomalies';
import { gridPointPosition, gridAnomalyRowKey } from './pages/GridAnomalies';

const client = vi.hoisted(() => ({
  fetchRuntimeConfig: vi.fn(),
  fetchKpiResults: vi.fn(),
  fetchCellAnomalies: vi.fn(),
  fetchGridAnomalies: vi.fn(),
  fetchSinkLatency: vi.fn()
}));

vi.mock('./api/client', async () => {
  const actual = await vi.importActual<typeof import('./api/client')>('./api/client');
  return {
    ...actual,
    fetchRuntimeConfig: client.fetchRuntimeConfig,
    fetchKpiResults: client.fetchKpiResults,
    fetchCellAnomalies: client.fetchCellAnomalies,
    fetchGridAnomalies: client.fetchGridAnomalies,
    fetchSinkLatency: client.fetchSinkLatency
  };
});

vi.mock('./pages/FlowOverview', () => ({
  default: () => <h1>Flow Overview</h1>
}));

vi.mock('./pages/ExecutionHistory', () => ({
  default: () => <h1>Execution History</h1>
}));

vi.mock('./pages/MetricsDashboard', () => ({
  default: () => <h1>Metrics Dashboard</h1>
}));

vi.mock('antd', () => {
  function Shell({ children }: { children: ReactNode }) {
    return <div>{children}</div>;
  }

  function Field({ children, label }: { children?: ReactNode; label?: ReactNode }) {
    return (
      <label>
        {label}
        {children}
      </label>
    );
  }

  return {
    Alert: ({ message, description }: { message?: ReactNode; description?: ReactNode }) => (
      <div role="alert">
        {message}
        {description}
      </div>
    ),
    Button: ({ children, htmlType, onClick }: { children?: ReactNode; htmlType?: 'submit'; onClick?: () => void }) => (
      <button type={htmlType === 'submit' ? 'submit' : 'button'} onClick={onClick}>
        {children}
      </button>
    ),
    Card: ({ children, title }: { children?: ReactNode; title?: ReactNode }) => (
      <section>
        {title ? <h2>{title}</h2> : null}
        {children}
      </section>
    ),
    Col: Shell,
    Empty: ({ description }: { description?: ReactNode }) => <div>{description}</div>,
    Form: Object.assign(
      ({ children, onFinish }: { children?: ReactNode; onFinish?: (values: Record<string, unknown>) => void }) => (
        <form
          onSubmit={(event) => {
            event.preventDefault();
            onFinish?.({});
          }}
        >
          {children}
        </form>
      ),
      {
        Item: Field,
        useForm: () => [{ resetFields: vi.fn(), getFieldsValue: vi.fn(() => ({})), setFieldsValue: vi.fn() }]
      }
    ),
    Input: ({ placeholder }: { placeholder?: string }) => <input placeholder={placeholder} />,
    InputNumber: ({ placeholder }: { placeholder?: string }) => <input placeholder={placeholder} />,
    Layout: Object.assign(Shell, {
      Sider: Shell,
      Content: Shell
    }),
    Menu: ({
      items,
      onClick
    }: {
      items: Array<{ key: string; label: string }>;
      onClick?: (item: { key: string }) => void;
    }) => (
      <nav>
        {items.map((item) => (
          <button key={item.key} type="button" onClick={() => onClick?.({ key: item.key })}>
            {item.label}
          </button>
        ))}
      </nav>
    ),
    Row: Shell,
    Select: ({ placeholder }: { placeholder?: string }) => <select aria-label={placeholder} />,
    Space: Shell,
    Table: ({ columns, dataSource }: { columns: Array<{ dataIndex?: string; title: ReactNode }>; dataSource: any[] }) => (
      <table>
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={String(column.dataIndex ?? column.title)}>{column.title}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {dataSource.map((row, rowIndex) => (
            <tr key={rowIndex}>
              {columns.map((column) => (
                <td key={String(column.dataIndex ?? column.title)}>
                  {column.dataIndex ? String(row[column.dataIndex] ?? '') : null}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    ),
    Tag: ({ children }: { children?: ReactNode }) => <span>{children}</span>,
    Typography: {
      Text: ({ children }: { children: ReactNode }) => <span>{children}</span>,
      Title: ({ children }: { children: ReactNode }) => <h1>{children}</h1>
    }
  };
});

beforeEach(() => {
  client.fetchRuntimeConfig.mockResolvedValue({
    dynamicBalancingEnabled: false,
    resultSink: 'starrocks',
    dlqEnabled: true,
    metricsEnabled: true,
    metricsHistoryEnabled: true,
    runId: 'run-test',
    runLabel: '',
    parallelism: 4,
    checkpointIntervalMs: 30_000,
    jobStatus: 'running',
    reportStatus: 'collecting'
  });
  client.fetchKpiResults.mockResolvedValue([]);
  client.fetchCellAnomalies.mockResolvedValue([]);
  client.fetchGridAnomalies.mockResolvedValue([]);
  client.fetchSinkLatency.mockResolvedValue([]);
});

test('renders result navigation entries and hides migrations when dynamic balancing is disabled', async () => {
  render(<App />);

  expect(await screen.findByRole('heading', { name: 'Flow Overview' })).toBeInTheDocument();
  expect(screen.getByRole('button', { name: 'KPI 1m' })).toBeInTheDocument();
  expect(screen.getByRole('button', { name: 'KPI 5m' })).toBeInTheDocument();
  expect(screen.getByRole('button', { name: '小区异常' })).toBeInTheDocument();
  expect(screen.getByRole('button', { name: '栅格异常' })).toBeInTheDocument();
  expect(screen.getByRole('button', { name: 'Sink 耗时' })).toBeInTheDocument();
  expect(screen.getByRole('button', { name: '执行历史' })).toBeInTheDocument();
  expect(screen.getByRole('button', { name: '指标面板' })).toBeInTheDocument();
  await waitFor(() => expect(screen.queryByRole('button', { name: '负载迁移' })).not.toBeInTheDocument());
});

test('loads mocked KPI rows from the KPI 1m page', async () => {
  client.fetchKpiResults.mockResolvedValue([
    {
      windowStartTs: 1717400000000,
      windowEndTs: 1717400060000,
      windowKind: 'MIN_1',
      joinQuality: 'JOINED',
      siteId: 'SITE-001',
      cellId: 'CELL-001',
      gridId: 'wx4g0e',
      numChrEvents: 10,
      numUsers: 42,
      avgRsrp: -92.1,
      avgSinr: 12.4,
      avgPrbUsageDl: 0.7,
      throughputDlMbpsAvg: 120.5,
      dropRate: 0.01,
      hoSuccessRate: 0.95,
      attachSuccessRate: 0.98
    }
  ]);

  render(<App />);
  fireEvent.click(await screen.findByRole('button', { name: 'KPI 1m' }));

  expect(await screen.findByRole('columnheader', { name: 'windowKind' })).toBeInTheDocument();
  expect(await screen.findByText('MIN_1')).toBeInTheDocument();
  expect(await screen.findByText('CELL-001')).toBeInTheDocument();
  expect(client.fetchKpiResults).toHaveBeenCalledWith('1m', {});
});

test('builds stable natural keys without row index input', () => {
  const kpiRow = {
    windowStartTs: 1717400000000,
    windowEndTs: 1717400060000,
    windowKind: 'MIN_1',
    joinQuality: 'JOINED',
    siteId: null,
    cellId: 'CELL-001',
    gridId: 'wx4g0e'
  };
  const cellAnomalyRow = {
    detectionTs: 1717400000000,
    eventTs: 1717399999000,
    siteId: 'SITE-001',
    cellId: 'CELL-001',
    gridId: null,
    anomalyType: 'LOW_SIGNAL',
    severity: 'HIGH'
  };
  const gridAnomalyRow = {
    detectionTs: 1717400000000,
    eventTs: 1717399999000,
    gridId: 'wx4g0e',
    anomalyType: 'COVERAGE_HOLE',
    severity: 'MEDIUM',
    latitude: 39.9,
    longitude: 116.4
  };

  expect(kpiRowKey(kpiRow)).toBe(kpiRowKey(kpiRow));
  expect(kpiRowKey(kpiRow)).toBe('1717400000000-1717400060000-CELL-001-MIN_1-JOINED');
  expect(anomalyRowKey(cellAnomalyRow)).toBe(anomalyRowKey(cellAnomalyRow));
  expect(anomalyRowKey(cellAnomalyRow)).toBe('1717400000000-1717399999000-SITE-001-CELL-001-<null>-LOW_SIGNAL-HIGH');
  expect(gridAnomalyRowKey(gridAnomalyRow)).toBe(gridAnomalyRowKey(gridAnomalyRow));
  expect(gridAnomalyRowKey(gridAnomalyRow)).toBe('1717400000000-1717399999000-wx4g0e-COVERAGE_HOLE-MEDIUM-39.9-116.4');
});

test('centers grid preview points on zero-span axes', () => {
  expect(gridPointPosition(39.9, 116.4, { minLat: 39.9, maxLat: 39.9, minLon: 116.4, maxLon: 116.4 })).toEqual({
    left: 50,
    top: 50
  });
  expect(gridPointPosition(39.9, 116.4, { minLat: 39.8, maxLat: 40, minLon: 116.4, maxLon: 116.4 })).toEqual({
    left: 50,
    top: 50
  });
});
