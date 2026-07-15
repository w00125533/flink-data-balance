import '@testing-library/jest-dom/vitest';
import { render, screen } from '@testing-library/react';
import type { ReactNode } from 'react';
import { beforeEach, expect, test, vi } from 'vitest';
import { fetchUserAnomalies } from '../api/client';
import UserAnomalies from './UserAnomalies';

vi.mock('../api/client', () => ({
  fetchUserAnomalies: vi.fn()
}));

vi.mock('antd', () => {
  function Shell({ children }: { children?: ReactNode }) {
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
    Button: ({ children, htmlType }: { children?: ReactNode; htmlType?: 'submit' }) => (
      <button type={htmlType === 'submit' ? 'submit' : 'button'}>{children}</button>
    ),
    Card: ({ children }: { children?: ReactNode }) => <section>{children}</section>,
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
    Typography: {
      Title: ({ children }: { children: ReactNode }) => <h1>{children}</h1>
    }
  };
});

beforeEach(() => {
  vi.mocked(fetchUserAnomalies).mockResolvedValue([
    {
      detectionTs: 1717400000000,
      eventTs: 1717399999000,
      entityType: 'USER',
      entityId: '460001234567890',
      windowStartTs: 1717399400000,
      windowEndTs: 1717399999000,
      imsi: '460001234567890',
      siteId: 'SITE-001',
      cellId: 'CELL-001',
      gridId: 'wx4g0e',
      anomalyType: 'USER_FAILURE',
      severity: 'HIGH',
      contextJson: '{"event":"ATTACH_FAIL"}',
      latitude: null,
      longitude: null,
      ruleVersion: 'v1.0'
    }
  ]);
});

test('renders user anomaly result columns and rows', async () => {
  render(<UserAnomalies />);

  expect(await screen.findByRole('columnheader', { name: 'entityType' })).toBeInTheDocument();
  expect(screen.getByRole('columnheader', { name: 'imsi' })).toBeInTheDocument();
  expect(await screen.findByText('USER_FAILURE')).toBeInTheDocument();
  expect(fetchUserAnomalies).toHaveBeenCalledWith({});
});
