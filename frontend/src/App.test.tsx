import '@testing-library/jest-dom/vitest';
import { render, screen } from '@testing-library/react';
import type { ReactNode } from 'react';
import { expect, test, vi } from 'vitest';
import App from './App';

vi.mock('./pages/FlowOverview', () => ({
  default: () => <h1>实时流处理总览</h1>
}));

vi.mock('./pages/ExecutionHistory', () => ({
  default: () => <h1>执行历史</h1>
}));

vi.mock('./pages/MigrationTimeline', () => ({
  default: () => <h1>负载迁移时间线</h1>
}));

vi.mock('./pages/MetricsDashboard', () => ({
  default: () => <h1>指标面板</h1>
}));

vi.mock('antd', () => {
  function Shell({ children }: { children: ReactNode }) {
    return <div>{children}</div>;
  }

  return {
    Layout: Object.assign(Shell, {
      Sider: Shell,
      Content: Shell
    }),
    Menu: ({ items }: { items: Array<{ key: string; label: string }> }) => (
      <nav>
        {items.map((item) => (
          <span key={item.key}>{item.label}</span>
        ))}
      </nav>
    ),
    Typography: {
      Text: ({ children }: { children: ReactNode }) => <span>{children}</span>
    }
  };
});

test('renders page title and navigation', () => {
  render(<App />);

  expect(screen.getByRole('heading', { name: '实时流处理总览' })).toBeInTheDocument();
  expect(screen.getByText('流处理总览')).toBeInTheDocument();
  expect(screen.getByText('执行历史')).toBeInTheDocument();
  expect(screen.getByText('负载迁移')).toBeInTheDocument();
  expect(screen.getByText('指标面板')).toBeInTheDocument();
});
