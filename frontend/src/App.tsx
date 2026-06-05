import { Layout, Menu, Typography } from 'antd';
import { useState } from 'react';
import ExecutionHistory from './pages/ExecutionHistory';
import FlowOverview from './pages/FlowOverview';
import MetricsDashboard from './pages/MetricsDashboard';
import MigrationTimeline from './pages/MigrationTimeline';

type PageKey = 'flow' | 'runs' | 'migrations' | 'metrics';

export default function App() {
  const [page, setPage] = useState<PageKey>('flow');

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Layout.Sider theme="light" width={220} breakpoint="lg" collapsedWidth={0}>
        <div style={{ padding: 20 }}>
          <Typography.Text strong>FDB Observability</Typography.Text>
        </div>
        <Menu
          mode="inline"
          selectedKeys={[page]}
          onClick={(item) => setPage(item.key as PageKey)}
          items={[
            { key: 'flow', label: '流处理总览' },
            { key: 'runs', label: '执行历史' },
            { key: 'migrations', label: '负载迁移' },
            { key: 'metrics', label: '指标面板' }
          ]}
        />
      </Layout.Sider>
      <Layout.Content style={{ padding: 24, background: '#f5f7fb' }}>
        {page === 'flow' ? <FlowOverview /> : null}
        {page === 'runs' ? <ExecutionHistory /> : null}
        {page === 'migrations' ? <MigrationTimeline /> : null}
        {page === 'metrics' ? <MetricsDashboard /> : null}
      </Layout.Content>
    </Layout>
  );
}
