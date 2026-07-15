import { Layout, Menu, Typography } from 'antd';
import { useEffect, useMemo, useState } from 'react';
import { fetchRuntimeConfig } from './api/client';
import CellAnomalies from './pages/CellAnomalies';
import ExecutionHistory from './pages/ExecutionHistory';
import FlowOverview from './pages/FlowOverview';
import GridAnomalies from './pages/GridAnomalies';
import KpiResults from './pages/KpiResults';
import MetricsDashboard from './pages/MetricsDashboard';
import MigrationTimeline from './pages/MigrationTimeline';
import SinkLatency from './pages/SinkLatency';
import UserAnomalies from './pages/UserAnomalies';

type PageKey =
  | 'flow'
  | 'kpi1m'
  | 'kpi5m'
  | 'cellAnomalies'
  | 'userAnomalies'
  | 'gridAnomalies'
  | 'sinkLatency'
  | 'runs'
  | 'metrics'
  | 'migrations';

export default function App() {
  const [page, setPage] = useState<PageKey>('flow');
  const [dynamicBalancingEnabled, setDynamicBalancingEnabled] = useState(false);

  useEffect(() => {
    let active = true;
    fetchRuntimeConfig()
      .then((config) => {
        if (active) {
          setDynamicBalancingEnabled(config.dynamicBalancingEnabled);
        }
      })
      .catch(() => {
        if (active) {
          setDynamicBalancingEnabled(false);
        }
      });
    return () => {
      active = false;
    };
  }, []);

  useEffect(() => {
    if (!dynamicBalancingEnabled && page === 'migrations') {
      setPage('flow');
    }
  }, [dynamicBalancingEnabled, page]);

  const menuItems = useMemo(
    () => [
      { key: 'flow', label: '流处理总览' },
      { key: 'kpi1m', label: 'KPI 1m' },
      { key: 'kpi5m', label: 'KPI 5m' },
      { key: 'userAnomalies', label: '用户异常' },
      { key: 'cellAnomalies', label: '小区异常' },
      { key: 'gridAnomalies', label: '栅格异常' },
      { key: 'sinkLatency', label: 'Sink 耗时' },
      { key: 'runs', label: '执行历史' },
      { key: 'metrics', label: '指标面板' },
      ...(dynamicBalancingEnabled ? [{ key: 'migrations', label: '负载迁移' }] : [])
    ],
    [dynamicBalancingEnabled]
  );

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
          items={menuItems}
        />
      </Layout.Sider>
      <Layout.Content style={{ padding: 24, background: '#f5f7fb' }}>
        {page === 'flow' ? <FlowOverview /> : null}
        {page === 'kpi1m' ? <KpiResults windowKind="1m" /> : null}
        {page === 'kpi5m' ? <KpiResults windowKind="5m" /> : null}
        {page === 'cellAnomalies' ? <CellAnomalies /> : null}
        {page === 'userAnomalies' ? <UserAnomalies /> : null}
        {page === 'gridAnomalies' ? <GridAnomalies /> : null}
        {page === 'sinkLatency' ? <SinkLatency /> : null}
        {page === 'runs' ? <ExecutionHistory /> : null}
        {page === 'metrics' ? <MetricsDashboard /> : null}
        {page === 'migrations' && dynamicBalancingEnabled ? <MigrationTimeline /> : null}
      </Layout.Content>
    </Layout>
  );
}
