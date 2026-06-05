import { Card } from 'antd';

export default function GrafanaEmbedPanel() {
  const src = import.meta.env.VITE_GRAFANA_URL
    || 'http://localhost:3000/d/fdb-streaming-observability/flink-data-balance-streaming-observability?orgId=1&refresh=10s';
  return (
    <Card size="small" title="Grafana 详细指标">
      <iframe title="Grafana" src={src} style={{ width: '100%', height: 720, border: 0 }} />
    </Card>
  );
}
