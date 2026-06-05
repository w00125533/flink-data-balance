import { Card, Descriptions, Table, Timeline } from 'antd';
import type { MigrationEvent, MovedVBucket } from '../types/observability';

export default function MigrationDiffPanel({ event }: { event: MigrationEvent }) {
  return (
    <Card title={event.migrationId} size="small">
      <Descriptions size="small" column={2}>
        <Descriptions.Item label="原因">{event.reason}</Descriptions.Item>
        <Descriptions.Item label="状态">{event.status}</Descriptions.Item>
        <Descriptions.Item label="路由版本">
          {event.routingVersionBefore} -&gt; {event.routingVersionAfter}
        </Descriptions.Item>
        <Descriptions.Item label="耗时">
          {event.startedAt} / {event.completedAt}
        </Descriptions.Item>
      </Descriptions>
      <Timeline
        style={{ marginTop: 16 }}
        items={[
          { children: '检测到负载不均衡' },
          { children: '生成 vbucket 迁移计划' },
          { children: '应用新路由版本' },
          { children: '迁移后负载稳定' }
        ]}
      />
      <Table<MovedVBucket>
        rowKey="vbucket"
        size="small"
        pagination={false}
        dataSource={event.movedVbuckets}
        columns={[
          { title: 'VBucket', dataIndex: 'vbucket' },
          { title: 'From', dataIndex: 'fromSubtask' },
          { title: 'To', dataIndex: 'toSubtask' },
          { title: '预计 EPS', dataIndex: 'estimatedEps' },
          { title: '热点 Key', dataIndex: 'hotKeys', render: (keys: string[]) => keys.join(', ') }
        ]}
      />
    </Card>
  );
}
