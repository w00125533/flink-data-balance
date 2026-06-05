import { Alert, Empty, Space, Typography } from 'antd';
import { useEffect, useState } from 'react';
import { fetchMigrationEvents, subscribeObservabilityEvents } from '../api/client';
import MigrationDiffPanel from '../components/MigrationDiffPanel';
import type { MigrationEvent } from '../types/observability';

export default function MigrationTimeline() {
  const [events, setEvents] = useState<MigrationEvent[]>([]);
  const [error, setError] = useState<string>();

  useEffect(() => {
    let active = true;
    fetchMigrationEvents()
      .then((nextEvents) => {
        if (active) {
          setEvents(nextEvents);
          setError(undefined);
        }
      })
      .catch((err: Error) => {
        if (active) {
          setError(err.message);
        }
      });
    const closeEvents = subscribeObservabilityEvents({
      onMigrationEvent: (nextEvents) => {
        if (active) {
          setEvents(nextEvents);
        }
      }
    });
    return () => {
      active = false;
      closeEvents();
    };
  }, []);

  return (
    <Space direction="vertical" size={16} style={{ width: '100%' }}>
      <Typography.Title level={3} style={{ margin: 0 }}>
        负载迁移时间线
      </Typography.Title>
      {error ? <Alert type="error" message="迁移事件请求失败" description={error} /> : null}
      {events.length === 0 ? <Empty description="暂无迁移事件" /> : null}
      {events.map((event) => (
        <MigrationDiffPanel key={event.migrationId} event={event} />
      ))}
    </Space>
  );
}
