import { randomUUID } from 'node:crypto';
import { isPostgresConnected, pool, inMemoryStore } from '../config/db.js';
import { broadcastHistoryNew } from './socketService.js';

export async function upsertNodeAlert({ nodeId, title, severity, soundLevel, threshold, description }) {
  const id = `alert_${randomUUID()}`;

  if (isPostgresConnected && pool) {
    const { rows: [alert] } = await pool.query(
      `INSERT INTO alerts (id, node_id, title, severity, sound_level, threshold, state, description, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, 'active', $7, statement_timestamp())
       ON CONFLICT ((LOWER(node_id))) WHERE state <> 'superseded'
       DO UPDATE SET title = EXCLUDED.title,
                     severity = EXCLUDED.severity,
                     sound_level = EXCLUDED.sound_level,
                     threshold = EXCLUDED.threshold,
                     description = EXCLUDED.description,
                     created_at = CASE WHEN alerts.state = 'active'
                                       THEN alerts.created_at ELSE EXCLUDED.created_at END,
                     state = 'active',
                     resolved_at = NULL
       RETURNING *, created_at = statement_timestamp() AS started`,
      [id, nodeId, title, severity, soundLevel, threshold, description]
    );
    return { alert, started: alert.started };
  }

  let alert = inMemoryStore.alerts.find((entry) => entry.node_id?.toLowerCase() === nodeId.toLowerCase());
  const started = alert?.state !== 'active';
  if (!alert) {
    alert = { id, node_id: nodeId };
    inMemoryStore.alerts.unshift(alert);
  }
  if (started) alert.created_at = new Date();
  Object.assign(alert, { title, severity, sound_level: soundLevel, threshold, description, state: 'active', resolved_at: null });
  return { alert, started };
}

export function formatAlert(row) {
  return {
    id: row.id,
    node: row.node_id,
    node_id: row.node_id,
    title: row.title,
    severity: row.severity,
    soundLevel: Number(row.sound_level),
    threshold: Number(row.threshold),
    state: row.state,
    description: row.description,
    createdAt: row.created_at,
    resolvedAt: row.resolved_at,
  };
}

export async function recordLeakStart(alert) {
  const now = new Date();
  const record = {
    id: `h_${randomUUID()}`,
    time: `Today, ${now.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}`,
    node: alert.node_id,
    event: 'Leak Event Detected',
    status: 'leak',
    createdAt: now.toISOString(),
  };
  if (isPostgresConnected && pool) {
    await pool.query(
      `INSERT INTO history (id, time, node, event, status, created_at)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [record.id, record.time, record.node, record.event, record.status, now]
    );
  } else {
    inMemoryStore.history.unshift({ ...record, created_at: now });
  }
  broadcastHistoryNew(record);
}
