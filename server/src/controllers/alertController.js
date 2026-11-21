import { formatAlert, recordLeakStart, upsertNodeAlert } from '../services/alertStore.js';
import { isPostgresConnected, pool, inMemoryStore } from '../config/db.js';
import {
  broadcastAlertResolved,
  broadcastAlertNew,
  broadcastAlertDeleted,
  broadcastNodeStatus,
  broadcastHistoryNew,
  broadcastSensorUpdate,
} from '../services/socketService.js';
import { clearLatestReadingLeak, getLatestReading } from './sensorController.js';

export async function getAlerts(req, res) {
  const { state } = req.query;

  try {
    if (isPostgresConnected && pool) {
      let queryText = "SELECT * FROM alerts WHERE state <> 'superseded'";
      const params = [];

      if (state) {
        queryText += ' AND state = $1';
        params.push(state);
      }
      queryText += ' ORDER BY created_at DESC';

      const result = await pool.query(queryText, params);
      const alerts = result.rows.map(row => ({
        id: row.id,
        node: row.node_id,
        title: row.title,
        severity: row.severity,
        soundLevel: parseFloat(row.sound_level),
        threshold: parseFloat(row.threshold),
        state: row.state,
        description: row.description,
        createdAt: row.created_at,
        resolvedAt: row.resolved_at,
      }));
      return res.json(alerts);
    }

    let alerts = [...inMemoryStore.alerts];
    if (state) {
      alerts = alerts.filter(a => a.state === state);
    }
    alerts.sort((a, b) => new Date(b.created_at) - new Date(a.created_at));

    return res.json(alerts.map(a => ({
      id: a.id,
      node: a.node_id,
      title: a.title,
      severity: a.severity,
      soundLevel: a.sound_level,
      threshold: a.threshold,
      state: a.state,
      description: a.description,
      createdAt: a.created_at,
      resolvedAt: a.resolved_at,
    })));
  } catch (err) {
    console.error('Error fetching alerts:', err);
    res.status(500).json({ error: 'Failed to fetch alerts' });
  }
}

export async function resolveAlert(req, res) {
  const { id } = req.params;

  try {
    let resolvedAlert = null;
    let targetNodeId = null;

    if (isPostgresConnected && pool) {
      const updateResult = await pool.query(
        `UPDATE alerts 
         SET state = 'resolved', resolved_at = NOW() 
         WHERE id = $1 AND state <> 'superseded'
         RETURNING *`,
        [id]
      );

      if (updateResult.rows.length === 0) {
        return res.status(404).json({ error: 'Alert not found' });
      }

      const row = updateResult.rows[0];
      targetNodeId = row.node_id;
      resolvedAlert = {
        id: row.id,
        node: row.node_id,
        title: row.title,
        severity: row.severity,
        soundLevel: parseFloat(row.sound_level),
        threshold: parseFloat(row.threshold),
        state: row.state,
        description: row.description,
        createdAt: row.created_at,
        resolvedAt: row.resolved_at,
      };

      // Check if this node has any other active alerts
      const activeCount = await pool.query(
        `SELECT COUNT(*) FROM alerts WHERE node_id = $1 AND state = 'active'`,
        [targetNodeId]
      );

      if (parseInt(activeCount.rows[0].count, 10) === 0) {
        await pool.query(
          `UPDATE nodes SET status = 'normal' WHERE id = $1`,
          [targetNodeId]
        );
        broadcastNodeStatus({ id: targetNodeId, status: 'normal' });
        clearLatestReadingLeak(targetNodeId);
        const latest = getLatestReading();
        if (latest) broadcastSensorUpdate(latest);
      }

      // Add to history
      const nowStr = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
      const historyRow = {
        id: `h${Date.now()}`,
        time: `Today, ${nowStr}`,
        node: targetNodeId,
        event: 'Leak Alert Resolved',
        status: 'normal',
        createdAt: new Date().toISOString(),
      };
      await pool.query(
        `INSERT INTO history (id, time, node, event, status, created_at)
         VALUES ($1, $2, $3, $4, $5, NOW())`,
        [historyRow.id, historyRow.time, historyRow.node, historyRow.event, historyRow.status]
      );
      broadcastHistoryNew(historyRow);
    } else {
      const alert = inMemoryStore.alerts.find(a => a.id === id);
      if (!alert) {
        return res.status(404).json({ error: 'Alert not found' });
      }

      alert.state = 'resolved';
      alert.resolved_at = new Date();
      targetNodeId = alert.node_id;

      resolvedAlert = {
        id: alert.id,
        node: alert.node_id,
        title: alert.title,
        severity: alert.severity,
        soundLevel: alert.sound_level,
        threshold: alert.threshold,
        state: alert.state,
        description: alert.description,
        createdAt: alert.created_at,
        resolvedAt: alert.resolved_at,
      };

      const hasOtherActive = inMemoryStore.alerts.some(
        a => a.node_id === targetNodeId && a.state === 'active'
      );
      if (!hasOtherActive) {
        const node = inMemoryStore.nodes.find(n => n.id === targetNodeId);
        if (node) {
          node.status = 'normal';
          broadcastNodeStatus({ id: targetNodeId, status: 'normal' });
        }
        clearLatestReadingLeak(targetNodeId);
        const latest = getLatestReading();
        if (latest) broadcastSensorUpdate(latest);
      }

      const nowStr = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
      const historyRow = {
        id: `h${Date.now()}`,
        time: `Today, ${nowStr}`,
        node: targetNodeId,
        event: 'Leak Alert Resolved',
        status: 'normal',
        created_at: new Date(),
        createdAt: new Date().toISOString(),
      };
      inMemoryStore.history.unshift(historyRow);
      broadcastHistoryNew(historyRow);
    }

    // Broadcast real-time update
    broadcastAlertResolved(resolvedAlert);

    return res.json({
      success: true,
      message: 'Alert resolved successfully',
      alert: resolvedAlert,
    });
  } catch (err) {
    console.error('Error resolving alert:', err);
    res.status(500).json({ error: 'Failed to resolve alert' });
  }
}

export async function createAlert(req, res) {
  const { node, title, severity = 'critical', soundLevel, threshold = 70, description } = req.body;

  if (!node || !soundLevel) {
    return res.status(400).json({ error: 'node and soundLevel are required' });
  }

  const alertTitle = title || (severity === 'critical' ? 'Critical Air Leak' : 'Air Leak Warning');
  const alertDesc = description || `Acoustic level reached ${soundLevel} dB exceeding ${threshold} dB`;

  try {
    const { alert, started } = await upsertNodeAlert({
      nodeId: node, title: alertTitle, severity, soundLevel, threshold, description: alertDesc,
    });
    const createdAlert = formatAlert(alert);

    if (isPostgresConnected && pool) {
      await pool.query("UPDATE nodes SET status = 'leak', sound_level = $1, last_seen = NOW() WHERE id = $2", [soundLevel, node]);
    } else {
      const nodeObj = inMemoryStore.nodes.find(n => n.id === node);
      if (nodeObj) {
        nodeObj.status = 'leak';
        nodeObj.sound_level = soundLevel;
        nodeObj.last_seen = new Date();
      }
    }

    if (started) await recordLeakStart(alert);

    broadcastAlertNew(createdAlert);
    broadcastNodeStatus({ id: node, status: 'leak', soundLevel });

    return res.status(201).json({
      success: true,
      alert: createdAlert,
    });
  } catch (err) {
    console.error('Error creating alert:', err);
    res.status(500).json({ error: 'Failed to create alert' });
  }
}

export async function deleteAlert(req, res) {
  const { id } = req.params;

  try {
    let targetNodeId = null;

    if (isPostgresConnected && pool) {
      // Get the alert first so we know the node
      const alertRes = await pool.query('SELECT * FROM alerts WHERE id = $1', [id]);
      if (alertRes.rows.length === 0) {
        return res.status(404).json({ error: 'Alert not found' });
      }

      targetNodeId = alertRes.rows[0].node_id;

      // Delete the alert
      await pool.query('DELETE FROM alerts WHERE id = $1', [id]);

      // Check if this node has any other active alerts remaining
      const activeCount = await pool.query(
        `SELECT COUNT(*) FROM alerts WHERE node_id = $1 AND state = 'active'`,
        [targetNodeId]
      );

      if (parseInt(activeCount.rows[0].count, 10) === 0) {
        await pool.query(
          `UPDATE nodes SET status = 'normal' WHERE id = $1`,
          [targetNodeId]
        );
        broadcastNodeStatus({ id: targetNodeId, status: 'normal' });
        clearLatestReadingLeak(targetNodeId);
        const latest = getLatestReading();
        if (latest) broadcastSensorUpdate(latest);
      }
    } else {
      const alertIndex = inMemoryStore.alerts.findIndex(a => a.id === id);
      if (alertIndex === -1) {
        return res.status(404).json({ error: 'Alert not found' });
      }

      targetNodeId = inMemoryStore.alerts[alertIndex].node_id;
      inMemoryStore.alerts.splice(alertIndex, 1);

      const hasOtherActive = inMemoryStore.alerts.some(
        a => a.node_id === targetNodeId && a.state === 'active'
      );
      if (!hasOtherActive) {
        const node = inMemoryStore.nodes.find(n => n.id === targetNodeId);
        if (node) {
          node.status = 'normal';
          broadcastNodeStatus({ id: targetNodeId, status: 'normal' });
        }
        clearLatestReadingLeak(targetNodeId);
        const latest = getLatestReading();
        if (latest) broadcastSensorUpdate(latest);
      }
    }

    // Broadcast alert deletion to all connected clients
    broadcastAlertDeleted(id, targetNodeId);

    return res.json({
      success: true,
      message: 'Alert deleted successfully',
    });
  } catch (err) {
    console.error('Error deleting alert:', err);
    res.status(500).json({ error: 'Failed to delete alert' });
  }
}
