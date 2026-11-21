import mqtt from 'mqtt';
import { ingestReading } from '../controllers/sensorController.js';
import { query, inMemoryStore, isPostgresConnected } from '../config/db.js';

let mqttClient = null;
let mqttConnected = false;
let ioRef = null;

export const MQTT_TOPIC = 'compressed-air/node1/data';

/**
 * Connect to HiveMQ broker and wire up message handlers.
 * Call once from server.js, passing the Socket.io instance.
 */
export function initMqtt(io) {
  ioRef = io;

  const BROKER_URL  = process.env.HIVEMQ_BROKER_URL || 'broker.hivemq.com';
  const MQTT_USER   = process.env.HIVEMQ_USERNAME || null;
  const MQTT_PASS   = process.env.HIVEMQ_PASSWORD || null;
  const MQTT_PORT   = parseInt(process.env.HIVEMQ_PORT || '1883', 10);

  const brokerUri = BROKER_URL.startsWith('mqtts://') || BROKER_URL.startsWith('mqtt://')
    ? BROKER_URL
    : `mqtt://${BROKER_URL}:${MQTT_PORT}`;

  console.log(`\n🔌 [MQTT] Connecting to HiveMQ broker: ${brokerUri}`);

  const connectOptions = {
    clientId: `compressed-air-server-${Math.random().toString(16).slice(2, 8)}`,
    clean: true,
    connectTimeout: 8000,
    reconnectPeriod: 5000,
    rejectUnauthorized: false,
  };

  // Only add credentials if provided (private broker)
  if (MQTT_USER) connectOptions.username = MQTT_USER;
  if (MQTT_PASS) connectOptions.password = MQTT_PASS;

  mqttClient = mqtt.connect(brokerUri, connectOptions);

  // ─── Connection Events ──────────────────────────────────────────────────
  mqttClient.on('connect', () => {
    mqttConnected = true;
    console.log('✅ [MQTT] Connected to HiveMQ!');

    mqttClient.subscribe(MQTT_TOPIC, { qos: 0 }, (err) => {
      if (err) {
        console.error('❌ [MQTT] Subscribe error:', err.message);
      } else {
        console.log('📡 [MQTT] Subscribed to topic:', MQTT_TOPIC);
      }
    });

    // Broadcast MQTT status to all dashboard clients
    ioRef?.emit('mqtt:status', { connected: true });
  });

  mqttClient.on('reconnect', () => {
    console.log('🔄 [MQTT] Reconnecting to HiveMQ...');
    mqttConnected = false;
    ioRef?.emit('mqtt:status', { connected: false });
  });

  mqttClient.on('offline', () => {
    console.log('⚠️  [MQTT] Client went offline.');
    mqttConnected = false;
    ioRef?.emit('mqtt:status', { connected: false });
  });

  mqttClient.on('error', (err) => {
    console.error('❌ [MQTT] Connection error:', err.message);
    mqttConnected = false;
  });

  // ─── Message Handler ────────────────────────────────────────────────────
  mqttClient.on('message', async (topic, payload) => {
    if (topic !== MQTT_TOPIC) return;

    let data;
    try {
      data = JSON.parse(payload.toString());
    } catch {
      console.warn(`[MQTT] Invalid JSON on topic "${topic}":`, payload.toString());
      return;
    }

    console.log(`[MQTT] 📨 ${topic}:`, JSON.stringify(data));

    if (data && typeof data === 'object') await handleSensorReading(data);
  });
}

// ─────────────────────────────────────────────────────────────────────────────
//  Helper: Verify if node is registered (flexible matching "Node 01", "Node 1", "node01", etc.)
// ─────────────────────────────────────────────────────────────────────────────
function normalizeNodeId(id) {
  if (!id) return '';
  const s = String(id).toLowerCase().replace(/[-_\s]/g, '');
  return s.replace(/^node0*([0-9]+)$/, 'node$1');
}

function isSameNode(id1, id2) {
  if (!id1 || !id2) return false;
  if (String(id1).toLowerCase().trim() === String(id2).toLowerCase().trim()) return true;
  return normalizeNodeId(id1) === normalizeNodeId(id2);
}

async function getRegisteredNode(nodeId) {
  if (!nodeId) return null;
  try {
    if (isPostgresConnected) {
      const res = await query('SELECT * FROM nodes');
      return res.rows.find(r => isSameNode(r.id, nodeId) || isSameNode(r.name, nodeId)) || null;
    }
    return inMemoryStore.nodes.find(n => isSameNode(n.id, nodeId) || isSameNode(n.name, nodeId)) || null;
  } catch (err) {
    console.error('[MQTT] Error checking if node is registered:', err);
    return null;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
//  HANDLER: Sensor reading  { nodeId, soundLevel, threshold, leakDetected }
// ─────────────────────────────────────────────────────────────────────────────
async function handleSensorReading(data) {
  // Support flexible node ID keys
  const rawNodeId = data.nodeId || data.node_id || data.id || data.node;

  if (!rawNodeId) {
    console.warn('[MQTT] Sensor reading missing nodeId:', data);
    return;
  }

  // Block message if node is not registered
  const matchedNode = await getRegisteredNode(rawNodeId);
  if (!matchedNode) {
    console.warn(`🛑 [MQTT] Blocked message: Node "${rawNodeId}" is not registered.`);
    return;
  }
  const nodeId = matchedNode.id;

  // Parse binary leak status from hardware (handles boolean true/false, "leak"/"normal", 1/0)
  const isLeak = data.leak !== undefined
    ? (data.leak === true || data.leak === 1 || data.leak === 'leak' || data.leak === 'true')
    : (data.leakDetected !== undefined
        ? Boolean(data.leakDetected)
        : (data.status === 'leak'));

  // If hardware does not send raw soundLevel, assign representative operational amplitude
  // (88.5 dB during a leak trigger, 54.0 dB during normal envelope)
  const soundLevel = data.soundLevel !== undefined && data.soundLevel !== null
    ? parseFloat(data.soundLevel)
    : (isLeak ? 88.5 : 54.0);

  const threshold = parseFloat(data.threshold) || 70.0;
  const leakDetected = isLeak;

  // Reuse the existing ingestReading controller
  try {
    await ingestReading({ nodeId, soundLevel, threshold, leakDetected, alert: data.alert, status: data.status });

    // Real-time push to dashboard
    ioRef?.emit('sensor:update', { nodeId, soundLevel, threshold, leakDetected, isBinarySensor: true });
  } catch (err) {
    console.error('[MQTT] Error ingesting sensor reading:', err.message);
  }
}

export { mqttConnected };
