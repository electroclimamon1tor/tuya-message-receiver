import { createHash } from "node:crypto";

function md5(text) {
  return createHash("md5").update(text).digest("hex");
}

function passwordTuya(clientId, secret) {
  return md5(clientId + md5(secret)).slice(8, 24);
}

function base64Bytes(text) {
  const bin = atob(text);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

async function dataToText(data) {
  if (typeof data === "string") return data;
  if (data instanceof ArrayBuffer) return new TextDecoder().decode(data);
  if (ArrayBuffer.isView(data)) {
    return new TextDecoder().decode(
      data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength)
    );
  }
  if (data?.arrayBuffer) {
    return new TextDecoder().decode(await data.arrayBuffer());
  }
  return String(data);
}

async function decryptGcm(base64Data, secret) {
  const raw = base64Bytes(base64Data);

  const iv = raw.slice(0, 12);
  const cipherAndTag = raw.slice(12);
  const keyBytes = new TextEncoder().encode(secret.slice(8, 24));

  const key = await crypto.subtle.importKey(
    "raw",
    keyBytes,
    "AES-GCM",
    false,
    ["decrypt"]
  );

  const plain = await crypto.subtle.decrypt(
    {
      name: "AES-GCM",
      iv,
      tagLength: 128,
    },
    key,
    cipherAndTag
  );

  return new TextDecoder().decode(plain);
}

function numero(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

async function ultimoEstado(env, sensorId) {
  const row = await env.DB.prepare(`
    SELECT
      temperatura_externa,
      temperatura_ambiente,
      humedad,
      bateria,
      online
    FROM mediciones
    WHERE sensor_id = ?
    ORDER BY fecha DESC
    LIMIT 1
  `).bind(sensorId).first();

  return {
    externa: row?.temperatura_externa ?? null,
    ambiente: row?.temperatura_ambiente ?? null,
    humedad: row?.humedad ?? null,
    bateria: row?.bateria ?? null,
    online: row?.online ?? 1,
  };
}

function aplicarPropiedad(state, prop) {
  if (prop.code === "temp_current") {
    const valor = numero(prop.value);
    state.ambiente = valor !== null ? valor / 10 : null;
  }

  if (prop.code === "temp_current_external") {
    const valor = numero(prop.value);
    state.externa = valor !== null ? valor / 10 : null;
  }

  if (
    prop.code === "humidity_value" ||
    prop.code === "humidity_current" ||
    prop.code === "humidity"
  ) {
    state.humedad = numero(prop.value);
  }

  if (
    prop.code === "battery_state" ||
    prop.code === "battery_percentage" ||
    prop.code === "battery_value"
  ) {
    state.bateria = String(prop.value);
  }
}

export async function guardarReporte(env, message) {
  const tipo = message?.bizCode;
const biz = message.bizData || {};
const sensorId = biz.devId;
if (!sensorId) return 0;
const identidad = await env.DB.prepare(`
  SELECT t.id, t.alias, ti.inicio
  FROM termometro_identidades ti
  JOIN termometros t ON t.id = ti.termometro_id AND t.sensor_id = ti.sensor_id
  WHERE ti.sensor_id = ? AND ti.fin IS NULL
`).bind(sensorId).first();
if (!identidad) return 0;
const nombre = identidad.alias;

if (tipo === "deviceOnline" || tipo === "deviceOffline") {
  if (!nombre) return 0;

  const estado =
    tipo === "deviceOnline" ? "online" : "offline";

  const fecha = Number(
    biz.time ||
    biz.timestamp ||
    message.ts ||
    Date.now()
  );

  if (!Number.isSafeInteger(fecha) || fecha < Number(identidad.inicio)) return 0;
  const resultado = await env.DB.prepare(`
    INSERT INTO conectividad (
      sensor_id,
      nombre,
      estado,
      fecha
    )
    SELECT ?, ?, ?, ? WHERE EXISTS (
      SELECT 1 FROM termometro_identidades
      WHERE sensor_id = ? AND fin IS NULL AND inicio <= ?
    )
  `).bind(
    sensorId,
    nombre,
    estado,
    fecha,
    sensorId,
    fecha
  ).run();

  return Number(resultado.meta?.changes || 0);
}

if (tipo !== "devicePropertyMessage") return 0;

  if (!nombre || !Array.isArray(biz.properties)) return 0;

  const grupos = new Map();

  for (const prop of biz.properties) {
    const t = Number(prop.time || message.ts || Date.now());

    if (!grupos.has(t)) grupos.set(t, []);
    grupos.get(t).push(prop);
  }

  const state = await ultimoEstado(env, sensorId);
  let guardadas = 0;

  const ordenados = [...grupos.entries()]
    .sort((a, b) => a[0] - b[0]);

  for (const [fecha, props] of ordenados) {
    if (!Number.isSafeInteger(fecha) || fecha < Number(identidad.inicio)) continue;
    for (const prop of props) {
      aplicarPropiedad(state, prop);
    }

    const resultado = await env.DB.prepare(`
      INSERT INTO mediciones (
        sensor_id,
        nombre,
        temperatura_externa,
        temperatura_ambiente,
        humedad,
        bateria,
        online,
        fecha
      )
      SELECT ?, ?, ?, ?, ?, ?, ?, ? WHERE EXISTS (
        SELECT 1 FROM termometro_identidades
        WHERE sensor_id = ? AND fin IS NULL AND inicio <= ?
      ) AND NOT EXISTS (
        SELECT 1 FROM mediciones WHERE sensor_id = ? AND fecha = ?
      )
    `).bind(
      sensorId,
      nombre,
      state.externa,
      state.ambiente,
      state.humedad,
      state.bateria,
      state.online,
      fecha,
      sensorId, fecha, sensorId, fecha
    ).run();

    guardadas += Number(resultado.meta?.changes || 0);
  }

  return guardadas;
}

async function procesarFrame(env, rawFrame) {
  const outer = JSON.parse(await dataToText(rawFrame));

  const payloadText = new TextDecoder().decode(
    base64Bytes(outer.payload)
  );

  const envelope = JSON.parse(payloadText);

  const encryption =
    outer?.properties?.em || "aes_ecb";

  if (encryption !== "aes_gcm") {
    throw new Error(
      `Cifrado inesperado: ${encryption}`
    );
  }

  const decrypted = await decryptGcm(
    envelope.data,
    env.TUYA_CLIENT_SECRET
  );

  const message = JSON.parse(decrypted);

  const saved = await guardarReporte(
    env,
    message
  );

  return {
    messageId: outer.messageId,
    saved,
  };
}

async function consumirCola(env) {
  const id = env.TUYA_CLIENT_ID;
  const secret = env.TUYA_CLIENT_SECRET;

  const endpoint =
    env.TUYA_MQ_ENDPOINT.replace(/\/+$/, "");

  const subscription = `${id}-sub`;

  const url =
    `${endpoint}/ws/v2/consumer/persistent/` +
    `${id}/out/event/${subscription}` +
    `?ackTimeoutMillis=3000&subscriptionType=Failover`;

  const response = await fetch(url, {
    headers: {
      Upgrade: "websocket",
      username: id,
      password: passwordTuya(id, secret),
    },
  });

  const ws = response.webSocket;

  if (!ws) {
    throw new Error(
      `Tuya rechazó WebSocket: HTTP ${response.status}`
    );
  }

  ws.accept();

  let received = 0;
  let saved = 0;
  const errors = [];

  let chain = Promise.resolve();
  let finishing = false;

  let idleTimer;
  let hardTimer;
  let resolveDone;

  const done = new Promise(resolve => {
    resolveDone = resolve;
  });

  const finish = async () => {
    if (finishing) return;

    finishing = true;

    clearTimeout(idleTimer);
    clearTimeout(hardTimer);

    await chain;

    try {
      ws.close(1000, "done");
    } catch {}

    resolveDone();
  };

  const resetIdle = () => {
    clearTimeout(idleTimer);

    idleTimer = setTimeout(
      () => void finish(),
      2500
    );
  };

  ws.addEventListener("message", event => {
    if (finishing) return;

    received++;
    resetIdle();

    chain = chain.then(async () => {
      try {
        const result =
          await procesarFrame(env, event.data);

        saved += result.saved;

        if (result.messageId) {
          ws.send(JSON.stringify({
            messageId: result.messageId
          }));
        }

      } catch (error) {
        errors.push(
          String(error?.message || error)
        );
      }
    });
  });

  ws.addEventListener("error", () => {
    errors.push("Error de WebSocket");
    void finish();
  });

  ws.addEventListener("close", () => {
    void finish();
  });

  resetIdle();

  hardTimer = setTimeout(
    () => void finish(),
    10000
  );

  await done;

  return {
    ok: errors.length === 0,
    received,
    saved,
    errors,
  };
}

export default {

  async fetch(request, env) {
    try {
      const result =
        await consumirCola(env);

      return Response.json(result, {
        headers: {
          "cache-control": "no-store"
        }
      });

    } catch (error) {
      return Response.json(
        {
          ok: false,
          error: String(
            error?.message || error
          )
        },
        {
          status: 500,
          headers: {
            "cache-control": "no-store"
          }
        }
      );
    }
  },

  async scheduled(event, env, ctx) {
    ctx.waitUntil(
      consumirCola(env)
    );
  },

};
