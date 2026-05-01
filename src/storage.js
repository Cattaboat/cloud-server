const fs = require('fs/promises');
const path = require('path');
const logger = require('./logger');
const config = require('./config');

const storagePath = config.storagePath;

const upstashUrl = process.env.UPSTASH_REDIS_REST_URL || '';
const upstashToken = process.env.UPSTASH_REDIS_REST_TOKEN || '';
const useUpstash = Boolean(upstashUrl && upstashToken);

let loaded = false;
let state = {rooms: {}};
let persistenceEnabled = true;

function disablePersistence(reason) {
  if (!persistenceEnabled) {
    return;
  }
  persistenceEnabled = false;
  logger.error('Persistent storage has been disabled for this process: ' + reason);
}

function getRoomKey(roomId) {
  return `room:${roomId}:variables`;
}

async function upstashPipeline(commands) {
  const response = await fetch(upstashUrl + '/pipeline', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${upstashToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(commands),
  });

  if (!response.ok) {
    throw new Error(`Upstash request failed: ${response.status} ${response.statusText}`);
  }

  const data = await response.json();
  if (!Array.isArray(data)) {
    throw new Error('Invalid Upstash response payload');
  }
  for (const item of data) {
    if (item && item.error) {
      throw new Error('Upstash error: ' + item.error);
    }
  }
  return data.map((item) => item ? item.result : null);
}

async function upstashCommand(...command) {
  const result = await upstashPipeline([command]);
  return result[0];
}

async function ensureLoaded() {
  if (useUpstash || loaded) {
    return;
  }

  try {
    const data = await fs.readFile(storagePath, 'utf8');
    state = JSON.parse(data);
    if (!state || typeof state !== 'object' || !state.rooms || typeof state.rooms !== 'object') {
      state = {rooms: {}};
    }
    logger.info('Loaded persistent storage from ' + storagePath);
  } catch (error) {
    const readError = /** @type {NodeJS.ErrnoException} */ (error);
    if (readError && readError.code !== 'ENOENT') {
      logger.error('Could not load persistent storage: ' + readError);
    }
    state = {rooms: {}};
  }

  loaded = true;
}

async function flush() {
  if (!persistenceEnabled || useUpstash) {
    return;
  }

  const directory = path.dirname(storagePath);
  try {
    await fs.mkdir(directory, {recursive: true});
    const tempPath = storagePath + '.tmp';
    await fs.writeFile(tempPath, JSON.stringify(state), 'utf8');
    await fs.rename(tempPath, storagePath);
  } catch (error) {
    disablePersistence(error);
  }
}

async function loadRoomVariables(roomId) {
  if (useUpstash) {
    const result = await upstashCommand('HGETALL', getRoomKey(roomId));
    if (!result || !Array.isArray(result) || result.length === 0) {
      return null;
    }

    const variables = {};
    for (let i = 0; i < result.length - 1; i += 2) {
      variables[result[i]] = result[i + 1];
    }
    return variables;
  }

  await ensureLoaded();
  return state.rooms[roomId] || null;
}

async function saveRoomVariable(roomId, variableName, value) {
  if (useUpstash) {
    await upstashCommand('HSET', getRoomKey(roomId), variableName, String(value));
    return;
  }

  await ensureLoaded();
  if (!state.rooms[roomId]) {
    state.rooms[roomId] = {};
  }
  state.rooms[roomId][variableName] = String(value);
  await flush();
}

async function deleteRoomVariable(roomId, variableName) {
  if (useUpstash) {
    await upstashCommand('HDEL', getRoomKey(roomId), variableName);
    return;
  }

  await ensureLoaded();
  if (!state.rooms[roomId]) {
    return;
  }
  delete state.rooms[roomId][variableName];
  if (Object.keys(state.rooms[roomId]).length === 0) {
    delete state.rooms[roomId];
  }
  await flush();
}

async function renameRoomVariable(roomId, oldName, newName, value) {
  if (useUpstash) {
    await upstashPipeline([
      ['HSET', getRoomKey(roomId), newName, String(value)],
      ['HDEL', getRoomKey(roomId), oldName],
    ]);
    return;
  }

  await ensureLoaded();
  if (!state.rooms[roomId]) {
    state.rooms[roomId] = {};
  }
  delete state.rooms[roomId][oldName];
  state.rooms[roomId][newName] = String(value);
  await flush();
}

if (useUpstash) {
  logger.info('Using Upstash Redis REST storage backend');
}

module.exports = {
  loadRoomVariables,
  saveRoomVariable,
  deleteRoomVariable,
  renameRoomVariable,
};
