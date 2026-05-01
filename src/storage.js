const fs = require('fs/promises');
const path = require('path');
const logger = require('./logger');
const config = require('./config');

const storagePath = config.storagePath;

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

async function ensureLoaded() {
  if (loaded) {
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
  if (!persistenceEnabled) {
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
  await ensureLoaded();
  return state.rooms[roomId] || null;
}

async function saveRoomVariable(roomId, variableName, value) {
  await ensureLoaded();
  if (!state.rooms[roomId]) {
    state.rooms[roomId] = {};
  }
  state.rooms[roomId][variableName] = String(value);
  await flush();
}

async function deleteRoomVariable(roomId, variableName) {
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
  await ensureLoaded();
  if (!state.rooms[roomId]) {
    state.rooms[roomId] = {};
  }
  delete state.rooms[roomId][oldName];
  state.rooms[roomId][newName] = String(value);
  await flush();
}

module.exports = {
  loadRoomVariables,
  saveRoomVariable,
  deleteRoomVariable,
  renameRoomVariable,
};
