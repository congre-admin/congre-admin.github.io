/**
 * Congre-Admin Backend API
 * Zero-Knowledge congregation management system.
 * Thin, stateless API layer: low-level DB ops + batch orchestration + auth.
 * No script properties for app state — all state passed explicitly via ssId.
 */

// ================================================================= //
// ENTRY POINTS
// ================================================================= //

/**
 * Handles GET requests (public read-only).
 * @example ?action=getData&sheet=NombreDeHoja&ssId=ID_DE_HOJA
 */
function doGet(e) {
  const action = e.parameter.action;
  const ssId = e.parameter.ssId;

  try {
    const ss = ssId ? SpreadsheetApp.openById(ssId) : SpreadsheetApp.getActiveSpreadsheet();

    if (action === 'getData') {
      return createResponse(getCachedSheetData(ss, e.parameter.sheet));
    }

    return createResponse({ error: 'Acción GET no válida' });
  } catch (err) {
    return createResponse({ error: err.message });
  }
}

/**
 * Handles POST requests.
 * Dispatch map pattern — all config from GSheet, no script properties for app state.
 */
function doPost(e) {
  try {
    const postData = JSON.parse(e.postData.contents);
    const action = postData.action;
    const payload = postData.payload || {};
    const sessionToken = payload.sessionToken || postData.sessionToken;
    const sheetName = payload.sheet || postData.sheet;
    const ssId = payload.ssId || postData.ssId;
    const coreSsId = payload.coreSsId || ssId;
    const module = payload.module || null;
    const mode = payload.mode || 'admin'; // 'admin' or 'public'

    // --- Auth actions (no ssId required) ---
    const session = sessionToken ? validateSession(sessionToken, ssId) : null;
    const authActions = {
      login: () => actionLogin(payload, ssId),
      register: () => actionRegister(payload, ssId, session),
      challenge: () => actionChallenge(payload, ssId),
      requestOTP: () => actionRequestOTP(payload, ssId),
      setupTOTP: () => actionSetupTOTP(payload, ssId),
      confirmTOTP: () => actionConfirmTOTP(payload, ssId),
      setupPasskey: () => actionSetupPasskey(payload, ssId),
      confirmPasskey: () => actionConfirmPasskey(payload, ssId),
      deletePasskey: () => actionDeletePasskey(payload, ssId),
      changePassword: () => actionChangePassword(payload, sessionToken, ssId),
      requestPasswordReset: () => actionRequestPasswordReset(payload, ssId),
      confirmPasswordReset: () => actionConfirmPasswordReset(payload, sessionToken, ssId),
      getAuthMethods: () => actionGetAuthMethods(payload, sessionToken, ssId),
      setDefaultAuthMethod: () => actionSetDefaultAuthMethod(payload, sessionToken, ssId),
      validateSession: () => {
        const s = validateSession(sessionToken, ssId);
        return { valid: s.valid, userId: s.userId };
      },
      confirmAction: () => actionConfirmAction(payload, sessionToken, ssId),
      refreshSession: () => refreshSessionToken(payload.sessionToken),
      logout: () => { invalidateSession(payload.sessionToken); return { success: true }; },
    };

    if (authActions[action]) return createResponse(authActions[action]());

    // --- Install (no ssId required) ---
    if (action === 'install') return createResponse(actionInstall(payload));

    // --- File actions (require session) ---
    const fileActions = {
      listFolderFiles: () => actionListFolderFiles(payload, sessionToken, ssId),
      uploadFile: () => actionUploadFile(payload, sessionToken, ssId),
      downloadFile: () => actionDownloadFile(payload, sessionToken, ssId),
      deleteFile: () => actionDeleteFile(payload, sessionToken, ssId),
      setFileSharing: () => actionSetFileSharing(payload, sessionToken, ssId),
      moveFileToFolder: () => actionMoveFileToFolder(payload, sessionToken, ssId),
    };

    if (fileActions[action]) return createResponse(fileActions[action]());

    // --- Data actions (require ssId) ---
    if (!ssId) return createResponse({ error: 'ERR_SS_ID_REQUIRED: Se requiere ssId para operaciones de datos' });

    const ss = SpreadsheetApp.openById(ssId);

    const dataActions = {
      getData: () => dataActionGetData(session, ss, ssId, sheetName, module, mode),
      batchExecute: () => batchExecute(session, ss, ssId, payload, module, mode),
      initSheet: () => dataActionInitSheet(session, ss, ssId, sheetName, module, postData),
      clearSheet: () => dataActionClearSheet(session, ss, ssId, sheetName, module),
      saveData: () => dataActionSaveData(session, ss, ssId, sheetName, module, payload, postData),
      deleteData: () => dataActionDeleteData(session, ss, ssId, sheetName, module, payload),
      hardDelete: () => dataActionHardDelete(session, ss, ssId, sheetName, module, payload),
      restoreData: () => dataActionRestoreData(session, ss, ssId, sheetName, module, payload),
    };

    if (dataActions[action]) return createResponse(dataActions[action]());

    // --- Admin actions (require admin permission on 'core') ---
    const adminActions = {
      getUsers: () => actionGetUsers(session, ssId),
      createUser: () => actionCreateUser(session, payload, ssId),
      updateUser: () => actionUpdateUser(session, payload, ssId),
      deleteUser: () => actionDeleteUser(session, payload, ssId),
      getPerfiles: () => actionGetPerfiles(session, ssId),
      createProfile: () => actionCreateProfile(session, payload, ssId),
      updateProfile: () => actionUpdateProfile(session, payload, ssId),
      deleteProfile: () => actionDeleteProfile(session, payload, ssId),
    };

    if (adminActions[action]) return createResponse(adminActions[action]());

    return createResponse({ error: 'Acción POST no válida: ' + action });
  } catch (err) {
    return createResponse({ error: err.message });
  }
}

// ================================================================= //
// BATCH EXECUTE — Unified Batch Orchestrator
// ================================================================= //

const BATCH_MAX_OPS = 50;

/**
 * Executes multiple operations in a single API call.
 * Sheet ops: read, readById, save, delete, hardDelete, restore, initSheet.
 * File ops: uploadFile, downloadFile, listFolderFiles, deleteFile, setFileSharing, moveFileToFolder.
 * Modes: "continue" (all ops, partial success) or "fail-fast" (stop on first error).
 */
function batchExecute(session, ss, ssId, payload, module, accessMode) {
  const operations = payload.operations || [];
  const execMode = payload.mode || 'continue'; // 'continue' or 'fail-fast'
  const isPublic = accessMode === 'public';
  
  // Server-side setup detection: only allow setup bypass if no users exist yet
  const isServerSetup = !_hasExistingUsers(ssId);
  const isSetup = isServerSetup || (payload.isSetup === true); // Accept legacy flag for backwards compat, but prefer server detection

  // Plugin auth enforcement: check if module requires auth
  let pluginAuthRequired = false;
  if (module && module !== 'core' && !isServerSetup) {
    try {
      const pluginData = getCachedSheetData(ss, 'Registro_Plugins');
      const plugin = pluginData.find(p => p.plugin_id === module);
      if (plugin && plugin.auth_required === true) {
        pluginAuthRequired = true;
      }
    } catch (e) {
      // Registro_Plugins may not exist or accessible - fall back to session check
    }
  }
  
  // If plugin requires auth, session MUST be valid (even for read operations)
  if (pluginAuthRequired && (!session || !session.valid)) {
    return { success: false, error: 'ERR_PLUGIN_AUTH_REQUIRED: Este plugin requiere autenticación' };
  }

  if (!operations.length) return { success: false, error: 'ERR_BATCH_EMPTY: No operations provided' };
  if (operations.length > BATCH_MAX_OPS) return { success: false, error: 'ERR_BATCH_TOO_LARGE: Max ' + BATCH_MAX_OPS + ' operations per call' };

  // Public mode: only read ops allowed, filter is_public rows, strip enc_ fields
  if (isPublic) {
    const writeOps = ['save', 'delete', 'hardDelete', 'restore', 'initSheet', 'clearSheet', 'uploadFile', 'deleteFile', 'setFileSharing', 'moveFileToFolder'];
    for (var i = 0; i < operations.length; i++) {
      if (writeOps.includes(operations[i].op)) {
        return { success: false, error: 'ERR_PUBLIC_READONLY: Public mode only allows read operations', failedAt: i };
      }
    }
  }

  // Setup mode: allow only initSheet and save, no session required (ONLY if server confirms no users)
  if (isServerSetup) {
    const safeOps = ['initSheet', 'save'];
    for (var i = 0; i < operations.length; i++) {
      if (safeOps.indexOf(operations[i].op) === -1) {
        return { success: false, error: 'ERR_SETUP_INVALID_OP: Only initSheet and save allowed in setup mode', failedAt: i };
      }
    }
  }

  // RBAC pre-check for write operations (skipped only in true server-side setup mode)
  if (!isServerSetup) {
    for (let i = 0; i < operations.length; i++) {
      const op = operations[i];
      if (['save', 'delete', 'hardDelete', 'restore', 'initSheet', 'uploadFile', 'deleteFile', 'setFileSharing', 'moveFileToFolder'].includes(op.op)) {
        if (!session || !session.valid) return { success: false, error: 'ERR_AUTH_REQUIRED', failedAt: i };
        if (['uploadFile', 'deleteFile', 'setFileSharing', 'moveFileToFolder'].includes(op.op)) {
          const permCheck = checkPermission(session, 'write', 'core', ssId, module);
          if (!permCheck.allowed) return { success: false, error: permCheck.error, failedAt: i };
        } else {
          const permCheck = checkPermission(session, 'write', op.sheet, ssId, module);
          if (!permCheck.allowed) return { success: false, error: permCheck.error, failedAt: i };
        }
      } else if (op.op === 'read' || op.op === 'readById') {
        if (session && session.valid) {
          const permCheck = checkPermission(session, 'read', op.sheet, ssId, module);
          if (!permCheck.allowed) return { success: false, error: permCheck.error, failedAt: i };
        }
      } else if (op.op === 'downloadFile' || op.op === 'listFolderFiles') {
        if (session && session.valid) {
          const permCheck = checkPermission(session, 'read', 'core', ssId, module);
          if (!permCheck.allowed) return { success: false, error: permCheck.error, failedAt: i };
        }
      }
    }
  }

  // Intra-batch sheet cache: { "SheetName": { sheet, headers, rows, dirty } }
  const sheetCache = {};
  const results = [];
  let succeeded = 0;
  let failed = 0;

  for (let i = 0; i < operations.length; i++) {
    const op = operations[i];
    const result = { index: i, op: op.op, sheet: op.sheet };

    // Skip if fail-fast mode and previous op failed
    if (execMode === 'fail-fast' && failed > 0) {
      result.success = false;
      result.error = 'ERR_SKIPPED: Previous operation failed';
      results.push(result);
      continue;
    }

    try {
      switch (op.op) {
        case 'read': {
          const cached = getBatchSheet(ss, op.sheet, sheetCache);
          if (!cached) { result.success = false; result.error = 'Hoja no encontrada'; break; }
          let data = cached.rows.map(row => {
            const obj = {};
            cached.headers.forEach((h, j) => {
              let val = row[j];
              if (typeof val === 'string' && (val.startsWith('[') || val.startsWith('{'))) {
                try { val = JSON.parse(val); } catch (e) {}
              }
              obj[h] = val;
            });
            return obj;
          }).filter(row => row._deleted !== true && row._deleted !== 'true');
          // Public mode: filter is_public rows and strip enc_ fields
          if (isPublic) {
            data = data.filter(row => row.is_public !== false && row.is_public !== 'false' && row.is_public !== 'NO');
            data = data.map(row => {
              const clean = {};
              Object.keys(row).forEach(key => {
                if (!key.startsWith('enc_')) clean[key] = row[key];
              });
              return clean;
            });
          }
          result.success = true;
          result.data = op.filter ? data.filter(r => matchFilter(r, op.filter)) : data;
          break;
        }

        case 'readById': {
          const cached = getBatchSheet(ss, op.sheet, sheetCache);
          if (!cached) { result.success = false; result.error = 'Hoja no encontrada'; break; }
          const idIndex = cached.headers.indexOf('id');
          const row = cached.rows.find(r => r[idIndex] == op.id);
          if (!row) { result.success = false; result.error = 'Registro no encontrado'; break; }
          const obj = {};
          cached.headers.forEach((h, j) => {
            let val = row[j];
            if (typeof val === 'string' && (val.startsWith('[') || val.startsWith('{'))) {
              try { val = JSON.parse(val); } catch (e) {}
            }
            obj[h] = val;
          });
          result.success = true;
          result.data = obj;
          break;
        }

        case 'save': {
          const cached = getBatchSheet(ss, op.sheet, sheetCache);
          if (!cached) { result.success = false; result.error = 'Hoja no encontrada'; break; }
          const idIndex = cached.headers.indexOf('id');
          const keyIndex = idIndex >= 0 ? idIndex : 0;
          const keyColumn = cached.headers[keyIndex];
          const vIndex = cached.headers.indexOf('_v');
          const tsIndex = cached.headers.indexOf('_ts');
          const data = op.data;

          let rowIndex = -1;
          let currentV = 0;
          for (let r = 0; r < cached.rows.length; r++) {
            if (cached.rows[r][keyIndex] == data[keyColumn]) {
              rowIndex = r;
              currentV = parseInt(cached.rows[r][vIndex]) || 0;
              break;
            }
          }

          const timestamp = new Date().toISOString();
          const values = cached.headers.map(h => {
            const val = data[h] !== undefined ? data[h] : (rowIndex >= 0 ? cached.rows[rowIndex][cached.headers.indexOf(h)] : '');
            if (val === undefined || val === null) return '';
            return (typeof val === 'object') ? JSON.stringify(val) : val;
          });
          if (vIndex >= 0) values[vIndex] = currentV + 1;
          if (tsIndex >= 0) values[tsIndex] = timestamp;

          if (rowIndex >= 0) {
            cached.rows[rowIndex] = values;
          } else {
            cached.rows.push(values);
          }
          cached.dirty = true;
          result.success = true;
          break;
        }

        case 'delete': {
          const cached = getBatchSheet(ss, op.sheet, sheetCache);
          if (!cached) { result.success = false; result.error = 'Hoja no encontrada'; break; }
          const idIndex = cached.headers.indexOf('id');
          const keyIndex = idIndex >= 0 ? idIndex : 0;
          const deletedIndex = cached.headers.indexOf('_deleted');
          const vIndex = cached.headers.indexOf('_v');
          const tsIndex = cached.headers.indexOf('_ts');
          let found = false;
          for (let r = 0; r < cached.rows.length; r++) {
            if (cached.rows[r][keyIndex] == op.id) {
              if (deletedIndex >= 0) cached.rows[r][deletedIndex] = true;
              if (vIndex >= 0) cached.rows[r][vIndex] = (parseInt(cached.rows[r][vIndex]) || 0) + 1;
              if (tsIndex >= 0) cached.rows[r][tsIndex] = new Date().toISOString();
              cached.dirty = true;
              found = true;
              break;
            }
          }
          result.success = found;
          if (!found) result.error = 'Registro no encontrado';
          break;
        }

        case 'hardDelete': {
          const cached = getBatchSheet(ss, op.sheet, sheetCache);
          if (!cached) { result.success = false; result.error = 'Hoja no encontrada'; break; }
          const idIndex = cached.headers.indexOf('id');
          const keyIndex = idIndex >= 0 ? idIndex : 0;
          let found = false;
          for (let r = 0; r < cached.rows.length; r++) {
            if (cached.rows[r][keyIndex] == op.id) {
              cached.rows.splice(r, 1);
              cached.dirty = true;
              found = true;
              break;
            }
          }
          result.success = found;
          if (!found) result.error = 'Registro no encontrado';
          break;
        }

        case 'restore': {
          const cached = getBatchSheet(ss, op.sheet, sheetCache);
          if (!cached) { result.success = false; result.error = 'Hoja no encontrada'; break; }
          const idIndex = cached.headers.indexOf('id');
          const keyIndex = idIndex >= 0 ? idIndex : 0;
          const deletedIndex = cached.headers.indexOf('_deleted');
          let found = false;
          for (let r = 0; r < cached.rows.length; r++) {
            if (cached.rows[r][keyIndex] == op.id) {
              if (deletedIndex >= 0) cached.rows[r][deletedIndex] = false;
              cached.dirty = true;
              found = true;
              break;
            }
          }
          result.success = found;
          if (!found) result.error = 'Registro no encontrado';
          break;
        }

        case 'initSheet': {
          let sheet = ss.getSheetByName(op.sheet);
          if (!sheet) {
            sheet = ss.insertSheet(op.sheet);
            sheet.appendRow(op.headers);
            sheet.getRange(1, 1, 1, op.headers.length).setFontWeight('bold').setBackground('#f3f3f3');
          } else if (!op.preserveExisting) {
            sheet.clearContents();
            sheet.appendRow(op.headers);
            sheet.getRange(1, 1, 1, op.headers.length).setFontWeight('bold').setBackground('#f3f3f3');
          } else if (sheet.getLastRow() === 0) {
            sheet.appendRow(op.headers);
            sheet.getRange(1, 1, 1, op.headers.length).setFontWeight('bold').setBackground('#f3f3f3');
          }
          sheetCache[op.sheet] = { sheet, headers: op.headers, rows: [], dirty: false };
          result.success = true;
          break;
        }

        // --- File operations ---

        case 'uploadFile': {
          const uploadResult = _batchUploadFile(payload.folderId, op);
          if (uploadResult.error) { result.success = false; result.error = uploadResult.error; }
          else { result.success = true; result.data = uploadResult; }
          break;
        }

        case 'downloadFile': {
          if (!op.fileId) { result.success = false; result.error = 'ERR_INVALID_REQUEST: FileId is required'; break; }
          try {
            const file = DriveApp.getFileById(op.fileId);
            const blob = file.getBlob();
            const bytes = blob.getBytes();
            result.success = true;
            result.data = { fileName: file.getName(), mimeType: file.getMimeType(), size: bytes.length, content: Utilities.base64Encode(bytes) };
          } catch (e) { result.success = false; result.error = 'ERR_FILE_NOT_FOUND: ' + e.message; }
          break;
        }

        case 'listFolderFiles': {
          const targetFolder = resolveTargetFolder(payload.folderId, op.subfolder);
          if (!targetFolder) { result.success = false; result.error = op.subfolder ? 'ERR_SUBFOLDER_NOT_FOUND' : 'ERR_FOLDER_NOT_FOUND'; break; }
          const files = targetFolder.getFiles();
          const fileList = [];
          while (files.hasNext()) {
            const f = files.next();
            fileList.push({ id: f.getId(), name: f.getName(), mimeType: f.getMimeType(), size: f.getSize(), created: f.getDateCreated().toISOString(), modified: f.getLastUpdated().toISOString(), url: f.getUrl(), shared: f.getSharingAccess() !== DriveApp.Access.PRIVATE, access: f.getSharingAccess(), permission: f.getSharingPermission() });
          }
          result.success = true;
          result.data = { files: fileList };
          break;
        }

        case 'deleteFile': {
          if (!op.fileId) { result.success = false; result.error = 'ERR_INVALID_REQUEST: FileId is required'; break; }
          try { DriveApp.getFileById(op.fileId).setTrashed(true); result.success = true; }
          catch (e) { result.success = false; result.error = 'ERR_FILE_NOT_FOUND: ' + e.message; }
          break;
        }

        case 'setFileSharing': {
          if (!op.fileId) { result.success = false; result.error = 'ERR_INVALID_REQUEST: FileId is required'; break; }
          const accessMap = { 'PRIVATE': DriveApp.Access.PRIVATE, 'ANYONE_WITH_LINK': DriveApp.Access.ANYONE_WITH_LINK, 'DOMAIN': DriveApp.Access.DOMAIN, 'ANYONE': DriveApp.Access.ANYONE };
          const permissionMap = { 'VIEW': DriveApp.Permission.VIEW, 'COMMENT': DriveApp.Permission.COMMENT, 'EDIT': DriveApp.Permission.EDIT };
          const access = accessMap[op.access];
          const permission = permissionMap[op.permission || 'VIEW'];
          if (!access) { result.success = false; result.error = 'ERR_INVALID_REQUEST: Invalid access level'; break; }
          if (!permission) { result.success = false; result.error = 'ERR_INVALID_REQUEST: Invalid permission'; break; }
          try {
            const file = DriveApp.getFileById(op.fileId);
            file.setSharing(access, permission);
            result.success = true;
            result.data = { fileId: file.getId(), access: op.access, permission: op.permission || 'VIEW', shareUrl: file.getUrl() + '?usp=sharing' };
          } catch (e) { result.success = false; result.error = 'ERR_FILE_NOT_FOUND: ' + e.message; }
          break;
        }

        case 'moveFileToFolder': {
          if (!op.fileId) { result.success = false; result.error = 'ERR_INVALID_REQUEST: FileId is required'; break; }
          const moveTarget = resolveTargetFolder(payload.folderId, op.subfolder);
          if (!moveTarget) { result.success = false; result.error = op.subfolder ? 'ERR_SUBFOLDER_NOT_FOUND' : 'ERR_FOLDER_NOT_FOUND'; break; }
          try {
            const file = DriveApp.getFileById(op.fileId);
            file.moveTo(moveTarget);
            result.success = true;
            result.data = { fileId: file.getId(), fileName: file.getName(), folderId: moveTarget.getId(), fileUrl: file.getUrl() };
          } catch (e) { result.success = false; result.error = 'ERR_FILE_NOT_FOUND: ' + e.message; }
          break;
        }

        default:
          result.success = false;
          result.error = 'ERR_UNKNOWN_OP: ' + op.op;
      }
    } catch (err) {
      result.success = false;
      result.error = err.message;
    }

    if (result.success) succeeded++; else failed++;
    results.push(result);
  }

  // Flush all dirty sheets
  for (const sheetName in sheetCache) {
    const cached = sheetCache[sheetName];
    if (cached && cached.dirty) {
      const allRows = [cached.headers, ...cached.rows];
      cached.sheet.getRange(1, 1, allRows.length, allRows[0].length).setValues(allRows);
    }
  }

  return {
    success: failed === 0,
    results: results,
    totalOps: operations.length,
    succeeded: succeeded,
    failed: failed,
  };
}

/**
 * Gets or creates a cached sheet representation for batch operations.
 */
function getBatchSheet(ss, sheetName, sheetCache) {
  if (sheetCache[sheetName]) return sheetCache[sheetName];
  const sheet = ss.getSheetByName(sheetName);
  if (!sheet) return null;
  const data = sheet.getDataRange().getValues();
  sheetCache[sheetName] = { sheet, headers: data[0], rows: data.slice(1), dirty: false };
  return sheetCache[sheetName];
}

/**
 * Simple key-value filter matching for batch read operations.
 */
function matchFilter(row, filter) {
  for (const key in filter) {
    if (row[key] !== filter[key]) return false;
  }
  return true;
}

// ================================================================= //
// DATA ACTIONS (thin wrappers with session + RBAC validation)
// ================================================================= //

function dataActionGetData(session, ss, ssId, sheetName, module, mode) {
  // Public mode: no session required, filter is_public rows, strip enc_ fields
  if (mode === 'public') {
    const allData = getCachedSheetData(ss, sheetName);
    const filtered = allData
      .filter(row => row.is_public !== false && row.is_public !== 'false' && row.is_public !== 'NO')
      .map(row => {
        const clean = {};
        Object.keys(row).forEach(key => {
          if (!key.startsWith('enc_')) clean[key] = row[key];
        });
        return clean;
      });
    return { success: true, data: filtered };
  }
  // Admin mode: validate session and permissions
  if (session && session.valid) {
    const permCheck = checkPermission(session, 'read', sheetName, ssId, module);
    if (!permCheck.allowed) return { error: permCheck.error };
  }
  return { success: true, data: getCachedSheetData(ss, sheetName) };
}

function dataActionInitSheet(session, ss, ssId, sheetName, module, postData) {
  if (!session || !session.valid) return { error: 'ERR_AUTH_REQUIRED' };
  const permCheck = checkPermission(session, 'write', sheetName, ssId, module);
  if (!permCheck.allowed) return { error: permCheck.error };

  let sheet = ss.getSheetByName(sheetName);
  if (!sheet) {
    sheet = ss.insertSheet(sheetName);
    sheet.appendRow(postData.headers);
  } else if (!postData.preserveExisting) {
    sheet.clearContents();
    sheet.getRange(1, 1, 1, postData.headers.length).setValues([postData.headers]).setFontWeight('bold').setBackground('#f3f3f3');
  } else if (sheet.getLastRow() === 0) {
    sheet.getRange(1, 1, 1, postData.headers.length).setValues([postData.headers]).setFontWeight('bold').setBackground('#f3f3f3');
  }
  return { success: true, message: 'Hoja inicializada' };
}

function dataActionClearSheet(session, ss, ssId, sheetName, module) {
  if (!session || !session.valid) return { error: 'ERR_AUTH_REQUIRED' };
  const permCheck = checkPermission(session, 'write', sheetName, ssId, module);
  if (!permCheck.allowed) return { error: permCheck.error };

  const sheet = ss.getSheetByName(sheetName);
  if (!sheet) return { error: 'Hoja no encontrada' };
  const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues();
  sheet.clearContents();
  if (headers.length > 0 && headers[0][0]) sheet.appendRow(headers[0]);
  return { success: true };
}

function dataActionSaveData(session, ss, ssId, sheetName, module, payload, postData) {
  if (!session || !session.valid) return { error: 'ERR_AUTH_REQUIRED' };
  const permCheck = checkPermission(session, 'write', sheetName, ssId, module);
  if (!permCheck.allowed) return { error: permCheck.error };

  const sheet = ss.getSheetByName(sheetName);
  if (!sheet) return { error: 'Hoja no encontrada: ' + sheetName };
  let existingRows = null;
  if (postData.expectedVersion !== undefined) {
    existingRows = sheet.getDataRange().getValues();
  }
  updateOrInsert(sheet, payload, false, { existingRows });

  // Auto-sync Configuracion to public sheet
  if (sheetName === 'Configuracion') {
    syncConfigToPublic(ss);
  }

  return { success: true, message: 'Datos guardados' };
}

/**
 * Syncs public settings (is_public=true) from Configuracion to public spreadsheet.
 */
function syncConfigToPublic(ss) {
  // Get public spreadsheet ID
  const configData = getCachedSheetData(ss, 'Configuracion');
  const publicSsRow = configData.find(r => r.clave === 'ss_publico');
  if (!publicSsRow || !publicSsRow.valor) return; // No public SS configured
  const publicSsId = publicSsRow.valor;

  // Open public spreadsheet
  let publicSs;
  try {
    publicSs = SpreadsheetApp.openById(publicSsId);
  } catch (e) {
    return;
  }

  // Get public rows (is_public=true)
  const publicRows = configData.filter(r => {
    return r.is_public !== false && r.is_public !== 'false' && r.is_public !== 'NO';
  });

  if (publicRows.length === 0) return;

  // Strip enc_ fields and prepare
  const cleanRows = publicRows.map(row => {
    const clean = {};
    for (const key in row) {
      if (!key.startsWith('enc_')) clean[key] = row[key];
    }
    return clean;
  });

  // Get or create Configuracion sheet in public spreadsheet
  let targetSheet = publicSs.getSheetByName('Configuracion');
  const headers = ['clave', 'valor', 'is_public', '_v', '_ts', '_deleted'];
  if (!targetSheet) {
    targetSheet = publicSs.insertSheet('Configuracion');
    targetSheet.getRange(1, 1, 1, headers.length).setValues([headers]).setFontWeight('bold').setBackground('#f3f3f3');
  }

  // Clear and rewrite all public config
  const lastRow = targetSheet.getLastRow();
  if (lastRow > 1) {
    targetSheet.getRange(2, 1, lastRow - 1, headers.length).clearContent();
  }

  // Write clean rows
  const values = cleanRows.map(row => {
    return headers.map(h => {
      const val = row[h];
      if (val === undefined || val === null) return '';
      return (typeof val === 'object') ? JSON.stringify(val) : val;
    });
  });

  if (values.length > 0) {
    targetSheet.getRange(2, 1, values.length, headers.length).setValues(values);
  }
}

function dataActionDeleteData(session, ss, ssId, sheetName, module, payload) {
  if (!session || !session.valid) return { error: 'ERR_AUTH_REQUIRED' };
  const permCheck = checkPermission(session, 'write', sheetName, ssId, module);
  if (!permCheck.allowed) return { error: permCheck.error };

  const sheet = ss.getSheetByName(sheetName);
  if (!sheet) return { error: 'Hoja no encontrada' };
  const result = softDeleteRow(sheet, payload.id);
  if (!result) return { success: false, error: 'Registro no encontrado' };
  return { success: true, message: 'Borrado lógico realizado' };
}

function dataActionHardDelete(session, ss, ssId, sheetName, module, payload) {
  if (!session || !session.valid) return { error: 'ERR_AUTH_REQUIRED' };
  const permCheck = checkPermission(session, 'write', sheetName, ssId, module);
  if (!permCheck.allowed) return { error: permCheck.error };

  const sheet = ss.getSheetByName(sheetName);
  if (!sheet) return { error: 'Hoja no encontrada' };
  deleteRowById(sheet, payload.id);
  return { success: true, message: 'Borrado físico realizado' };
}

function dataActionRestoreData(session, ss, ssId, sheetName, module, payload) {
  if (!session || !session.valid) return { error: 'ERR_AUTH_REQUIRED' };
  const permCheck = checkPermission(session, 'write', sheetName, ssId, module);
  if (!permCheck.allowed) return { error: permCheck.error };

  const sheet = ss.getSheetByName(sheetName);
  if (!sheet) return { error: 'Hoja no encontrada' };
  const result = restoreRow(sheet, payload.id);
  if (!result) return { success: false, error: 'Registro no encontrado' };
  return { success: true, message: 'Registro restaurado' };
}

// ================================================================= //
// CACHING SYSTEM
// ================================================================= //

const CACHE_TTL_DATA = 600; // 10 minutes
const CACHE_TTL_LOOKUP = 300; // 5 minutes

function getCachedSheetData(ss, sheetName) {
  Logger.log('getCachedSheetData: ssId=' + ss.getId() + ', sheetName=' + sheetName);
  const sheet = ss.getSheetByName(sheetName);
  Logger.log('getCachedSheetData: sheet=' + (sheet ? sheet.getName() : 'null'));
  if (!sheet) return [];
  const data = getSheetData(sheet);
  Logger.log('getCachedSheetData: rows=' + data.length);
  return data;
}

function getCached(key, fetchFn) {
  const cache = CacheService.getScriptCache();
  const cached = cache.get(key);
  if (cached) return JSON.parse(cached);

  const data = fetchFn();
  if (data) cache.put(key, JSON.stringify(data), CACHE_TTL_LOOKUP);
  return data;
}

function invalidateCache(pattern) {
  // GAS CacheService doesn't support pattern-based invalidation.
  // Cache expires automatically per TTL. This function is a no-op.
  Logger.log('Cache invalidation requested for pattern: ' + pattern + ' (no-op — cache expires automatically)');
}

// ================================================================= //
// CORE DATA PRIMITIVES
// ================================================================= //

/**
 * Reads sheet data, auto-parses JSON cells, filters soft-deleted rows.
 */
function getSheetData(sheet, includeDeleted) {
  if (!sheet) return [];
  const rows = sheet.getDataRange().getValues();
  if (rows.length < 1) return [];
  const headers = rows[0];

  return rows.slice(1).map(row => {
    const obj = {};
    headers.forEach((h, i) => {
      let val = row[i];
      if (typeof val === 'string' && (val.startsWith('[') || val.startsWith('{'))) {
        try { val = JSON.parse(val); } catch (e) {}
      }
      obj[h] = val;
    });
    return obj;
  }).filter(row => includeDeleted || (row._deleted !== true && row._deleted !== 'true'));
}

/**
 * Upserts a row with automatic versioning (_v, _ts).
 */
function updateOrInsert(sheet, item, onlyIfNew, options) {
  if (!sheet) return;
  const rows = options?.existingRows || sheet.getDataRange().getValues();
  const headers = rows[0];
  const idIndex = headers.indexOf('id');
  const vIndex = headers.indexOf('_v');

  let rowIndex = -1;
  let currentV = 0;

  for (let i = 1; i < rows.length; i++) {
    if (rows[i][idIndex] == item.id) {
      rowIndex = i + 1;
      currentV = vIndex >= 0 ? (parseInt(rows[i][vIndex]) || 0) : 0;
      break;
    }
  }

  if (rowIndex > 0 && onlyIfNew) return;

  const timestamp = new Date().toISOString();
  const newItem = { ...item, _v: currentV + 1, _ts: timestamp };

  const values = headers.map(h => {
    const val = newItem[h];
    if (val === undefined || val === null) return '';
    return (typeof val === 'object') ? JSON.stringify(val) : val;
  });

  if (rowIndex > 0) {
    sheet.getRange(rowIndex, 1, 1, values.length).setValues([values]);
  } else {
    sheet.appendRow(values);
  }

  // Invalidate lookup caches
  const sheetName = sheet.getName();
  if (sheetName === 'Usuarios') invalidateCache('u:');
  if (sheetName === 'Perfiles') { invalidateCache('p:'); invalidateCache('p:all'); }
}

/**
 * Soft-deletes a row (sets _deleted=true, increments _v, updates _ts).
 * Optimized: single setValues() call instead of 3x setValue().
 */
function softDeleteRow(sheet, id) {
  if (!sheet || !id) return false;
  const rows = sheet.getDataRange().getValues();
  if (rows.length <= 1) return false;
  const headers = rows[0];
  const idIndex = headers.indexOf('id');
  if (idIndex < 0) return false;

  const deletedIndex = headers.indexOf('_deleted');
  const vIndex = headers.indexOf('_v');
  const tsIndex = headers.indexOf('_ts');

  for (let i = 1; i < rows.length; i++) {
    if (rows[i][idIndex] == id) {
      const rowNum = i + 1;
      const newRow = [...rows[i]];
      if (deletedIndex >= 0) newRow[deletedIndex] = true;
      if (vIndex >= 0) newRow[vIndex] = (parseInt(rows[i][vIndex]) || 0) + 1;
      if (tsIndex >= 0) newRow[tsIndex] = new Date().toISOString();
      sheet.getRange(rowNum, 1, 1, newRow.length).setValues([newRow]);

      invalidateSheetCache(sheet.getName());
      return true;
    }
  }
  return false;
}

/**
 * Restores a soft-deleted row (sets _deleted=false).
 */
function restoreRow(sheet, id) {
  if (!sheet || !id) return false;
  const rows = sheet.getDataRange().getValues();
  if (rows.length <= 1) return false;
  const headers = rows[0];
  const idIndex = headers.indexOf('id');
  if (idIndex < 0) return false;

  const deletedIndex = headers.indexOf('_deleted');

  for (let i = 1; i < rows.length; i++) {
    if (rows[i][idIndex] == id) {
      const rowNum = i + 1;
      if (deletedIndex >= 0) {
        sheet.getRange(rowNum, deletedIndex + 1).setValue(false);
      }
      invalidateSheetCache(sheet.getName());
      return true;
    }
  }
  return false;
}

/**
 * Gets version history for a record (includes soft-deleted).
 */
function getVersionHistory(sheet, id) {
  return getSheetData(sheet, true)
    .filter(row => row.id === id)
    .sort((a, b) => (b._v || 0) - (a._v || 0));
}

/**
 * Physically deletes a row from the sheet.
 */
function deleteRowById(sheet, id) {
  if (!sheet || !id) return;
  const rows = sheet.getDataRange().getValues();
  if (rows.length <= 1) return;
  const idIndex = rows[0].indexOf('id');
  if (idIndex < 0) return;

  for (let i = 1; i < rows.length; i++) {
    if (rows[i][idIndex] == id) {
      sheet.deleteRow(i + 1);
      invalidateSheetCache(sheet.getName());
      break;
    }
  }
}

/**
 * Invalidates cache entries for a sheet by name.
 */
function invalidateSheetCache(sheetName) {
  if (sheetName === 'Usuarios') invalidateCache('u:');
  if (sheetName === 'Perfiles') { invalidateCache('p:'); invalidateCache('p:all'); }
}

// ================================================================= //
// RESPONSE HELPER
// ================================================================= //

function createResponse(data) {
  return ContentService.createTextOutput(JSON.stringify(data))
    .setMimeType(ContentService.MimeType.JSON);
}

// ================================================================= //
// AUTHENTICATION — ZERO-KNOWLEDGE
// All config from GSheet, no script properties for app state.
// ================================================================= //

const SESSION_TTL = 86400; // 24 hours in seconds
const CONFIRM_ACTION_TTL = 1800; // 30 minutes in seconds (step-up auth inactivity)

// --- User Lookup ---

function getUsuariosSheet(ssId) {
  return SpreadsheetApp.openById(ssId).getSheetByName('Usuarios');
}

function getUserByUsername(username, ssId) {
  return getCached('u:un:' + username, () => {
    const sheet = getUsuariosSheet(ssId);
    if (!sheet) return null;
    return getSheetData(sheet).find(row => row.username === username) || null;
  });
}

function getUserById(id, ssId) {
  return getCached('u:id:' + id, () => {
    const sheet = getUsuariosSheet(ssId);
    if (!sheet) return null;
    return getSheetData(sheet).find(row => row.id === id) || null;
  });
}

// --- Security: Check if system is initialized ---
function _hasExistingUsers(ssId) {
  try {
    const sheet = getUsuariosSheet(ssId);
    if (!sheet) return false;
    const data = getCachedSheetData(sheet.getParent(), 'Usuarios');
    // Check for any non-deleted users
    return data.some(row => row._deleted !== true && row._deleted !== 'true' && row.username);
  } catch (e) {
    return false;
  }
}

// --- Password ---

function hashPassword(password) {
  if (!password) return '';
  return Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, password)
    .map(b => ('00' + (b < 0 ? b + 256 : b).toString(16)).slice(-2)).join('');
}

function verifyPassword(password, hash) {
  return !!(password && hash && hashPassword(password) === hash);
}

function validatePasswordComplexity(password) {
  const errors = [];
  if (!password) { errors.push('La contraseña es requerida'); return { valid: false, errors }; }
  if (password.length < 8) errors.push('Mínimo 8 caracteres');
  if (password.length > 128) errors.push('Máximo 128 caracteres');
  if (!/[a-z]/.test(password)) errors.push('Al menos una letra minúscula');
  if (!/[A-Z]/.test(password)) errors.push('Al menos una letra mayúscula');
  if (!/[0-9]/.test(password)) errors.push('Al menos un número');
  if (!/[^a-zA-Z0-9]/.test(password)) errors.push('Al menos un carácter especial');
  return { valid: errors.length === 0, errors };
}

// --- Config Parsers ---

function parseAuthConfig(authConfig) {
  const defaults = { default_method: 'passkey', password_hash: '', recovery_enabled: true, email_otp: { enabled: false }, totp: { enabled: false }, passkeys: [] };
  if (!authConfig) return defaults;
  try { return typeof authConfig === 'string' ? JSON.parse(authConfig) : authConfig; } catch (e) { return defaults; }
}

function parseUserMetadata(metadata) {
  const defaults = { last_login: null, last_password_change: null, failed_login_attempts: 0, created_from_ip: null };
  if (!metadata) return defaults;
  try { return typeof metadata === 'string' ? JSON.parse(metadata) : metadata; } catch (e) { return defaults; }
}

// --- User CRUD ---

function createUser(userData, ssId) {
  const sheet = getUsuariosSheet(ssId);
  if (!sheet) throw new Error('Hoja Usuarios no encontrada');

  if (getUserByUsername(userData.username, ssId)) throw new Error('ERR_USER_EXISTS: El usuario ya existe');
  if (!userData.password) throw new Error('ERR_PASSWORD_REQUIRED: La contraseña es requerida');
  const pwValidation = validatePasswordComplexity(userData.password);
  if (!pwValidation.valid) throw new Error('ERR_PASSWORD_WEAK: ' + pwValidation.errors.join(', '));

  const now = new Date().toISOString();
  const authConfig = {
    default_method: userData.default_method || 'passkey',
    password_hash: hashPassword(userData.password),
    recovery_enabled: true,
    email_otp: { enabled: true, created_at: now },
    totp: { enabled: false, secret: null, created_at: null },
    passkeys: [],
  };
  const metadata = {
    last_login: null,
    last_password_change: now,
    failed_login_attempts: 0,
    created_from_ip: userData.ip || null,
  };

  const user = {
    id: Utilities.getUuid(),
    username: userData.username,
    email: userData.email || '',
    wrapped_mk: userData.wrapped_mk || '',
    perfilIds: JSON.stringify(userData.perfilIds || ['p_publicador']),
    auth_config: JSON.stringify(authConfig),
    metadata: JSON.stringify(metadata),
    created_at: now,
    _ts: now,
  };

  updateOrInsert(sheet, user, false);
  invalidateCache('u:');
  return { success: true, user: { id: user.id, username: user.username } };
}

function updateUser(id, updates, ssId) {
  const sheet = getUsuariosSheet(ssId);
  if (!sheet) throw new Error('Hoja Usuarios no encontrada');

  const user = getUserById(id, ssId);
  if (!user) throw new Error('ERR_USER_NOT_FOUND: Usuario no encontrado');

  const processed = { ...updates };
  if (updates.auth_config && typeof updates.auth_config === 'object') processed.auth_config = JSON.stringify(updates.auth_config);
  if (updates.metadata && typeof updates.metadata === 'object') processed.metadata = JSON.stringify(updates.metadata);

  updateOrInsert(sheet, { ...user, ...processed, _ts: new Date().toISOString() }, false);
  invalidateCache('u:');
  return { success: true, user: { id: user.id, username: user.username } };
}

function updateUserPassword(userId, newPassword, ssId) {
  const user = getUserById(userId, ssId);
  if (!user) throw new Error('ERR_USER_NOT_FOUND: Usuario no encontrado');

  const authConfig = parseAuthConfig(user.auth_config);
  authConfig.password_hash = hashPassword(newPassword);
  const metadata = parseUserMetadata(user.metadata);
  const now = new Date().toISOString();
  metadata.last_password_change = now;

  updateUser(userId, { auth_config: authConfig, metadata: metadata }, ssId);
  return { success: true };
}

function updateUserMetadata(userId, updates, ssId) {
  const user = getUserById(userId, ssId);
  if (!user) throw new Error('ERR_USER_NOT_FOUND: Usuario no encontrado');
  const metadata = { ...parseUserMetadata(user.metadata), ...updates };
  return updateUser(userId, { metadata: metadata }, ssId);
}

function incrementFailedLoginAttempts(userId, ssId) {
  const user = getUserById(userId, ssId);
  if (!user) return;
  const metadata = parseUserMetadata(user.metadata);
  metadata.failed_login_attempts = (metadata.failed_login_attempts || 0) + 1;
  updateUser(userId, { metadata: metadata }, ssId);
}

function resetFailedLoginAttempts(userId, ssId) {
  const user = getUserById(userId, ssId);
  if (!user) return;
  const metadata = parseUserMetadata(user.metadata);
  metadata.failed_login_attempts = 0;
  updateUser(userId, { metadata: metadata }, ssId);
}

function getUserMetadataValue(userId, key, ssId) {
  const user = getUserById(userId, ssId);
  if (!user) return null;
  return parseUserMetadata(user.metadata)[key] || null;
}

function invalidateAllUserSessions(userId) {
  const sessions = getUserSessions(userId);
  sessions.forEach(s => { try { invalidateSession(s.token); } catch (e) {} });
}

// ================================================================= //
// ADMIN ACTIONS — User & Profile Management
// ================================================================= //

function actionGetUsers(session, ssId) {
  // Require admin permission on 'core' module
  if (!validarPermiso(session.userId, 'core', 'read', ssId, 'Usuarios')) {
    throw new Error('ERR_PERMISSION_DENIED: No tienes permiso para ver usuarios');
  }
  
  const sheet = getUsuariosSheet(ssId);
  if (!sheet) throw new Error('ERR_SHEET_NOT_FOUND: Hoja Usuarios no encontrada');
  
  const allUsers = getSheetData(sheet);
  const allPerfiles = getAllPerfiles(ssId);
  
  // Parse perfilIds and enrich with profile names
  const users = allUsers
    .filter(u => !u._deleted || u._deleted !== 'true')
    .map(u => {
      let perfilIds = [];
      try { perfilIds = u.perfilIds ? JSON.parse(u.perfilIds) : []; } catch (e) { perfilIds = []; }
      
      // Resolve profile names
      const perfiles = perfilIds.map(pid => {
        const p = allPerfiles.find(pf => pf.id === pid);
        return p ? { id: pf.id, nombre: pf.nombre } : { id: pid, nombre: pid };
      });
      
      // Parse metadata
      let metadata = {};
      try { metadata = u.metadata ? JSON.parse(u.metadata) : {}; } catch (e) {}
      
      // Parse auth_config for display
      let authConfig = {};
      try { authConfig = u.auth_config ? JSON.parse(u.auth_config) : {}; } catch (e) {}
      
      return {
        id: u.id,
        username: u.username,
        email: u.email || '',
        perfilIds: perfilIds,
        perfiles: perfiles,
        active: !u._deleted || u._deleted !== 'true',
        created_at: u.created_at,
        last_login: metadata.last_login || null,
        failed_attempts: metadata.failed_login_attempts || 0,
        has_password: !!(authConfig.password_hash),
        has_totp: !!(authConfig.totp && authConfig.totp.enabled),
        has_passkeys: !!(authConfig.passkeys && authConfig.passkeys.length > 0),
      };
    });
  
  return { success: true, users: users };
}

function actionCreateUser(session, payload, ssId) {
  if (!validarPermiso(session.userId, 'core', 'write', ssId, 'Usuarios')) {
    throw new Error('ERR_PERMISSION_DENIED: No tienes permiso para crear usuarios');
  }
  
  const { username, email, password, perfilIds, wrapped_mk } = payload;
  
  if (!username) throw new Error('ERR_USERNAME_REQUIRED: El nombre de usuario es requerido');
  if (!password) throw new Error('ERR_PASSWORD_REQUIRED: La contraseña es requerida');
  
  // Default to p_publicador if no profiles specified
  const finalPerfilIds = perfilIds && perfilIds.length > 0 ? perfilIds : ['p_publicador'];
  
  const result = createUser({ username, email, password, perfilIds: finalPerfilIds, wrapped_mk }, ssId);
  return result;
}

function actionUpdateUser(session, payload, ssId) {
  if (!validarPermiso(session.userId, 'core', 'write', ssId, 'Usuarios')) {
    throw new Error('ERR_PERMISSION_DENIED: No tienes permiso para modificar usuarios');
  }
  
  const { id, username, email, perfilIds, active } = payload;
  
  if (!id) throw new Error('ERR_USER_ID_REQUIRED: El ID de usuario es requerido');
  
  const user = getUserById(id, ssId);
  if (!user) throw new Error('ERR_USER_NOT_FOUND: Usuario no encontrado');
  
  // Prevent modifying own admin account
  if (id === session.userId) {
    throw new Error('ERR_SELF_MODIFY: No puedes modificar tu propia cuenta de administrador');
  }
  
  const updates = {};
  if (username !== undefined) updates.username = username;
  if (email !== undefined) updates.email = email;
  if (perfilIds !== undefined) updates.perfilIds = JSON.stringify(perfilIds);
  if (active !== undefined) updates._deleted = active ? 'false' : 'true';
  
  updateUser(id, updates, ssId);
  invalidateCache('u:');
  return { success: true };
}

function actionDeleteUser(session, payload, ssId) {
  if (!validarPermiso(session.userId, 'core', 'write', ssId, 'Usuarios')) {
    throw new Error('ERR_PERMISSION_DENIED: No tienes permiso para eliminar usuarios');
  }
  
  const { id } = payload;
  
  if (!id) throw new Error('ERR_USER_ID_REQUIRED: El ID de usuario es requerido');
  
  const user = getUserById(id, ssId);
  if (!user) throw new Error('ERR_USER_NOT_FOUND: Usuario no encontrado');
  
  // Prevent deleting own admin account
  if (id === session.userId) {
    throw new Error('ERR_SELF_DELETE: No puedes eliminar tu propia cuenta de administrador');
  }
  
  // Soft delete (mark as deleted)
  updateUser(id, { _deleted: 'true' }, ssId);
  invalidateCache('u:');
  
  // Invalidate all sessions for this user
  invalidateAllUserSessions(id);
  
  return { success: true };
}

function actionGetPerfiles(session, ssId) {
  if (!validarPermiso(session.userId, 'core', 'read', ssId, 'Perfiles')) {
    throw new Error('ERR_PERMISSION_DENIED: No tienes permiso para ver perfiles');
  }
  
  const perfiles = getAllPerfiles(ssId);
  
  const parsedPerfiles = perfiles
    .filter(p => !p._deleted || p._deleted !== 'true')
    .map(p => {
      let permisos = {};
      try { permisos = p.permisos ? JSON.parse(p.permisos) : {}; } catch (e) { permisos = {}; }
      
      return {
        id: p.id,
        nombre: p.nombre,
        descripcion: p.descripcion || '',
        permisos: permisos,
        _v: p._v,
      };
    });
  
  return { success: true, perfiles: parsedPerfiles };
}

function actionCreateProfile(session, payload, ssId) {
  if (!validarPermiso(session.userId, 'core', 'write', ssId, 'Perfiles')) {
    throw new Error('ERR_PERMISSION_DENIED: No tienes permiso para crear perfiles');
  }
  
  const { id, nombre, descripcion, permisos } = payload;
  
  if (!id) throw new Error('ERR_PROFILE_ID_REQUIRED: El ID de perfil es requerido');
  if (!nombre) throw new Error('ERR_PROFILE_NAME_REQUIRED: El nombre de perfil es requerido');
  
  // Check if profile with same id exists
  const existing = getPerfilById(id, ssId);
  if (existing) throw new Error('ERR_PROFILE_EXISTS: Ya existe un perfil con ese ID');
  
  const sheet = getPerfilesSheet(ssId);
  if (!sheet) throw new Error('ERR_SHEET_NOT_FOUND: Hoja Perfiles no encontrada');
  
  const profile = {
    id: id,
    nombre: nombre,
    descripcion: descripcion || '',
    permisos: JSON.stringify(permisos || {}),
    _ts: new Date().toISOString(),
  };
  
  updateOrInsert(sheet, profile, false);
  invalidateCache('p:');
  return { success: true, profile: { id: profile.id, nombre: profile.nombre } };
}

function actionUpdateProfile(session, payload, ssId) {
  if (!validarPermiso(session.userId, 'core', 'write', ssId, 'Perfiles')) {
    throw new Error('ERR_PERMISSION_DENIED: No tienes permiso para modificar perfiles');
  }
  
  const { id, nombre, descripcion, permisos } = payload;
  
  if (!id) throw new Error('ERR_PROFILE_ID_REQUIRED: El ID de perfil es requerido');
  
  const sheet = getPerfilesSheet(ssId);
  if (!sheet) throw new Error('ERR_SHEET_NOT_FOUND: Hoja Perfiles no encontrada');
  
  const existing = getPerfilById(id, ssId);
  if (!existing) throw new Error('ERR_PROFILE_NOT_FOUND: Perfil no encontrado');
  
  const updates = {};
  if (nombre !== undefined) updates.nombre = nombre;
  if (descripcion !== undefined) updates.descripcion = descripcion;
  if (permisos !== undefined) updates.permisos = JSON.stringify(permisos);
  updates._ts = new Date().toISOString();
  
  updateOrInsert(sheet, { ...existing, ...updates }, false);
  invalidateCache('p:');
  return { success: true };
}

function actionDeleteProfile(session, payload, ssId) {
  if (!validarPermiso(session.userId, 'core', 'write', ssId, 'Perfiles')) {
    throw new Error('ERR_PERMISSION_DENIED: No tienes permiso para eliminar perfiles');
  }
  
  const { id } = payload;
  
  if (!id) throw new Error('ERR_PROFILE_ID_REQUIRED: El ID de perfil es requerido');
  
  // Prevent deleting system profiles
  if (id === 'p_admin' || id === 'p_publicador') {
    throw new Error('ERR_PROFILE_PROTECTED: No puedes eliminar perfiles del sistema');
  }
  
  const sheet = getPerfilesSheet(ssId);
  if (!sheet) throw new Error('ERR_SHEET_NOT_FOUND: Hoja Perfiles no encontrada');
  
  const existing = getPerfilById(id, ssId);
  if (!existing) throw new Error('ERR_PROFILE_NOT_FOUND: Perfil no encontrado');
  
  // Soft delete
  updateOrInsert(sheet, { ...existing, _deleted: 'true', _ts: new Date().toISOString() }, false);
  invalidateCache('p:');
  return { success: true };
}

// --- Auth Helpers ---

/**
 * Resolves a user from either a valid session token or username+password.
 */
function resolveUser(sessionToken, username, password, ssId) {
  if (sessionToken) {
    const session = validateSession(sessionToken, ssId);
    if (!session.valid) return { error: 'ERR_AUTH_INVALID: Sesión inválida o expirada' };
    const user = getUserById(session.userId, ssId);
    return user ? { user, username: user.username } : { error: 'ERR_USER_NOT_FOUND' };
  }
  if (!username || !password) return { error: 'ERR_INVALID_CREDENTIALS: Usuario y contraseña requeridos' };
  const user = getUserByUsername(username, ssId);
  if (!user) return { error: 'ERR_USER_NOT_FOUND' };
  if (!verifyPassword(password, parseAuthConfig(user.auth_config).password_hash)) {
    return { error: 'ERR_INVALID_CREDENTIALS: Contraseña incorrecta' };
  }
  return { user, username };
}

/**
 * Derives rpId from an origin URL for WebAuthn.
 */
function deriveRpId(origin) {
  if (!origin) return 'localhost';
  try {
    const match = origin.match(/^https?:\/\/([^:\/]+)/);
    return match ? match[1] : 'localhost';
  } catch (e) { return 'localhost'; }
}

/**
 * Generates a random base64 challenge for WebAuthn.
 */
function generateChallenge() {
  const bytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, Utilities.getUuid() + Date.now());
  return Utilities.base64Encode(bytes);
}

/**
 * Returns enabled auth methods from an authConfig object.
 */
function getEnabledMethods(authConfig) {
  const methods = [];
  if (authConfig.passkeys?.length > 0) methods.push('passkey');
  if (authConfig.totp?.enabled) methods.push('totp');
  if (authConfig.email_otp?.enabled) methods.push('email_otp');
  return methods;
}

/**
 * Updates user auth_config and clears related cache entries.
 */
function updateUserAuthConfig(user, authConfig, ssId) {
  updateUser(user.id, { auth_config: authConfig }, ssId);
  CacheService.getScriptCache().remove('u:un:' + user.username);
  CacheService.getScriptCache().remove('u:id:' + user.id);
}

// ================================================================= //
// AUTH ACTIONS
// ================================================================= //

function actionLogin(payload, ssId) {
  try {
    const { username, password, method, code, passkeyAssertion } = payload;

    const rateLimit = checkRateLimit('login:' + username, 5, 60);
    if (!rateLimit.allowed) {
      return { success: false, error: 'ERR_RATE_LIMITED: Demasiados intentos. Intenta más tarde.', retryAfter: rateLimit.resetIn, step: 'password' };
    }

    if (!ssId) return { success: false, error: 'ERR_SS_ID_REQUIRED: Se requiere ssId para login', step: 'password' };

    const user = getUserByUsername(username, ssId);
    if (!user) return { success: false, error: 'ERR_AUTH_INVALID: Usuario no encontrado', step: 'password' };

    const authConfig = parseAuthConfig(user.auth_config);

    // Step 1: Password verification
    if (!password) return { success: false, error: 'ERR_PASSWORD_REQUIRED: Ingrese su contraseña', step: 'password' };
    if (!verifyPassword(password, authConfig.password_hash)) {
      incrementFailedLoginAttempts(user.id, ssId);
      logAccess(username, false, 'Contraseña inválida', ssId);
      return { success: false, error: 'ERR_AUTH_INVALID: Contraseña incorrecta', step: 'password' };
    }
    resetFailedLoginAttempts(user.id, ssId);

    // Step 2: Detect enabled methods
    const enabledMethods = getEnabledMethods(authConfig);

    if (!method) {
      if (enabledMethods.length === 1) {
        const singleMethod = enabledMethods[0];
        if (singleMethod === 'email_otp') {
          const otpResult = actionRequestOTP({ username: username }, ssId);
          if (!otpResult.success) return otpResult;
          return { success: false, step: singleMethod, availableMethods: enabledMethods, message: 'Código enviado automáticamente' };
        }
        return { success: false, step: singleMethod, availableMethods: enabledMethods, message: 'Ingrese su código' };
      }
      return { success: false, step: 'method', availableMethods: enabledMethods, defaultMethod: authConfig.default_method || 'passkey', message: 'Seleccione método de autenticación' };
    }

    // Step 3: Verify selected method
    if (method === 'totp') {
      if (!authConfig.totp?.enabled || !authConfig.totp?.secret) return { success: false, error: 'ERR_TOTP_NOT_CONFIGURED', step: 'method' };
      if (!code) return { success: false, error: 'ERR_CODE_REQUIRED: Ingrese código TOTP', step: 'totp' };
      if (!verifyTOTP(authConfig.totp.secret, code)) {
        logAccess(username, false, 'TOTP inválido', ssId);
        return { success: false, error: 'ERR_AUTH_INVALID: Código TOTP inválido', step: 'totp' };
      }
    } else if (method === 'email_otp') {
      if (!authConfig.email_otp?.enabled) return { success: false, error: 'ERR_EMAIL_OTP_NOT_CONFIGURED', step: 'method' };
      if (!code) return { success: false, error: 'ERR_CODE_REQUIRED: Ingrese código del email', step: 'email_otp' };
      if (!verifyEmailOTP(username, code)) {
        logAccess(username, false, 'Email OTP inválido', ssId);
        return { success: false, error: 'ERR_AUTH_INVALID: Código inválido', step: 'email_otp' };
      }
    } else if (method === 'passkey') {
      if (!authConfig.passkeys?.length) return { success: false, error: 'ERR_PASSKEY_NOT_CONFIGURED', step: 'method' };
      if (!passkeyAssertion) return { success: false, error: 'ERR_PASSKEY_REQUIRED', step: 'passkey' };
      if (!authConfig.passkeys.find(pk => pk.id === passkeyAssertion.credentialId)) {
        logAccess(username, false, 'Passkey inválido', ssId);
        return { success: false, error: 'ERR_AUTH_INVALID: Passkey no reconocido', step: 'passkey' };
      }
    }

    // Success
    updateUserMetadata(user.id, { last_login: new Date().toISOString() }, ssId);
    const session = generateSessionToken(user.id, ssId);
    logAccess(username, true, 'Login exitoso', ssId);

    return {
      success: true,
      sessionToken: session.sessionToken,
      wrapped_mk: user.wrapped_mk,
      expiresAt: session.expiresAt,
      user: { id: user.id, username: user.username, perfilId: user.perfilId },
    };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

function actionRegister(payload, ssId, session) {
  try {
    if (!ssId) return { success: false, error: 'ERR_SS_ID_REQUIRED' };
    
    // Security: If system already has users, require authenticated session with admin permissions
    if (_hasExistingUsers(ssId)) {
      if (!session || !session.valid) return { success: false, error: 'ERR_AUTH_REQUIRED: Sistema ya inicializado. Inicia sesión primero.' };
      const permCheck = checkPermission(session, 'write', 'usuarios', ssId, 'core');
      if (!permCheck.allowed) return { success: false, error: 'ERR_PERMISSION_DENIED: No tienes permiso para crear usuarios.' };
    }
    
    if (!payload.email || !payload.email.trim()) return { success: false, error: 'ERR_EMAIL_REQUIRED: El email es requerido' };
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(payload.email.trim())) return { success: false, error: 'ERR_EMAIL_INVALID: Formato de email inválido' };

    const result = createUser({
      username: payload.username,
      email: payload.email.trim(),
      password: payload.password,
      wrapped_mk: payload.wrapped_mk,
      perfilId: payload.perfilId,
      ip: payload.ip,
    }, ssId);

    try { sendWelcomeEmail(payload.email, payload.username, 'tu congregación'); } catch (e) { Logger.log('Error sending welcome email: ' + e.message); }

    return { success: true, user: result.user };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

function actionChallenge(payload, ssId) {
  try {
    if (!ssId) return { success: false, error: 'ERR_SS_ID_REQUIRED' };
    const user = getUserByUsername(payload.username, ssId);
    if (!user) return { success: false, error: 'ERR_USER_NOT_FOUND' };

    const authConfig = parseAuthConfig(user.auth_config);
    const challenge = generateChallenge();

    PropertiesService.getUserProperties().setProperty(
      'passkey_challenge_' + payload.username,
      JSON.stringify({ challenge, createdAt: new Date().toISOString(), expiresAt: new Date(Date.now() + 5 * 60 * 1000).toISOString() })
    );

    return {
      success: true,
      challenge: challenge,
      rpId: deriveRpId(payload.origin),
      timeout: 60000,
      allowCredentials: (authConfig.passkeys || []).map(pk => ({ id: pk.id, type: 'public-key' })),
      userVerification: 'preferred',
    };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

function actionSetupTOTP(payload, ssId) {
  try {
    const { sessionToken, username, password } = payload;
    const resolved = resolveUser(sessionToken, username, password, ssId);
    if (resolved.error) return { success: false, error: resolved.error };

    const totpResult = generateTOTPSecret(resolved.username);
    if (!totpResult.success) return totpResult;

    PropertiesService.getUserProperties().setProperty(
      'totp_pending_' + resolved.username,
      JSON.stringify({ secret: totpResult.secret, createdAt: new Date().toISOString(), expiresAt: new Date(Date.now() + 10 * 60 * 1000).toISOString() })
    );

    return { success: true, secret: totpResult.secret, otpURI: totpResult.otpURI };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

function actionConfirmTOTP(payload, ssId) {
  try {
    const { sessionToken, username, password, code } = payload;
    const resolved = resolveUser(sessionToken, username, password, ssId);
    if (resolved.error) return { success: false, error: resolved.error };

    const pendingStr = PropertiesService.getUserProperties().getProperty('totp_pending_' + resolved.username);
    if (!pendingStr) return { success: false, error: 'ERR_NO_PENDING_TOTP: No hay configuración TOTP pendiente' };

    const pending = JSON.parse(pendingStr);
    if (new Date(pending.expiresAt) < new Date()) {
      PropertiesService.getUserProperties().deleteProperty('totp_pending_' + resolved.username);
      return { success: false, error: 'ERR_TOTP_EXPIRED: La configuración ha expirado' };
    }

    if (!verifyTOTP(pending.secret, code)) return { success: false, error: 'ERR_INVALID_CODE: Código inválido' };

    const authConfig = parseAuthConfig(resolved.user.auth_config);
    authConfig.totp = { enabled: true, secret: pending.secret, created_at: new Date().toISOString() };

    if (authConfig.default_method !== 'passkey') {
      authConfig.default_method = 'totp';
    }

    updateUserAuthConfig(resolved.user, authConfig, ssId);

    PropertiesService.getUserProperties().deleteProperty('totp_pending_' + resolved.username);
    return { success: true, message: 'TOTP configurado correctamente' };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

function actionSetupPasskey(payload, ssId) {
  try {
    const { sessionToken, username, password, deviceName } = payload;
    const resolved = resolveUser(sessionToken, username, password, ssId);
    if (resolved.error) return { success: false, error: resolved.error };

    const authConfig = parseAuthConfig(resolved.user.auth_config);
    const challenge = generateChallenge();
    const userIdBytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, resolved.username + Date.now());

    PropertiesService.getUserProperties().setProperty(
      'passkey_setup_' + resolved.username,
      JSON.stringify({ challenge, deviceName: deviceName || 'Dispositivo nuevo', username: resolved.username, createdAt: new Date().toISOString(), expiresAt: new Date(Date.now() + 10 * 60 * 1000).toISOString() })
    );

    return {
      success: true,
      challenge: challenge,
      rpId: deriveRpId(payload.origin),
      timeout: 60000,
      user: { id: Utilities.base64Encode(userIdBytes), name: resolved.username, displayName: resolved.username },
      pubKeyCredParams: [{ type: 'public-key', alg: -7 }, { type: 'public-key', alg: -257 }],
      attestation: 'preferred',
      excludeCredentials: (authConfig.passkeys || []).map(pk => ({ id: pk.id, type: 'public-key' })),
    };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

function actionConfirmPasskey(payload, ssId) {
  try {
    const { sessionToken, username, password, attestation } = payload;
    const resolved = resolveUser(sessionToken, username, password, ssId);
    if (resolved.error) return { success: false, error: resolved.error };

    const pendingStr = PropertiesService.getUserProperties().getProperty('passkey_setup_' + resolved.username);
    if (!pendingStr) return { success: false, error: 'ERR_PASSKEY_SETUP_EXPIRED: La configuración expiró' };

    const pending = JSON.parse(pendingStr);
    if (new Date(pending.expiresAt) < new Date()) {
      PropertiesService.getUserProperties().deleteProperty('passkey_setup_' + resolved.username);
      return { success: false, error: 'ERR_PASSKEY_SETUP_EXPIRED: La configuración expiró' };
    }

    const authConfig = parseAuthConfig(resolved.user.auth_config);
    authConfig.passkeys = authConfig.passkeys || [];
    authConfig.passkeys.push({
      id: attestation.id,
      public_key: attestation.response.publicKey || '',
      device_name: pending.deviceName,
      created_at: new Date().toISOString(),
    });

    authConfig.default_method = 'passkey';

    updateUserAuthConfig(resolved.user, authConfig, ssId);
    PropertiesService.getUserProperties().deleteProperty('passkey_setup_' + resolved.username);

    return { success: true, message: 'Passkey configurado exitosamente', passkeyId: attestation.id };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

function actionDeletePasskey(payload, ssId) {
  try {
    const { sessionToken, passkeyId } = payload;
    const session = validateSession(sessionToken, ssId);
    if (!session.valid) return { success: false, error: 'ERR_AUTH_INVALID' };

    const user = getUserById(session.userId, ssId);
    if (!user) return { success: false, error: 'ERR_USER_NOT_FOUND' };

    const authConfig = parseAuthConfig(user.auth_config);
    const idx = (authConfig.passkeys || []).findIndex(pk => pk.id === passkeyId);
    if (idx === -1) return { success: false, error: 'ERR_PASSKEY_NOT_FOUND: Passkey no encontrado' };

    authConfig.passkeys.splice(idx, 1);
    updateUserAuthConfig(user, authConfig, ssId);
    return { success: true, message: 'Passkey eliminado' };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

function actionChangePassword(payload, sessionToken, ssId) {
  try {
    const { old_password, new_password } = payload;
    const session = validateSession(sessionToken, ssId);
    if (!session.valid) return { success: false, error: 'ERR_AUTH_INVALID' };

    if (!old_password || !new_password) return { success: false, error: 'ERR_INVALID_CREDENTIALS: Contraseñas requeridas' };
    if (new_password.length < 8) return { success: false, error: 'ERR_WEAK_PASSWORD: Mínimo 8 caracteres' };

    const user = getUserById(session.userId, ssId);
    if (!user) return { success: false, error: 'ERR_USER_NOT_FOUND' };

    const authConfig = parseAuthConfig(user.auth_config);
    if (!verifyPassword(old_password, authConfig.password_hash)) {
      updateUserMetadata(session.userId, { failed_login_attempts: (getUserMetadataValue(session.userId, 'failed_login_attempts', ssId) || 0) + 1 }, ssId);
      return { success: false, error: 'ERR_INVALID_CREDENTIALS: Contraseña actual incorrecta' };
    }

    authConfig.password_hash = hashPassword(new_password);
    updateUserAuthConfig(user, authConfig, ssId);
    updateUserMetadata(session.userId, { last_password_change: new Date().toISOString(), failed_login_attempts: 0 }, ssId);
    logAccess(user.username, true, 'Contraseña cambiada', ssId);

    return { success: true };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

function actionConfirmPasswordReset(payload, sessionToken, ssId) {
  try {
    const { userId, token, newPassword } = payload;
    if (!userId || !token || !newPassword) return { success: false, error: 'ERR_INVALID_REQUEST: Datos incompletos' };

    const pwValidation = validatePasswordComplexity(newPassword);
    if (!pwValidation.valid) return { success: false, error: 'ERR_PASSWORD_WEAK: ' + pwValidation.errors.join(', ') };

    const stored = PropertiesService.getUserProperties().getProperty('pwd_reset_' + userId);
    if (!stored) return { success: false, error: 'ERR_INVALID_TOKEN: Token inválido o expirado' };

    const resetData = JSON.parse(stored);
    if (resetData.token !== token) return { success: false, error: 'ERR_INVALID_TOKEN: Token inválido' };
    if (new Date(resetData.expiresAt) < new Date()) {
      PropertiesService.getUserProperties().deleteProperty('pwd_reset_' + userId);
      return { success: false, error: 'ERR_TOKEN_EXPIRED: El token ha expirado' };
    }

    const user = getUserById(userId, ssId);
    if (!user) return { success: false, error: 'ERR_USER_NOT_FOUND' };

    updateUserPassword(userId, newPassword, ssId);
    invalidateAllUserSessions(userId);
    PropertiesService.getUserProperties().deleteProperty('pwd_reset_' + userId);

    try { sendPasswordChangedEmail(user.email || user.username, user.username); } catch (e) {}
    logAccess(user.username, true, 'Contraseña restablecida', ssId);

    return { success: true, message: 'Contraseña restablecida exitosamente' };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

function actionRequestPasswordReset(payload, ssId) {
  try {
    if (!ssId) return { success: false, error: 'ERR_SS_ID_REQUIRED' };
    const user = getUserByUsername(payload.username, ssId);
    if (!user) return { success: true, message: 'Si el usuario existe, recibirás un email' };

    const resetToken = Utilities.getUuid();
    PropertiesService.getUserProperties().setProperty(
      'pwd_reset_' + user.id,
      JSON.stringify({ token: resetToken, expiresAt: new Date(Date.now() + 60 * 60 * 1000).toISOString() })
    );

    const resetLink = 'https://congre-admin.github.io/admin/reset-password?token=' + resetToken + '&userId=' + user.id;
    sendPasswordResetEmail(user.email || payload.username, user.username, resetLink);
    logAccess(payload.username, true, 'Solicitud de reset de contraseña', ssId);

    return { success: true, message: 'Si el usuario existe, recibirás un email con instrucciones' };
  } catch (err) {
    Logger.log('Error en requestPasswordReset: ' + err.message);
    return { success: false, error: err.message };
  }
}

function actionRequestOTP(payload, ssId) {
  try {
    if (!ssId) return { success: false, error: 'ERR_SS_ID_REQUIRED' };
    const isVerification = payload.verifyOnly === true;
    const user = getUserByUsername(payload.username, ssId);
    if (!user) return { success: false, error: 'ERR_USER_NOT_FOUND', debug: { username: payload.username } };

    const email = user.email || payload.username;
    Logger.log('actionRequestOTP: username=' + payload.username + ', resolved email=' + email);

    if (!isVerification) {
      const rateLimit = checkRateLimit('otp:' + payload.username, 5, 60);
      if (!rateLimit.allowed) {
        return { success: false, error: 'ERR_RATE_LIMITED: Demasiados códigos solicitados.', retryAfter: rateLimit.resetIn };
      }
    }

    const code = Math.floor(100000 + Math.random() * 900000).toString();
    PropertiesService.getUserProperties().setProperty(
      'otp_' + payload.username,
      JSON.stringify({ code, createdAt: new Date().toISOString(), expiresAt: new Date(Date.now() + 10 * 60 * 1000).toISOString() })
    );

    try { sendOTPEmail(email, code, 'Congregación'); } catch (e) {
      Logger.log('Error sending OTP email: ' + e.message);
      return { success: false, error: 'ERR_EMAIL_SEND: No se pudo enviar el código por email', debug: { email, error: e.message } };
    }

    logAccess(payload.username, true, 'OTP enviado por email', ssId);
    return { success: true, message: 'Código enviado por email', debug: { email } };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

function actionGetAuthMethods(payload, sessionToken, ssId) {
  try {
    const session = validateSession(sessionToken, ssId);
    if (!session.valid) return { success: false, error: 'ERR_AUTH_INVALID' };

    const user = getUserById(session.userId, ssId);
    if (!user) return { success: false, error: 'ERR_USER_NOT_FOUND' };

    const authConfig = parseAuthConfig(user.auth_config);
    const methods = getEnabledMethods(authConfig);

    return {
      success: true,
      methods: methods,
      defaultMethod: authConfig.default_method,
      passkeys: authConfig.passkeys || [],
      totp: { enabled: authConfig.totp?.enabled || false },
      email_otp: { enabled: authConfig.email_otp?.enabled || false },
      recovery_enabled: authConfig.recovery_enabled ?? true,
    };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

function actionSetDefaultAuthMethod(payload, sessionToken, ssId) {
  try {
    const { method } = payload;
    const session = validateSession(sessionToken, ssId);
    if (!session.valid) return { success: false, error: 'ERR_AUTH_INVALID' };

    const user = getUserById(session.userId, ssId);
    if (!user) return { success: false, error: 'ERR_USER_NOT_FOUND' };

    const authConfig = parseAuthConfig(user.auth_config);
    authConfig.default_method = method;
    updateUserAuthConfig(user, authConfig, ssId);

    return { success: true };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Confirms a sensitive action with 2FA (step-up authentication).
 * Validates session, checks trigger, then validates second factor.
 */
function actionConfirmAction(payload, sessionToken, ssId) {
  const { code, passkeyAssertion } = payload;
  
  // Validate session first
  const session = validateSession(sessionToken, ssId);
  if (!session.valid) return { confirmed: false, error: 'ERR_AUTH_INVALID' };
  
  const user = getUserById(session.userId, ssId);
  if (!user) return { confirmed: false, error: 'ERR_USER_NOT_FOUND' };
  
  // Check if user is locked
  const metadata = parseUserMetadata(user.metadata);
  if (metadata?.locked) return { confirmed: false, error: 'ERR_ACCOUNT_LOCKED', locked: true };
  
  // Check inactivity timeout (if lastAction exists)
  if (metadata?.lastAction) {
    const lastActionTime = new Date(metadata.lastAction).getTime();
    const idleMs = Date.now() - lastActionTime;
    const idleSeconds = Math.floor(idleMs / 1000);
    if (idleSeconds > CONFIRM_ACTION_TTL) {
      // Need re-confirmation due to inactivity
      return { confirmed: false, error: 'ERR_CONFIRM_REQUIRED_INACTIVITY', needsConfirmation: true, idleSeconds };
    }
  }
  
  // Check failed attempts
  const failedAttempts = metadata?.confirm_failed_attempts || 0;
  if (failedAttempts >= 5) {
    // Lock account
    updateUserMetadata(user.id, { locked: true, locked_at: new Date().toISOString() }, ssId);
    return { confirmed: false, error: 'ERR_ACCOUNT_LOCKED', locked: true };
  }
  
  const authConfig = parseAuthConfig(user.auth_config);
  const enabledMethods = getEnabledMethods(authConfig);
  
  if (enabledMethods.length === 0) {
    // No 2FA configured - fail safe, require password
    return { confirmed: false, error: 'ERR_2FA_NOT_CONFIGURED', requiresPassword: true };
  }
  
  // Validate based on default method
  const defaultMethod = authConfig.default_method;
  let isValid = false;
  let error = 'ERR_CONFIRM_FAILED';
  
  if (defaultMethod === 'totp' && authConfig.totp?.enabled) {
    if (!code) return { confirmed: false, error: 'ERR_CODE_REQUIRED', method: 'totp' };
    isValid = verifyTOTP(authConfig.totp.secret, code);
    if (!isValid) error = 'ERR_INVALID_CODE';
  } else if (defaultMethod === 'email_otp' && authConfig.email_otp?.enabled) {
    if (!code) return { confirmed: false, error: 'ERR_CODE_REQUIRED', method: 'email_otp' };
    isValid = verifyEmailOTP(user.username, code);
    if (!isValid) error = 'ERR_INVALID_CODE';
  } else if (defaultMethod === 'passkey' && authConfig.passkeys?.length > 0) {
    if (!passkeyAssertion) return { confirmed: false, error: 'ERR_PASSKEY_REQUIRED', method: 'passkey' };
    // Passkey validation happens at browser level - trust the assertion if received
    if (!passkeyAssertion.credentialId) {
      isValid = false;
      error = 'ERR_INVALID_PASSKEY';
    } else {
      isValid = true;
    }
  } else {
    // Fallback - no valid 2FA method
    return { confirmed: false, error: 'ERR_2FA_NOT_CONFIGURED', requiresPassword: true };
  }
  
  if (!isValid) {
    // Increment failed attempts
    updateUserMetadata(user.id, { confirm_failed_attempts: failedAttempts + 1 }, ssId);
    const remaining = 5 - failedAttempts - 1;
    return { confirmed: false, error, remainingAttempts: remaining };
  }
  
  // Success - reset failed attempts and update lastAction
  updateUserMetadata(user.id, { confirm_failed_attempts: 0, lastAction: new Date().toISOString() }, ssId);
  return { confirmed: true };
}

// ================================================================= //
// SESSION MANAGEMENT
// ================================================================= //

function generateSessionToken(userId, ssId) {
  const user = getUserById(userId, ssId);
  if (!user) throw new Error('ERR_USER_NOT_FOUND');

  const token = Utilities.getUuid() + '_' + Utilities.getUuid();
  const expiresAt = new Date(Date.now() + SESSION_TTL * 1000).toISOString();

  const sessions = getUserSessions(userId);
  sessions.push({ token, userId, ssId, createdAt: new Date().toISOString(), expiresAt });
  PropertiesService.getUserProperties().setProperty('sessions_' + userId, JSON.stringify(sessions));
  _addToSessionIndex(token, userId, expiresAt);

  return { sessionToken: token, expiresAt, userId };
}

function getUserSessions(userId) {
  const stored = PropertiesService.getUserProperties().getProperty('sessions_' + userId);
  return stored ? JSON.parse(stored) : [];
}

let _sessionIndex = null;

function _loadSessionIndex() {
  if (_sessionIndex) return _sessionIndex;
  const stored = CacheService.getScriptCache().get('session_index');
  _sessionIndex = stored ? JSON.parse(stored) : {};
  return _sessionIndex;
}

function _saveSessionIndex() {
  if (!_sessionIndex) return;
  try { CacheService.getScriptCache().put('session_index', JSON.stringify(_sessionIndex), SESSION_TTL); } catch (e) {}
}

function _addToSessionIndex(token, userId, expiresAt) {
  const idx = _loadSessionIndex();
  idx[token] = { userId, expiresAt };
  _saveSessionIndex();
}

function _removeFromSessionIndex(token) {
  const idx = _loadSessionIndex();
  delete idx[token];
  _saveSessionIndex();
}

function _findSessionInProperties(token) {
  const props = PropertiesService.getUserProperties();
  for (const key of (props.getKeys() || [])) {
    if (!key.startsWith('sessions_')) continue;
    const sessions = JSON.parse(props.getProperty(key) || '[]');
    for (const s of sessions) {
      if (s.token === token && new Date(s.expiresAt) > new Date()) return s;
    }
  }
  return null;
}

function validateSession(token, ssId) {
  const idx = _loadSessionIndex();
  let session = idx[token];

  if (!session) {
    session = _findSessionInProperties(token);
    if (session) { idx[token] = session; _saveSessionIndex(); }
  }

  if (session && new Date(session.expiresAt) > new Date()) {
    const user = ssId ? getUserById(session.userId, ssId) : null;
    return { valid: true, userId: session.userId, username: user?.username, expiresAt: session.expiresAt };
  }

  if (session) { delete idx[token]; _saveSessionIndex(); }
  return { valid: false };
}

function invalidateSession(token) {
  _removeFromSessionIndex(token);
  const props = PropertiesService.getUserProperties();
  for (const key of (props.getKeys() || [])) {
    if (!key.startsWith('sessions_')) continue;
    const sessions = JSON.parse(props.getProperty(key) || '[]');
    const filtered = sessions.filter(s => s.token !== token);
    if (filtered.length !== sessions.length) props.setProperty(key, JSON.stringify(filtered));
  }
}

function refreshSessionToken(token) {
  const props = PropertiesService.getUserProperties();
  for (const key of (props.getKeys() || [])) {
    if (!key.startsWith('sessions_')) continue;
    const sessions = JSON.parse(props.getProperty(key) || '[]');
    const idx = sessions.findIndex(s => s.token === token);
    if (idx === -1) continue;

    const session = sessions[idx];
    if (new Date(session.expiresAt) < new Date()) return { success: false, error: 'ERR_SESSION_EXPIRED' };

    const timeLeft = new Date(session.expiresAt) - new Date();
    if (timeLeft > 60 * 60 * 1000) {
      return { success: true, message: 'Sesión válida', expiresAt: session.expiresAt, needsRefresh: false };
    }

    const newExpiresAt = new Date(Date.now() + SESSION_TTL * 1000).toISOString();
    sessions[idx].expiresAt = newExpiresAt;
    sessions[idx].lastRefresh = new Date().toISOString();
    props.setProperty(key, JSON.stringify(sessions));
    _addToSessionIndex(token, session.userId, newExpiresAt);

    return { success: true, expiresAt: newExpiresAt, needsRefresh: false };
  }
  return { success: false, error: 'ERR_SESSION_NOT_FOUND' };
}

function getActiveSessions(userId) {
  const now = new Date();
  return getUserSessions(userId)
    .filter(s => new Date(s.expiresAt) > now)
    .map(s => ({ token: s.token, createdAt: s.createdAt, expiresAt: s.expiresAt, lastRefresh: s.lastRefresh || null }));
}

function invalidateAllSessions(userId) {
  PropertiesService.getUserProperties().deleteProperty('sessions_' + userId);
  const idx = _loadSessionIndex();
  Object.keys(idx).filter(k => idx[k].userId === userId).forEach(k => delete idx[k]);
  _saveSessionIndex();
  return { success: true, message: 'Todas las sesiones cerradas' };
}

// ================================================================= //
// RBAC — ROLE-BASED ACCESS CONTROL
// ================================================================= //

function getPerfilesSheet(ssId) {
  return SpreadsheetApp.openById(ssId).getSheetByName('Perfiles');
}

function getPerfilById(perfilId, ssId) {
  return getCached('p:id:' + perfilId, () => {
    const sheet = getPerfilesSheet(ssId);
    if (!sheet) return null;
    return getSheetData(sheet).find(row => row.id === perfilId) || null;
  });
}

function getAllPerfiles(ssId) {
  return getCached('p:all', () => {
    const sheet = getPerfilesSheet(ssId);
    if (!sheet) return [];
    return getSheetData(sheet);
  });
}

function normalizePermisos(permisos) {
  if (!permisos) return {};
  if (typeof permisos === 'object') return permisos;
  if (typeof permisos === 'string') { try { return JSON.parse(permisos); } catch (e) { return {}; } }
  return {};
}

/**
 * Resolves a permission from flat or granular format.
 * Flat: "RW" → returns "RW"
 * Granular: {"configuracion":"RW","*":"R"} → returns permiso[key] or permiso['*']
 */
function resolvePermission(modulePerm, sheetName) {
  if (!modulePerm) return null;
  if (typeof modulePerm === 'string') return modulePerm;
  if (typeof modulePerm === 'object') {
    var key = sheetName.toLowerCase();
    return modulePerm[key] || modulePerm['*'] || null;
  }
  return null;
}

function getPermiso(perfilId, modulo, ssId) {
  const perfil = getPerfilById(perfilId, ssId);
  if (!perfil) return null;
  return normalizePermisos(perfil.permisos)[modulo] || null;
}

/**
 * Get merged permissions from all user profiles.
 * Returns: { modulo: { read: bool, write: bool, delete: bool, export: bool } }
 */
function getUserPermissions(userId, ssId) {
  const user = getUserById(userId, ssId);
  if (!user) return {};
  
  // Parse perfilIds
  let perfilIds = [];
  try { perfilIds = user.perfilIds ? JSON.parse(user.perfilIds) : []; } catch (e) { perfilIds = []; }
  
  // Default to p_publicador if empty
  if (!perfilIds.length) perfilIds = ['p_publicador'];
  
  // Merge permissions from all profiles
  const merged = {};
  
  for (const pid of perfilIds) {
    const perfil = getPerfilById(pid, ssId);
    if (!perfil) continue;
    
    const perms = normalizePermisos(perfil.permisos);
    
    for (const [modulo, perm] of Object.entries(perms)) {
      // Handle granular format: { read: true, write: true, ... }
      if (typeof perm === 'object') {
        if (!merged[modulo]) merged[modulo] = { read: false, write: false, delete: false, export: false };
        merged[modulo].read = merged[modulo].read || perm.read || perm.R || perm.RW;
        merged[modulo].write = merged[modulo].write || perm.write || perm.W || perm.RW;
        merged[modulo].delete = merged[modulo].delete || perm.delete || perm.D || perm.RW;
        merged[modulo].export = merged[modulo].export || perm.export || perm.E || perm.RW;
      } 
      // Handle flat format: "RW", "R", "W", "none"
      else if (typeof perm === 'string') {
        if (!merged[modulo]) merged[modulo] = { read: false, write: false, delete: false, export: false };
        const p = perm.toUpperCase();
        if (p === 'RW' || p === 'R') merged[modulo].read = true;
        if (p === 'RW' || p === 'W') { merged[modulo].write = true; merged[modulo].delete = true; }
        if (p === 'RW' || p === 'E') merged[modulo].export = true;
      }
    }
  }
  
  return merged;
}

/**
 * Validate user permission, merging from all profiles.
 * For granular: checks { read, write, delete } directly
 * For flat: maps "R"→read, "W"→write, "RW"→all
 */
function validarPermiso(userId, modulo, accion, ssId, sheetName) {
  const perms = getUserPermissions(userId, ssId);
  const modulePerm = perms[modulo] || perms[sheetName] || perms['*'];
  
  if (!modulePerm) return false;
  
  // Handle granular object format
  if (typeof modulePerm === 'object') {
    if (accion === 'read') return !!modulePerm.read;
    if (accion === 'write') return !!modulePerm.write;
    if (accion === 'delete') return !!modulePerm.delete;
    if (accion === 'export') return !!modulePerm.export;
    return false;
  }
  
  // Handle flat string format
  if (typeof modulePerm === 'string') {
    const p = modulePerm.toUpperCase();
    const map = { read: ['R', 'RW'], write: ['W', 'RW'], delete: ['RW'], export: ['E', 'RW'] };
    return (map[accion] || []).includes(p);
  }
  
  return false;
}

function getUserPermisos(userId, ssId) {
  return getUserPermissions(userId, ssId);
}

function checkPermission(session, action, sheetName, ssId, module) {
  if (!session || !session.valid) return { allowed: false, error: 'ERR_AUTH_INVALID' };
  if (!validarPermiso(session.userId, module || sheetName, action, ssId, sheetName)) {
    logAccess(session.username, false, 'Permiso denegado: ' + action + ' en ' + sheetName, ssId);
    return { allowed: false, error: 'ERR_PERMISSION_DENIED' };
  }
  return { allowed: true };
}

// ================================================================= //
// RATE LIMITING
// ================================================================= //

function checkRateLimit(identifier, maxRequests, windowSeconds) {
  const cache = CacheService.getScriptCache();
  const key = 'rl:' + identifier;
  const current = parseInt(cache.get(key) || '0', 10);
  if (current >= maxRequests) return { allowed: false, remaining: 0, resetIn: windowSeconds };
  cache.put(key, (current + 1).toString(), windowSeconds);
  return { allowed: true, remaining: maxRequests - current - 1, resetIn: windowSeconds };
}

// ================================================================= //
// TOTP
// ================================================================= //

const BASE32_CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';

function generateBase32Secret(size) {
  let secret = '';
  const randomBase = Utilities.getUuid() + Utilities.getUuid();
  for (let i = 0; i < size; i++) secret += BASE32_CHARS.charAt(randomBase.charCodeAt(i % randomBase.length) % 32);
  return secret;
}

function generateTOTPSecret(username) {
  try {
    const secret = generateBase32Secret(20);
    const issuer = 'CongreAdmin';
    return {
      success: true,
      secret,
      otpURI: 'otpauth://totp/' + encodeURIComponent(issuer + ':' + username) + '?secret=' + secret + '&issuer=' + encodeURIComponent(issuer) + '&algorithm=SHA1&digits=6&period=30',
    };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

function base32tohex(base32) {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  let bits = '';
  for (let i = 0; i < base32.length; i++) {
    const val = chars.indexOf(base32[i].toUpperCase());
    if (val !== -1) bits += val.toString(2).padStart(5, '0');
  }
  let hex = '';
  for (let i = 0; i < bits.length; i += 4) hex += '0123456789abcdef'[parseInt(bits.substr(i, 4), 2)];
  return hex;
}

function generateTOTP(secret, timeStepSeconds, digits) {
  return generateTOTPAtTime(secret, Math.floor(Date.now() / 1000), timeStepSeconds, digits);
}

function generateTOTPAtTime(secret, timestamp, timeStepSeconds, digits) {
  const hex = base32tohex(secret);
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < hex.length; i += 2) bytes[i / 2] = parseInt(hex.substr(i, 2), 16);

  let counter = Math.floor(timestamp / timeStepSeconds);
  const counterBytes = new Uint8Array(8);
  for (let i = 7; i >= 0; i--) { counterBytes[i] = counter & 0xff; counter = counter >>> 8; }

  const hmac = Utilities.computeHmacSignature(Utilities.MacAlgorithm.HMAC_SHA_1, counterBytes, bytes);
  const offset = hmac[hmac.length - 1] & 0xf;
  const truncated = (((hmac[offset] & 0x7f) << 24) | ((hmac[offset + 1] & 0xff) << 16) | ((hmac[offset + 2] & 0xff) << 8) | (hmac[offset + 3] & 0xff)) % Math.pow(10, digits);
  return truncated.toString().padStart(digits, '0');
}

function verifyTOTP(secret, code) {
  if (!secret || !code || code.length !== 6 || !/^\d+$/.test(code)) return false;
  const timestamp = Math.floor(Date.now() / 1000);
  for (let i = -1; i <= 1; i++) {
    if (generateTOTPAtTime(secret, timestamp + i * 30, 30, 6) === code) return true;
  }
  return false;
}

// ================================================================= //
// EMAIL
// ================================================================= //

function sendOTPEmail(email, code, congregationName) {
  MailApp.sendEmail({ to: email, subject: 'Código de verificación - Congre-Admin', name: 'Congre-Admin', body: 'Tu código de verificación es: ' + code + '\n\nEste código expira en 10 minutos.\n\nSi no solicitaste este código, puedes ignorar este email.' });
}

function sendWelcomeEmail(email, username, congregationName) {
  MailApp.sendEmail({ to: email, subject: 'Bienvenido a Congre-Admin', name: 'Congre-Admin', body: 'Hola ' + username + ',\n\nTu cuenta en Congre-Admin ha sido creada exitosamente.\n\nCongregación: ' + congregationName + '\nUsuario: ' + username + '\n\nYa puedes iniciar sesión en la aplicación.' });
}

function sendPasswordResetEmail(email, username, resetLink) {
  MailApp.sendEmail({ to: email, subject: 'Restablecer contraseña - Congre-Admin', name: 'Congre-Admin', body: 'Hola ' + username + ',\n\nHas solicitado restablecer tu contraseña.\n\nEnlace para crear una nueva contraseña:\n' + resetLink + '\n\nEste enlace expirará en 1 hora.\n\nSi no solicitaste este cambio, ignora este email.' });
}

function sendPasswordChangedEmail(email, username) {
  MailApp.sendEmail({ to: email, subject: 'Contraseña actualizada - Congre-Admin', name: 'Congre-Admin', body: 'Hola ' + username + ',\n\nTu contraseña ha sido actualizada exitosamente.\n\nSi no realizaste este cambio, contacta al administrador inmediatamente.' });
}

function verifyEmailOTP(username, code) {
  try {
    const stored = PropertiesService.getUserProperties().getProperty('otp_' + username);
    if (!stored) return false;
    const otpData = JSON.parse(stored);
    if (new Date(otpData.expiresAt) < new Date() || otpData.code !== code) return false;
    PropertiesService.getUserProperties().deleteProperty('otp_' + username);
    return true;
  } catch (e) { return false; }
}

// ================================================================= //
// AUDIT LOGGING
// ================================================================= //

function logAccess(username, success, details, ssId) {
  try {
    if (!ssId) return;
    const ss = SpreadsheetApp.openById(ssId);
    let sheet = ss.getSheetByName('Logs_Accesos');
    if (!sheet) {
      sheet = ss.insertSheet('Logs_Accesos');
      sheet.appendRow(['timestamp', 'username', 'success', 'details', 'ip']);
    }
    sheet.appendRow([new Date().toISOString(), username, success ? 'YES' : 'NO', details, 'SERVER']);
  } catch (err) { Logger.log('Error guardando log: ' + err.message); }
}

// ================================================================= //
// INSTALLATION
// ================================================================= //

function createSpreadsheet(name) {
  try {
    const ss = SpreadsheetApp.create(name || 'CongreAdmin');
    return { success: true, ssId: ss.getId(), url: ss.getUrl(), name: ss.getName() };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Creates Core and Public spreadsheets inside a dedicated Drive folder.
 * Returns IDs/URLs — frontend handles all storage.
 * NO script properties used — fully multi-tenant.
 */
function actionInstall(payload) {
  try {
    const { nombreCongregacion, numeroCongregacion, nombreMostrar, gasUrl } = payload;
    const nombreLimpio = (nombreCongregacion || 'SinNombre').replace(/[^a-zA-Z0-9]/g, '');

    // 1. Create Drive folder with subfolders
    const folder = DriveApp.createFolder('CongreAdmin-' + nombreLimpio);
    folder.createFolder('backups');
    folder.createFolder('documentos');
    folder.createFolder('exportaciones');

    // 2. Create spreadsheets
    const ssResult = createSpreadsheet('CongreAdmin-' + nombreLimpio + '-Core');
    if (!ssResult.success) return { success: false, error: 'Error creando spreadsheet: ' + ssResult.error };

    const ssPublicResult = createSpreadsheet('CongreAdmin-' + nombreLimpio + '-Public');
    if (!ssPublicResult.success) return { success: false, error: 'Error creando spreadsheet público: ' + ssPublicResult.error };

    // 3. Move spreadsheets into folder
    DriveApp.getFileById(ssResult.ssId).moveTo(folder);
    DriveApp.getFileById(ssPublicResult.ssId).moveTo(folder);

    // 4. Share public spreadsheet
    DriveApp.getFileById(ssPublicResult.ssId).setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW);

    return {
      success: true,
      ssId: ssResult.ssId,
      ssUrl: ssResult.url,
      publicSsId: ssPublicResult.ssId,
      publicSsUrl: ssPublicResult.url,
      folderId: folder.getId(),
      folderUrl: folder.getUrl(),
      nombreCongregacion,
      numeroCongregacion,
      nombreMostrar: nombreMostrar || 'Co. ' + nombreCongregacion,
      message: 'Spreadsheets y carpeta Drive creados. Configura tablas y datos desde el frontend.',
    };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Internal helper: upload a file for batchExecute.
 * Returns { fileId, fileUrl, fileName, size } or { error }.
 */
function _batchUploadFile(folderId, op) {
  if (!op.content) return { error: 'ERR_INVALID_REQUEST: Content is required' };
  if (!op.fileName) return { error: 'ERR_INVALID_REQUEST: FileName is required' };

  const mimeType = op.mimeType || 'application/octet-stream';
  if (ALLOWED_MIMETYPES.indexOf(mimeType) === -1) return { error: 'ERR_INVALID_MIMETYPE: ' + mimeType };
  if (op.content.length * 0.75 > FILE_MAX_SIZE) return { error: 'ERR_FILE_TOO_LARGE: Max 37MB' };

  let decodedContent;
  try { decodedContent = Utilities.base64Decode(op.content); }
  catch (e) { return { error: 'ERR_INVALID_BASE64: Content is not valid base64' }; }

  const targetFolder = resolveTargetFolder(folderId, op.subfolder);
  if (!targetFolder) return { error: op.subfolder ? 'ERR_SUBFOLDER_NOT_FOUND' : 'ERR_FOLDER_NOT_FOUND' };

  const blob = Utilities.newBlob(decodedContent, mimeType, op.fileName);
  const file = targetFolder.createFile(blob);
  return { fileId: file.getId(), fileUrl: file.getUrl(), fileName: file.getName(), size: file.getSize() };
}

// ================================================================= //
// FILE MANAGEMENT — Drive Folder System
// ================================================================= //

const FILE_MAX_SIZE = 37 * 1024 * 1024; // 37MB (GAS doPost limit)

const ALLOWED_MIMETYPES = [
  'application/pdf',
  'image/jpeg', 'image/png', 'image/gif', 'image/svg+xml', 'image/webp',
  'text/plain', 'text/csv', 'text/html',
  'application/json',
  'application/zip',
  'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  'application/vnd.ms-excel',
  'application/msword',
  'application/vnd.oasis.opendocument.text',
  'application/vnd.oasis.opendocument.spreadsheet',
  'audio/mpeg', 'audio/wav', 'audio/ogg',
  'video/mp4', 'video/webm',
];

/**
 * Helper: resolve target folder (main folder or subfolder).
 */
function resolveTargetFolder(folderId, subfolder) {
  const folder = DriveApp.getFolderById(folderId);
  if (!subfolder) return folder;
  const subfolders = folder.getFoldersByName(subfolder);
  if (!subfolders.hasNext()) return null;
  return subfolders.next();
}

/**
 * Helper: validate session for file actions.
 */
function validateFileSession(sessionToken, ssId) {
  if (!sessionToken) return { error: 'ERR_AUTH_REQUIRED' };
  const session = validateSession(sessionToken, ssId);
  if (!session.valid) return { error: 'ERR_AUTH_INVALID' };
  const permCheck = checkPermission(session, 'write', 'core', ssId);
  if (!permCheck.allowed) return { error: permCheck.error };
  return { session };
}

/**
 * Standalone action: Lists files in the installation Drive folder.
 * Thin wrapper — same logic used by batchExecute.
 */
function actionListFolderFiles(payload, sessionToken, ssId) {
  const auth = validateFileSession(sessionToken, ssId);
  if (auth.error) return { success: false, error: auth.error };

  const targetFolder = resolveTargetFolder(payload.folderId, payload.subfolder);
  if (!targetFolder) return { success: false, error: payload.subfolder ? 'ERR_SUBFOLDER_NOT_FOUND' : 'ERR_FOLDER_NOT_FOUND' };

  const files = targetFolder.getFiles();
  const result = [];
  while (files.hasNext()) {
    const f = files.next();
    const sharing = f.getSharingAccess();
    const permission = f.getSharingPermission();
    result.push({
      id: f.getId(),
      name: f.getName(),
      mimeType: f.getMimeType(),
      size: f.getSize(),
      created: f.getDateCreated().toISOString(),
      modified: f.getLastUpdated().toISOString(),
      url: f.getUrl(),
      shared: sharing !== DriveApp.Access.PRIVATE,
      access: sharing,
      permission: permission,
    });
  }

  return { success: true, files: result };
}

/**
 * Standalone action: Uploads a base64-encoded file.
 * Thin wrapper — same logic used by batchExecute.
 */
function actionUploadFile(payload, sessionToken, ssId) {
  const auth = validateFileSession(sessionToken, ssId);
  if (auth.error) return { success: false, error: auth.error };

  const uploadResult = _batchUploadFile(payload.folderId, payload);
  if (uploadResult.error) return { success: false, error: uploadResult.error };

  return { success: true, fileId: uploadResult.fileId, fileUrl: uploadResult.fileUrl, fileName: uploadResult.fileName, size: uploadResult.size };
}

/**
 * Standalone action: Downloads a file from Drive as base64.
 * Same logic used by batchExecute.
 */
function actionDownloadFile(payload, sessionToken, ssId) {
  const auth = validateFileSession(sessionToken, ssId);
  if (auth.error) return { success: false, error: auth.error };

  if (!payload.fileId) return { success: false, error: 'ERR_INVALID_REQUEST: FileId is required' };

  try {
    const file = DriveApp.getFileById(payload.fileId);
    const blob = file.getBlob();
    const bytes = blob.getBytes();
    return { success: true, fileName: file.getName(), mimeType: file.getMimeType(), size: bytes.length, content: Utilities.base64Encode(bytes) };
  } catch (e) {
    return { success: false, error: 'ERR_FILE_NOT_FOUND: ' + e.message };
  }
}

/**
 * Standalone action: Deletes (trashes) a file.
 * Same logic used by batchExecute.
 */
function actionDeleteFile(payload, sessionToken, ssId) {
  const auth = validateFileSession(sessionToken, ssId);
  if (auth.error) return { success: false, error: auth.error };

  if (!payload.fileId) return { success: false, error: 'ERR_INVALID_REQUEST: FileId is required' };

  try {
    DriveApp.getFileById(payload.fileId).setTrashed(true);
    return { success: true, message: 'Archivo eliminado' };
  } catch (e) {
    return { success: false, error: 'ERR_FILE_NOT_FOUND: ' + e.message };
  }
}

/**
 * Standalone action: Sets sharing permissions on a file.
 * Same logic used by batchExecute.
 */
function actionSetFileSharing(payload, sessionToken, ssId) {
  const auth = validateFileSession(sessionToken, ssId);
  if (auth.error) return { success: false, error: auth.error };

  if (!payload.fileId) return { success: false, error: 'ERR_INVALID_REQUEST: FileId is required' };

  const accessMap = {
    'PRIVATE': DriveApp.Access.PRIVATE,
    'ANYONE_WITH_LINK': DriveApp.Access.ANYONE_WITH_LINK,
    'DOMAIN': DriveApp.Access.DOMAIN,
    'ANYONE': DriveApp.Access.ANYONE,
  };
  const permissionMap = {
    'VIEW': DriveApp.Permission.VIEW,
    'COMMENT': DriveApp.Permission.COMMENT,
    'EDIT': DriveApp.Permission.EDIT,
  };

  const access = accessMap[payload.access];
  const permission = permissionMap[payload.permission || 'VIEW'];

  if (!access) return { success: false, error: 'ERR_INVALID_REQUEST: Invalid access level. Use PRIVATE, ANYONE_WITH_LINK, DOMAIN, or ANYONE' };
  if (!permission) return { success: false, error: 'ERR_INVALID_REQUEST: Invalid permission. Use VIEW, COMMENT, or EDIT' };

  try {
    const file = DriveApp.getFileById(payload.fileId);
    file.setSharing(access, permission);
    return { success: true, fileId: file.getId(), access: payload.access, permission: payload.permission || 'VIEW', shareUrl: file.getUrl() + '?usp=sharing' };
  } catch (e) {
    return { success: false, error: 'ERR_FILE_NOT_FOUND: ' + e.message };
  }
}

/**
 * Standalone action: Moves a file into the installation Drive folder.
 * Same logic used by batchExecute.
 */
function actionMoveFileToFolder(payload, sessionToken, ssId) {
  const auth = validateFileSession(sessionToken, ssId);
  if (auth.error) return { success: false, error: auth.error };

  if (!payload.fileId) return { success: false, error: 'ERR_INVALID_REQUEST: FileId is required' };

  const targetFolder = resolveTargetFolder(payload.folderId, payload.subfolder);
  if (!targetFolder) return { success: false, error: payload.subfolder ? 'ERR_SUBFOLDER_NOT_FOUND' : 'ERR_FOLDER_NOT_FOUND' };

  try {
    const file = DriveApp.getFileById(payload.fileId);
    file.moveTo(targetFolder);
    return { success: true, fileId: file.getId(), fileName: file.getName(), folderId: targetFolder.getId(), fileUrl: file.getUrl() };
  } catch (e) {
    return { success: false, error: 'ERR_FILE_NOT_FOUND: ' + e.message };
  }
}
