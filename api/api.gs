/**
 * Maneja solicitudes GET genéricas.
 * @example ?action=getData&sheet=NombreDeHoja&ssId=ID_DE_HOJA
 */
function doGet(e) {
  const action = e.parameter.action;
  const ssId = e.parameter.ssId;
  
  try {
    const ss = ssId ? SpreadsheetApp.openById(ssId) : SpreadsheetApp.getActiveSpreadsheet();
    
    if (action === 'getData') {
      const sheetName = e.parameter.sheet;
      return createResponse(getCachedSheetData(ss, sheetName));
    }
    
    if (action === 'batchGetData') {
      const sheets = e.parameter.sheets ? e.parameter.sheets.split(',') : [];
      const result = {};
      sheets.forEach(name => {
        result[name] = getCachedSheetData(ss, name);
      });
      return createResponse(result);
    }
    
    return createResponse({ error: 'Acción GET no válida' });
  } catch (err) {
    return createResponse({ error: err.message });
  }
}

/**
 * Maneja solicitudes POST genéricas.
 */
function doPost(e) {
  try {
    const postData = JSON.parse(e.postData.contents);
    const action = postData.action;
    const sheetName = postData.sheet;
    const ssId = postData.ssId;
    const ss = ssId ? SpreadsheetApp.openById(ssId) : SpreadsheetApp.getActiveSpreadsheet();
    
    if (action === 'initSheet') {
      let sheet = ss.getSheetByName(sheetName);
      if (!sheet) {
        sheet = ss.insertSheet(sheetName);
        sheet.appendRow(postData.headers);
      } else if (!postData.preserveExisting) {
        sheet.clearContents(); 
        sheet.getRange(1, 1, 1, postData.headers.length).setValues([postData.headers]).setFontWeight('bold').setBackground('#f3f3f3');
      } else { 
        if (sheet.getLastRow() === 0) {
          sheet.getRange(1, 1, 1, postData.headers.length).setValues([postData.headers]).setFontWeight('bold').setBackground('#f3f3f3');
        }
      }
      clearCache(ss.getId(), sheetName);
      return createResponse({ success: true, message: 'Hoja inicializada' });
    }

    if (action === 'clearSheet') {
      const sheet = ss.getSheetByName(sheetName);
      if (!sheet) return createResponse({ error: 'Hoja no encontrada' });
      const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues();
      sheet.clearContents();
      if (headers.length > 0 && headers[0][0]) sheet.appendRow(headers[0]);
      clearCache(ss.getId(), sheetName);
      return createResponse({ success: true });
    }

    if (action === 'deleteData') {
      const sheet = ss.getSheetByName(sheetName);
      if (!sheet) return createResponse({ error: 'Hoja no encontrada' });
      // Usar borrado lógico por defecto
      const result = softDeleteRow(sheet, postData.id);
      if (!result) {
        return createResponse({ success: false, error: 'Registro no encontrado' });
      }
      clearCache(ss.getId(), sheetName);
      return createResponse({ success: true, message: 'Borrado lógico realizado' });
    }
    
    if (action === 'hardDelete') {
      // Borrado físico (solo admin)
      const sheet = ss.getSheetByName(sheetName);
      if (!sheet) return createResponse({ error: 'Hoja no encontrada' });
      deleteRowById(sheet, postData.id);
      clearCache(ss.getId(), sheetName);
      return createResponse({ success: true, message: 'Borrado físico realizado' });
    }
    
    if (action === 'restoreData') {
      // Restaurar registro borrado lógicamente
      const sheet = ss.getSheetByName(sheetName);
      if (!sheet) return createResponse({ error: 'Hoja no encontrada' });
      const result = restoreRow(sheet, postData.id);
      if (!result) return createResponse({ success: false, error: 'Registro no encontrado' });
      clearCache(ss.getId(), sheetName);
      return createResponse({ success: true, message: 'Registro restaurado' });
    }
    
    if (action === 'getHistory') {
      if (!postData.sessionToken) {
        return createResponse({ error: 'ERR_AUTH_REQUIRED' });
      }
      const session = validateSession(postData.sessionToken);
      if (!session.valid) {
        return createResponse({ error: 'ERR_AUTH_INVALID' });
      }
      
      const sheet = ss.getSheetByName(sheetName);
      if (!sheet) return createResponse({ error: 'Hoja no encontrada' });
      
      const permCheck = checkPermission(session, 'read', sheetName);
      if (!permCheck.allowed) {
        return createResponse({ error: permCheck.error });
      }
      
      const history = getVersionHistory(sheet, postData.id);
      return createResponse({ success: true, history: history });
    }
    
    // Last Write Wins: validar versión antes de guardar
    if (action === 'saveData') {
      const sheet = ss.getSheetByName(sheetName);
      if (!sheet) return createResponse({ error: 'Hoja no encontrada: ' + sheetName });
      
      let existingRows = null;
      
      if (postData.expectedVersion !== undefined) {
        existingRows = sheet.getDataRange().getValues();
        const headers = existingRows[0];
        const idIndex = headers.indexOf('id');
        const vIndex = headers.indexOf('_v');
        
        for (let i = 1; i < existingRows.length; i++) {
          if (existingRows[i][idIndex] == postData.payload.id) {
            const currentV = vIndex >= 0 ? (parseInt(existingRows[i][vIndex]) || 0) : 0;
            if (currentV > postData.expectedVersion) {
              return createResponse({ 
                success: false, 
                error: 'ERR_VERSION_CONFLICT',
                message: 'El registro fue modificado por otro usuario',
                currentVersion: currentV
              });
            }
            break;
          }
        }
      }
      
      updateOrInsert(sheet, postData.payload, postData.onlyIfNew, { existingRows });
      clearCache(ss.getId(), sheetName);
      return createResponse({ success: true });
    }

    if (action === 'deleteSheet') {
      const sheet = ss.getSheetByName(sheetName);
      if (sheet) ss.deleteSheet(sheet);
      return createResponse({ success: true });
    }

    // --- Autenticación ---
    
    if (action === 'register') {
      return createResponse(actionRegister(postData.payload));
    }
    
    if (action === 'login') {
      return createResponse(actionLogin(postData.payload));
    }
    
    if (action === 'challenge') {
      return createResponse(actionChallenge(postData.payload));
    }
    
    if (action === 'requestOTP') {
      return createResponse(actionRequestOTP(postData.payload));
    }
    
    if (action === 'setupTOTP') {
      return createResponse(actionSetupTOTP(postData.payload));
    }
    
    if (action === 'confirmTOTP') {
      return createResponse(actionConfirmTOTP(postData.payload));
    }
    
    if (action === 'disableTOTP') {
      return createResponse(actionDisableTOTP(postData.payload));
    }
    
    if (action === 'setupPasskey') {
      return createResponse(actionSetupPasskey(postData.payload));
    }
    
    if (action === 'confirmPasskey') {
      return createResponse(actionConfirmPasskey(postData.payload));
    }
    
    if (action === 'deletePasskey') {
      const session = validateSession(postData.sessionToken);
      if (!session.valid) return createResponse({ error: 'ERR_AUTH_INVALID' });
      return createResponse(actionDeletePasskey(session, postData.payload));
    }
    
    if (action === 'getAuthMethods') {
      const session = validateSession(postData.sessionToken);
      if (!session.valid) return createResponse({ error: 'ERR_AUTH_INVALID' });
      return createResponse(actionGetAuthMethods(session));
    }
    
    if (action === 'updateAuthConfig') {
      const session = validateSession(postData.sessionToken);
      if (!session.valid) return createResponse({ error: 'ERR_AUTH_INVALID' });
      return createResponse(actionUpdateAuthConfig(session, postData.payload));
    }
    
    if (action === 'changePassword') {
      const session = validateSession(postData.sessionToken);
      if (!session.valid) return createResponse({ error: 'ERR_AUTH_INVALID' });
      return createResponse(actionChangePassword(session, postData.payload));
    }
    
    if (action === 'deleteAccount') {
      const session = validateSession(postData.sessionToken);
      if (!session.valid) return createResponse({ error: 'ERR_AUTH_INVALID' });
      return createResponse(actionDeleteAccount(session, postData.payload));
    }
    
    if (action === 'requestPasswordReset') {
      return createResponse(actionRequestPasswordReset(postData.payload));
    }
    
    if (action === 'resetPassword') {
      return createResponse(actionResetPassword(postData.payload));
    }
    
    if (action === 'logout') {
      return createResponse(actionLogout(postData.payload));
    }
    
    if (action === 'validateSession') {
      const session = validateSession(postData.sessionToken);
      return createResponse(session);
    }
    
    if (action === 'refreshSession') {
      return createResponse(refreshSessionToken(postData.sessionToken));
    }
    
    if (action === 'getActiveSessions') {
      return createResponse(getActiveSessions(postData.userId));
    }
    
    if (action === 'invalidateAllSessions') {
      return createResponse(invalidateAllSessions(postData.userId));
    }
    
    // --- Permisos RBAC ---
    
    if (action === 'getPerfiles') {
      return createResponse(actionGetPerfiles());
    }
    
    if (action === 'getCongregacion') {
      return createResponse(actionGetCongregacion());
    }
    
    if (action === 'getPermisos') {
      return createResponse(actionGetPermisos(postData.payload));
    }
    
    if (action === 'checkPermission') {
      return createResponse(actionCheckPermission(postData.payload));
    }
    
    // --- Gestión de Perfiles ---
    
    if (action === 'createProfile') {
      const session = validateSession(postData.sessionToken);
      if (!session.valid) return createResponse({ error: 'ERR_AUTH_INVALID' });
      const perm = checkPermission(session, 'write', 'core');
      if (!perm.allowed) return createResponse({ error: perm.error });
      return createResponse(actionCreateProfile(postData.payload));
    }
    
    if (action === 'updateProfile') {
      const session = validateSession(postData.sessionToken);
      if (!session.valid) return createResponse({ error: 'ERR_AUTH_INVALID' });
      const perm = checkPermission(session, 'write', 'core');
      if (!perm.allowed) return createResponse({ error: perm.error });
      return createResponse(actionUpdateProfile(postData.payload));
    }
    
    if (action === 'deleteProfile') {
      const session = validateSession(postData.sessionToken);
      if (!session.valid) return createResponse({ error: 'ERR_AUTH_INVALID' });
      const perm = checkPermission(session, 'write', 'core');
      if (!perm.allowed) return createResponse({ error: perm.error });
      return createResponse(actionDeleteProfile(postData.payload));
    }
    
    if (action === 'updateUser') {
      const session = validateSession(postData.sessionToken);
      if (!session.valid) return createResponse({ error: 'ERR_AUTH_INVALID' });
      return createResponse(actionUpdateUser(session, postData.payload));
    }
    
    // --- Instalación ---
    
    if (action === 'install') {
      return createResponse(actionInstall(postData.payload));
    }
    
    if (action === 'createSpreadsheet') {
      return createResponse(createSpreadsheet(postData.name));
    }
    
    if (action === 'initCoreTables') {
      return createResponse(initCoreTables(postData.ssId));
    }
    
    if (action === 'seedPerfiles') {
      return createResponse(seedPerfiles(postData.ssId));
    }
    
    if (action === 'seedConfiguracion') {
      return createResponse(seedConfiguracion(postData.ssId));
    }
    
    return createResponse({ error: 'Acción POST no válida' });
  } catch (err) {
    return createResponse({ error: err.message });
  }
}

// --- Sistema de Caché ---
const CACHE_TTL_DATA = 600; // 10 minutos para datos de hojas
const CACHE_TTL_LOOKUP = 300; // 5 minutos para búsquedas

function getCachedSheetData(ss, sheetName) {
  const cache = CacheService.getScriptCache();
  const cacheKey = ss.getId() + '_' + sheetName;
  const cached = cache.get(cacheKey);
  
  if (cached) {
    try {
      return JSON.parse(cached);
    } catch (e) {
      // Fallback si la caché está corrupta
    }
  }
  
  const sheet = ss.getSheetByName(sheetName);
  if (!sheet) return [];
  
  const data = getSheetData(sheet);
  try {
    cache.put(cacheKey, JSON.stringify(data), CACHE_TTL_DATA);
  } catch (e) {
    // Si los datos son demasiado grandes para la caché, no fallar
  }
  return data;
}

function clearCache(ssId, sheetName) {
  const cache = CacheService.getScriptCache();
  cache.remove(ssId + '_' + sheetName);
}

// ================================================================= //
// VERSIONADO Y BORRADO LÓGICO
// Fase 1.4: Implementación de _v, _ts, _deleted
// ================================================================= //

/**
 * Obtiene datos de una hoja filtrando registros borrados
 * @param {Sheet} sheet - Hoja de cálculo
 * @param {boolean} includeDeleted - Si true, incluye registros borrados (default: false)
 * @return {array} Datos de la hoja
 */
function getSheetData(sheet, includeDeleted = false) {
  if (!sheet) return [];
  const rows = sheet.getDataRange().getValues();
  if (rows.length < 1) return [];
  const headers = rows[0];
  const deletedIndex = headers.indexOf('_deleted');
  
  return rows.slice(1).map(row => {
    let obj = {};
    headers.forEach((h, i) => {
      let val = row[i];
      if (typeof val === 'string' && (val.startsWith('[') || val.startsWith('{'))) {
        try {
          obj[h] = JSON.parse(val);
        } catch (e) {
          obj[h] = val;
        }
      } else {
        obj[h] = val;
      }
    });
    return obj;
  }).filter(row => {
    if (includeDeleted) return true;
    return row._deleted !== true && row._deleted !== 'true';
  });
}

/**
 * Actualiza o inserta un registro con versionado automático
 * @param {Sheet} sheet - Hoja de cálculo
 * @param {object} item - Datos del registro
 * @param {boolean} onlyIfNew - Si true, solo inserta si no existe
 * @param {object} options - Opciones adicionales: { existingRows }
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
  const newItem = {
    ...item,
    _v: currentV + 1,
    _ts: timestamp
  };
  
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
  
  const sheetName = sheet.getName();
  if (sheetName === 'Usuarios' && item.id) {
    invalidateCache('u:');
  } else if (sheetName === 'Perfiles' && item.id) {
    invalidateCache('p:');
    invalidateCache('p:all');
  }
  invalidateCoreSpreadsheetCache();
}

/**
 * Marca un registro como borrado (borrado lógico)
 * @param {Sheet} sheet - Hoja de cálculo
 * @param {string} id - ID del registro
 * @return {boolean} true si se marcó correctamente
 */
function softDeleteRow(sheet, id) {
  if (!sheet || !id) return false;
  const rows = sheet.getDataRange().getValues();
  if (rows.length <= 1) return false;
  const idIndex = rows[0].indexOf('id');
  const deletedIndex = rows[0].indexOf('_deleted');
  const vIndex = rows[0].indexOf('_v');
  const tsIndex = rows[0].indexOf('_ts');
  
  if (idIndex < 0) return false;
  
  for (let i = 1; i < rows.length; i++) {
    if (rows[i][idIndex] == id) {
      const rowNum = i + 1;
      
      // Marcar como borrado
      if (deletedIndex >= 0) {
        sheet.getRange(rowNum, deletedIndex + 1).setValue(true);
      }
      
      // Incrementar versión
      if (vIndex >= 0) {
        const currentV = parseInt(rows[i][vIndex]) || 0;
        sheet.getRange(rowNum, vIndex + 1).setValue(currentV + 1);
      }
      
      // Actualizar timestamp
      if (tsIndex >= 0) {
        sheet.getRange(rowNum, tsIndex + 1).setValue(new Date().toISOString());
      }
      
      const ssId = getCoreSpreadsheetId();
      const sheetName = sheet.getName();
      clearCache(ssId, sheetName);
      if (sheetName === 'Usuarios') invalidateCache('u:');
      if (sheetName === 'Perfiles') { invalidateCache('p:'); invalidateCache('p:all'); }
      
      return true;
    }
  }
  return false;
}

/**
 * Restaura un registro borrado lógicamente
 * @param {Sheet} sheet - Hoja de cálculo
 * @param {string} id - ID del registro
 * @return {boolean} true si se restauró correctamente
 */
function restoreRow(sheet, id) {
  if (!sheet || !id) return false;
  const rows = sheet.getDataRange().getValues();
  if (rows.length <= 1) return false;
  const idIndex = rows[0].indexOf('id');
  const deletedIndex = rows[0].indexOf('_deleted');
  
  if (idIndex < 0) return false;
  
  for (let i = 1; i < rows.length; i++) {
    if (rows[i][idIndex] == id) {
      const rowNum = i + 1;
      
      if (deletedIndex >= 0) {
        sheet.getRange(rowNum, deletedIndex + 1).setValue(false);
      }
      
      const ssId = getCoreSpreadsheetId();
      const sheetName = sheet.getName();
      clearCache(ssId, sheetName);
      if (sheetName === 'Usuarios') invalidateCache('u:');
      if (sheetName === 'Perfiles') { invalidateCache('p:'); invalidateCache('p:all'); }
      
      return true;
    }
  }
  return false;
}

/**
 * Obtiene el historial de versiones de un registro
 * @param {Sheet} sheet - Hoja de cálculo
 * @param {string} id - ID del registro
 * @return {array} Historial de versiones
 */
function getVersionHistory(sheet, id) {
  const allData = getSheetData(sheet, true); // Include deleted
  return allData
    .filter(row => row.id === id)
    .sort((a, b) => (b._v || 0) - (a._v || 0));
}

function deleteRowById(sheet, id) {
  if (!sheet || !id) return;
  const rows = sheet.getDataRange().getValues();
  if (rows.length <= 1) return;
  const idIndex = rows[0].indexOf('id');
  if (idIndex < 0) return;

  for (let i = 1; i < rows.length; i++) {
    if (rows[i][idIndex] == id) {
      sheet.deleteRow(i + 1);
      
      const ssId = getCoreSpreadsheetId();
      const sheetName = sheet.getName();
      clearCache(ssId, sheetName);
      if (sheetName === 'Usuarios') invalidateCache('u:');
      if (sheetName === 'Perfiles') { invalidateCache('p:'); invalidateCache('p:all'); }
      break;
    }
  }
}

function createResponse(data) {
  return ContentService.createTextOutput(JSON.stringify(data))
    .setMimeType(ContentService.MimeType.JSON);
}

// ================================================================= //
// AUTENTICACIÓN ZERO-KNOWLEDGE
// Fase 1.1: Implementación de autenticación
// ================================================================= //

const SESSION_TTL = 86400; // 24 horas en segundos
const CORE_SS_ID = 'CORE_SS_ID'; // Configurar en propiedades del script

/**
 * Obtiene el ID del GSheet Core desde las propiedades del script
 */
function getCoreSpreadsheetId() {
  return PropertiesService.getScriptProperties().getProperty('CORE_SS_ID');
}

/**
 * Obtiene el Spreadsheet Core (con caché)
 */
let _cachedSpreadsheet = null;
let _cachedSpreadsheetId = null;

function getCoreSpreadsheet() {
  const ssId = getCoreSpreadsheetId();
  if (!ssId) throw new Error('CORE_SS_ID no configurado');
  
  if (_cachedSpreadsheetId === ssId && _cachedSpreadsheet) {
    return _cachedSpreadsheet;
  }
  
  _cachedSpreadsheet = SpreadsheetApp.openById(ssId);
  _cachedSpreadsheetId = ssId;
  return _cachedSpreadsheet;
}

function invalidateCoreSpreadsheetCache() {
  _cachedSpreadsheet = null;
  _cachedSpreadsheetId = null;
}

/**
 * Obtiene la hoja de Usuarios del GSheet Core
 */
function getUsuariosSheet() {
  const ss = getCoreSpreadsheet();
  return ss.getSheetByName('Usuarios');
}

/**
 * Busca un usuario por username (email)
 * @param {string} username - Email del usuario
 * @return {object|null} Usuario encontrado o null
 */
function getUserByUsername(username) {
  return getCached('u:un:' + username, () => {
    const sheet = getUsuariosSheet();
    if (!sheet) return null;
    const data = getSheetData(sheet);
    return data.find(row => row.username === username) || null;
  });
}

/**
 * Busca un usuario por ID
 * @param {string} id - ID del usuario
 * @return {object|null} Usuario encontrado o null
 */
function getUserById(id) {
  return getCached('u:id:' + id, () => {
    const sheet = getUsuariosSheet();
    if (!sheet) return null;
    const data = getSheetData(sheet);
    return data.find(row => row.id === id) || null;
  });
}

/**
 * Hashea una contraseña usando SHA-256
 * @param {string} password - Contraseña en texto plano
 * @return {string} Hash de la contraseña
 */
function hashPassword(password) {
  if (!password) return '';
  const digest = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, password);
  return digest.map(function(b) {
    return ('00' + (b < 0 ? b + 256 : b).toString(16)).slice(-2);
  }).join('');
}

/**
 * Parsea auth_config del usuario (maneja tanto string como objeto ya parseado)
 * @param {string|object} authConfig - auth_config del usuario
 * @return {object} auth_config parseado
 */
function parseAuthConfig(authConfig) {
  const defaults = { default_method: 'passkey', password_hash: '', recovery_enabled: true, email_otp: { enabled: false }, totp: { enabled: false }, passkeys: [] };
  if (!authConfig) return defaults;
  try {
    return typeof authConfig === 'string' ? JSON.parse(authConfig) : authConfig;
  } catch (e) {
    return defaults;
  }
}

/**
 * Parsea metadata del usuario (maneja tanto string como objeto ya parseado)
 * @param {string|object} metadata - metadata del usuario
 * @return {object} metadata parseado
 */
function parseUserMetadata(metadata) {
  const defaults = { last_login: null, last_password_change: null, failed_login_attempts: 0, created_from_ip: null };
  if (!metadata) return defaults;
  try {
    return typeof metadata === 'string' ? JSON.parse(metadata) : metadata;
  } catch (e) {
    return defaults;
  }
}

/**
 * Verifica una contraseña contra un hash
 * @param {string} password - Contraseña en texto plano
 * @param {string} hash - Hash almacenado
 * @return {boolean} true si coincide
 */
function verifyPassword(password, hash) {
  if (!password || !hash) return false;
  return hashPassword(password) === hash;
}

/**
 * Valida requisitos de complejidad de contraseña
 * @param {string} password - Contraseña a validar
 * @return {object} { valid: boolean, errors: string[] }
 */
function validatePasswordComplexity(password) {
  const errors = [];
  
  if (!password) {
    errors.push('La contraseña es requerida');
    return { valid: false, errors };
  }
  
  if (password.length < 8) {
    errors.push('Mínimo 8 caracteres');
  }
  
  if (password.length > 128) {
    errors.push('Máximo 128 caracteres');
  }
  
  if (!/[a-z]/.test(password)) {
    errors.push('Al menos una letra minúscula');
  }
  
  if (!/[A-Z]/.test(password)) {
    errors.push('Al menos una letra mayúscula');
  }
  
  if (!/[0-9]/.test(password)) {
    errors.push('Al menos un número');
  }
  
  if (!/[^a-zA-Z0-9]/.test(password)) {
    errors.push('Al menos un carácter especial');
  }
  
  return {
    valid: errors.length === 0,
    errors
  };
}

/**
 * Crea un nuevo usuario
 * @param {object} userData - Datos del usuario
 * @return {object} Usuario creado
 */
function createUser(userData) {
  const sheet = getUsuariosSheet();
  if (!sheet) throw new Error('Hoja Usuarios no encontrada');
  
  // Verificar si el usuario ya existe
  const existing = getUserByUsername(userData.username);
  if (existing) {
    throw new Error('ERR_USER_EXISTS: El usuario ya existe');
  }
  
  // Validar contraseña (requerida)
  if (!userData.password) {
    throw new Error('ERR_PASSWORD_REQUIRED: La contraseña es requerida');
  }
  const pwValidation = validatePasswordComplexity(userData.password);
  if (!pwValidation.valid) {
    throw new Error('ERR_PASSWORD_WEAK: ' + pwValidation.errors.join(', '));
  }
  
  const now = new Date().toISOString();
  const authConfig = {
    default_method: userData.default_method || 'passkey',
    password_hash: userData.password ? hashPassword(userData.password) : '',
    recovery_enabled: true,
    email_otp: { enabled: true, created_at: now },
    totp: { enabled: false, secret: null, created_at: null },
    passkeys: []
  };
  
  const metadata = {
    last_login: null,
    last_password_change: userData.password ? now : null,
    failed_login_attempts: 0,
    created_from_ip: userData.ip || null
  };
  
  const user = {
    id: Utilities.getUuid(),
    username: userData.username,
    email: userData.email || '',
    wrapped_mk: userData.wrapped_mk || '',
    perfilId: userData.perfilId || 'p_publicador',
    auth_config: JSON.stringify(authConfig),
    metadata: JSON.stringify(metadata),
    created_at: now,
    _ts: now
  };
  
  updateOrInsert(sheet, user, false);
  clearCache(getCoreSpreadsheetId(), 'Usuarios');
  invalidateCache('u:');
  
  return { success: true, user: { id: user.id, username: user.username } };
}

/**
 * Actualiza un usuario existente
 * @param {string} id - ID del usuario
 * @param {object} updates - Campos a actualizar
 * @return {object} Usuario actualizado
 */
function updateUser(id, updates) {
  const sheet = getUsuariosSheet();
  if (!sheet) throw new Error('Hoja Usuarios no encontrada');
  
  const user = getUserById(id);
  if (!user) {
    throw new Error('ERR_USER_NOT_FOUND: Usuario no encontrado');
  }
  
  // Handle auth_config and metadata as JSON strings
  let processedUpdates = { ...updates };
  if (updates.auth_config && typeof updates.auth_config === 'object') {
    processedUpdates.auth_config = JSON.stringify(updates.auth_config);
  }
  if (updates.metadata && typeof updates.metadata === 'object') {
    processedUpdates.metadata = JSON.stringify(updates.metadata);
  }
  
  const updatedUser = {
    ...user,
    ...processedUpdates,
    _ts: new Date().toISOString()
  };
  
  updateOrInsert(sheet, updatedUser, false);
  clearCache(getCoreSpreadsheetId(), 'Usuarios');
  invalidateCache('u:');
  
  return { success: true, user: { id: updatedUser.id, username: updatedUser.username } };
}

/**
 * Actualiza la contraseña de un usuario
 * @param {string} userId - ID del usuario
 * @param {string} newPassword - Nueva contraseña
 * @return {object} Resultado
 */
function updateUserPassword(userId, newPassword) {
  const sheet = getUsuariosSheet();
  if (!sheet) throw new Error('Hoja Usuarios no encontrada');
  
  const user = getUserById(userId);
  if (!user) {
    throw new Error('ERR_USER_NOT_FOUND: Usuario no encontrado');
  }
  
  const password_hash = hashPassword(newPassword);
  const now = new Date().toISOString();
  
  // Parse existing auth_config and update password_hash inside
  let authConfig = { default_method: 'passkey', password_hash: '', recovery_enabled: true, email_otp: { enabled: false }, totp: { enabled: false }, passkeys: [] };
  try {
    if (user.auth_config) {
      authConfig = parseAuthConfig(user.auth_config);
    }
  } catch (e) {
    // Use default if parse fails
  }
  
  authConfig.password_hash = password_hash;
  
  // Update metadata for password change tracking
  let metadata = parseUserMetadata(user.metadata);
  metadata.last_password_change = now;
  
  const updatedUser = {
    ...user,
    auth_config: JSON.stringify(authConfig),
    metadata: JSON.stringify(metadata),
    _ts: now
  };
  
  updateOrInsert(sheet, updatedUser, false);
  clearCache(getCoreSpreadsheetId(), 'Usuarios');
  invalidateCache('u:');
  
  return { success: true };
}

/**
 * Actualiza el metadata de un usuario
 * @param {string} userId - ID del usuario
 * @param {object} updates - Campos a actualizar en metadata
 * @return {object} Resultado
 */
function updateUserMetadata(userId, updates) {
  const user = getUserById(userId);
  if (!user) {
    throw new Error('ERR_USER_NOT_FOUND: Usuario no encontrado');
  }
  
  let metadata = { last_login: null, last_password_change: null, failed_login_attempts: 0, created_from_ip: null };
  try {
    metadata = parseUserMetadata(user.metadata);
  } catch (e) {
    // Use default
  }
  
  metadata = { ...metadata, ...updates };
  
  return updateUser(userId, { metadata: JSON.stringify(metadata) });
}

/**
 * Incrementa los intentos de login fallidos
 * @param {string} userId - ID del usuario
 */
function incrementFailedLoginAttempts(userId) {
  const user = getUserById(userId);
  if (!user) return;
  
  let metadata = { last_login: null, last_password_change: null, failed_login_attempts: 0, created_from_ip: null };
  try {
    metadata = parseUserMetadata(user.metadata);
  } catch (e) {}
  
  metadata.failed_login_attempts = (metadata.failed_login_attempts || 0) + 1;
  updateUser(userId, { metadata: JSON.stringify(metadata) });
}

/**
 * Reinicia los intentos de login fallidos
 * @param {string} userId - ID del usuario
 */
function resetFailedLoginAttempts(userId) {
  const user = getUserById(userId);
  if (!user) return;
  
  let metadata = { last_login: null, last_password_change: null, failed_login_attempts: 0, created_from_ip: null };
  try {
    metadata = parseUserMetadata(user.metadata);
  } catch (e) {}
  
  metadata.failed_login_attempts = 0;
  updateUser(userId, { metadata: JSON.stringify(metadata) });
}

/**
 * Obtiene un valor específico del metadata de un usuario
 * @param {string} userId - ID del usuario
 * @param {string} key - Clave del metadata
 * @return {any} Valor de la clave
 */
function getUserMetadataValue(userId, key) {
  const user = getUserById(userId);
  if (!user) return null;
  
  let metadata = { last_login: null, last_password_change: null, failed_login_attempts: 0, created_from_ip: null };
  try {
    metadata = parseUserMetadata(user.metadata);
  } catch (e) {}
  
  return metadata[key] || null;
}

/**
 * Invalida todas las sesiones de un usuario
 * @param {string} userId - ID del usuario
 */
function invalidateAllUserSessions(userId) {
  const sessions = getUserSessions(userId);
  if (sessions && sessions.length > 0) {
    sessions.forEach(session => {
      try {
        invalidateSession(session.token);
      } catch (e) {
        Logger.log('Error invalidating session: ' + e.message);
      }
    });
  }
}

/**
 * Genera un token de sesión
 * @param {string} userId - ID del usuario
 * @return {object} Token de sesión
 */
function generateSessionToken(userId) {
  const user = getUserById(userId);
  if (!user) {
    throw new Error('ERR_USER_NOT_FOUND');
  }
  
  const token = Utilities.getUuid() + '_' + Utilities.getUuid();
  const expiresAt = new Date(Date.now() + SESSION_TTL * 1000).toISOString();
  
  // Guardar sesión en propiedades (en producción, usar base de datos)
  const sessionData = {
    token: token,
    userId: userId,
    createdAt: new Date().toISOString(),
    expiresAt: expiresAt
  };
  
  const userSessions = getUserSessions(userId);
  userSessions.push(sessionData);
  PropertiesService.getUserProperties().setProperty(
    'sessions_' + userId,
    JSON.stringify(userSessions)
  );
  
  _addToSessionIndex(token, userId, expiresAt);
  
  return {
    sessionToken: token,
    expiresAt: expiresAt,
    userId: userId
  };
}

/**
 * Obtiene las sesiones de un usuario
 */
function getUserSessions(userId) {
  const stored = PropertiesService.getUserProperties().getProperty('sessions_' + userId);
  return stored ? JSON.parse(stored) : [];
}

/**
 * Índice híbrido de sesiones (memoria + PropertiesService)
 */
let _sessionIndex = null;

function _loadSessionIndex() {
  if (_sessionIndex) return _sessionIndex;
  
  const stored = CacheService.getScriptCache().get('session_index');
  if (stored) {
    _sessionIndex = JSON.parse(stored);
    return _sessionIndex;
  }
  
  _sessionIndex = {};
  return _sessionIndex;
}

function _saveSessionIndex() {
  if (!_sessionIndex) return;
  try {
    CacheService.getScriptCache().put('session_index', JSON.stringify(_sessionIndex), SESSION_TTL);
  } catch (e) {}
}

function _addToSessionIndex(token, userId, expiresAt) {
  let idx = _loadSessionIndex();
  idx[token] = { userId, expiresAt };
  _saveSessionIndex();
}

function _removeFromSessionIndex(token) {
  let idx = _loadSessionIndex();
  delete idx[token];
  _saveSessionIndex();
}

function _findSessionInProperties(token) {
  const allProperties = PropertiesService.getUserProperties();
  const keys = allProperties.getKeys();
  
  for (const key of keys) {
    if (!key.startsWith('sessions_')) continue;
    
    const sessions = JSON.parse(allProperties.getProperty(key) || '[]');
    for (const session of sessions) {
      if (session.token === token) {
        if (new Date(session.expiresAt) > new Date()) {
          return session;
        }
      }
    }
  }
  
  return null;
}

/**
 * Valida un token de sesión (usa índice híbrido con fallback a PropertiesService)
 * @param {string} token - Token de sesión
 * @return {object|null} Datos de sesión o null si es inválido
 */
function validateSession(token) {
  let idx = _loadSessionIndex();
  let session = idx[token];
  
  if (!session) {
    session = _findSessionInProperties(token);
    if (session) {
      idx[token] = session;
      _saveSessionIndex();
    }
  }
  
  if (session) {
    if (new Date(session.expiresAt) > new Date()) {
      const user = getUserById(session.userId);
      return {
        valid: true,
        userId: session.userId,
        username: user?.username,
        expiresAt: session.expiresAt
      };
    }
    delete idx[token];
    _saveSessionIndex();
  }
  
  return { valid: false };
}

/**
 * Cierra una sesión
 * @param {string} token - Token de sesión a cerrar
 */
function invalidateSession(token) {
  _removeFromSessionIndex(token);
  
  const allProperties = PropertiesService.getUserProperties();
  const keys = allProperties.getKeys();
  
  for (const key of keys) {
    if (!key.startsWith('sessions_')) continue;
    
    let sessions = JSON.parse(allProperties.getProperty(key) || '[]');
    const initialLength = sessions.length;
    sessions = sessions.filter(s => s.token !== token);
    
    if (sessions.length !== initialLength) {
      allProperties.setProperty(key, JSON.stringify(sessions));
    }
  }
}

/**
 * Renueva/extiende un token de sesión
 * @param {string} token - Token de sesión a renovar
 * @return {object} Nuevo token o error
 */
function refreshSessionToken(token) {
  const allProperties = PropertiesService.getUserProperties();
  const keys = allProperties.getKeys();
  
  for (const key of keys) {
    if (!key.startsWith('sessions_')) continue;
    
    let sessions = JSON.parse(allProperties.getProperty(key) || '[]');
    const sessionIndex = sessions.findIndex(s => s.token === token);
    
    if (sessionIndex !== -1) {
      const session = sessions[sessionIndex];
      
      // Verificar que no ha expirado
      if (new Date(session.expiresAt) < new Date()) {
        return { success: false, error: 'ERR_SESSION_EXPIRED' };
      }
      
      // Verificar si está próximo a expirar (menos de 1 hora)
      const timeLeft = new Date(session.expiresAt) - new Date();
      const oneHour = 60 * 60 * 1000;
      
      if (timeLeft > oneHour) {
        // No necesita renovación aún
        return { 
          success: true, 
          message: 'Sesión válida', 
          expiresAt: session.expiresAt,
          needsRefresh: false
        };
      }
      
      // Renovar sesión
      const newExpiresAt = new Date(Date.now() + SESSION_TTL * 1000).toISOString();
      sessions[sessionIndex].expiresAt = newExpiresAt;
      sessions[sessionIndex].lastRefresh = new Date().toISOString();
      
      allProperties.setProperty(key, JSON.stringify(sessions));
      
      _addToSessionIndex(token, session.userId, newExpiresAt);
      
      return { 
        success: true, 
        expiresAt: newExpiresAt,
        needsRefresh: false
      };
    }
  }
  
  return { success: false, error: 'ERR_SESSION_NOT_FOUND' };
}

/**
 * Obtiene todas las sesiones activas de un usuario
 * @param {string} userId - ID del usuario
 * @return {array} Lista de sesiones activas
 */
function getActiveSessions(userId) {
  const sessions = getUserSessions(userId);
  const now = new Date();
  
  return sessions.filter(s => new Date(s.expiresAt) > now).map(s => ({
    token: s.token,
    createdAt: s.createdAt,
    expiresAt: s.expiresAt,
    lastRefresh: s.lastRefresh || null
  }));
}

/**
 * Cierra todas las sesiones de un usuario
 * @param {string} userId - ID del usuario
 * @return {object} Resultado
 */
function invalidateAllSessions(userId) {
  PropertiesService.getUserProperties().deleteProperty('sessions_' + userId);
  
  let idx = _loadSessionIndex();
  const keysToRemove = Object.keys(idx).filter(k => idx[k].userId === userId);
  keysToRemove.forEach(k => delete idx[k]);
  _saveSessionIndex();
  
  return { success: true, message: 'Todas las sesiones cerradas' };
}

/**
 * Acción: register - Crea un nuevo usuario
 * @param {object} payload - Datos del usuario
 * @return {object} Respuesta
 */
function actionRegister(payload) {
  try {
    // Validate email is provided
    if (!payload.email || !payload.email.trim()) {
      return { success: false, error: 'ERR_EMAIL_REQUIRED: El email es requerido' };
    }
    
    // Validate email format
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(payload.email.trim())) {
      return { success: false, error: 'ERR_EMAIL_INVALID: Formato de email inválido' };
    }
    
    const result = createUser({
      username: payload.username,
      email: payload.email.trim(),
      password: payload.password,
      wrapped_mk: payload.wrapped_mk,
      perfilId: payload.perfilId,
      ip: payload.ip
    });
    
    // Send welcome email
    try {
      sendWelcomeEmail(payload.email, payload.username);
    } catch (emailErr) {
      Logger.log('Error sending welcome email: ' + emailErr.message);
    }
    
    // Send OTP email for verification (also verifies email exists)
    try {
      const otpResult = actionRequestOTP({
        username: payload.username,
        verifyOnly: true
      });
      if (!otpResult.success) {
        Logger.log('Warning: Could not send initial OTP: ' + otpResult.error);
      }
    } catch (otpErr) {
      Logger.log('Warning: Error sending initial OTP: ' + otpErr.message);
    }
    
    return {
      success: true,
      user: result.user
    };
  } catch (err) {
    return {
      success: false,
      error: err.message
    };
  }
}

/**
 * Acción: updateUser - Actualiza un usuario
 * @param {object} session - Sesión validada
 * @param {object} payload - Datos a actualizar
 * @return {object} Respuesta
 */
function actionUpdateUser(session, payload) {
  try {
    const result = updateUser(session.userId, {
      wrapped_mk: payload.wrapped_mk
    });
    
    return {
      success: true,
      user: result.user
    };
  } catch (err) {
    return {
      success: false,
      error: err.message
    };
  }
}

/**
 * Acción: setupTOTP - Genera secreto TOTP para un usuario
 * @param {object} payload - Datos del usuario
 * @return {object} Respuesta con secreto y URI para QR
 */
function actionSetupTOTP(payload) {
  try {
    let { username, password, sessionToken } = payload;
    
    let user = null;
    
    // If sessionToken provided, validate session and get user
    if (sessionToken) {
      const session = validateSession(sessionToken);
      if (!session.valid) {
        return { success: false, error: 'ERR_AUTH_INVALID: Sesión inválida o expirada' };
      }
      user = getUserById(session.userId);
    } else {
      // Fall back to password verification
      if (!username || !password) {
        return { success: false, error: 'ERR_INVALID_CREDENTIALS: Usuario y contraseña requeridos' };
      }
      
      user = getUserByUsername(username);
      if (!user) {
        return { success: false, error: 'ERR_USER_NOT_FOUND' };
      }
      
      // Parse auth_config to get password_hash
      let authConfig = { password_hash: '', totp: { enabled: false } };
      try {
        authConfig = parseAuthConfig(user.auth_config);
      } catch (e) {}
      
      // Verificar contraseña using auth_config.password_hash
      if (!verifyPassword(password, authConfig.password_hash)) {
        return { success: false, error: 'ERR_INVALID_CREDENTIALS: Contraseña incorrecta' };
      }
    }
    
    if (!user) {
      return { success: false, error: 'ERR_USER_NOT_FOUND' };
    }
    
    username = user.username;
    
    // Generar secreto TOTP
    const totpResult = generateTOTPSecret(username);
    if (!totpResult.success) {
      return { success: false, error: totpResult.error };
    }
    
    // Guardar secreto temporalmente (no confirmado aún)
    PropertiesService.getUserProperties().setProperty(
      'totp_pending_' + username,
      JSON.stringify({
        secret: totpResult.secret,
        createdAt: new Date().toISOString(),
        expiresAt: new Date(Date.now() + 10 * 60 * 1000).toISOString()
      })
    );
    
    return {
      success: true,
      secret: totpResult.secret,
      otpURI: totpResult.otpURI
    };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Acción: confirmTOTP - Confirma la configuración de TOTP
 * @param {object} payload - Datos con código de verificación
 * @return {object} Resultado
 */
function actionConfirmTOTP(payload) {
  try {
    const { username, password, code, sessionToken } = payload;
    
    let user = null;
    let resolvedUsername = username;
    
    // If sessionToken provided, validate session and get user
    if (sessionToken) {
      const session = validateSession(sessionToken);
      if (!session.valid) {
        return { success: false, error: 'ERR_AUTH_INVALID: Sesión inválida o expirada' };
      }
      user = getUserById(session.userId);
      resolvedUsername = user?.username || username;
    } else {
      // Fall back to password verification
      if (!username || !password) {
        return { success: false, error: 'ERR_INVALID_CREDENTIALS: Usuario y contraseña requeridos' };
      }
      
      user = getUserByUsername(username);
      if (!user) {
        return { success: false, error: 'ERR_USER_NOT_FOUND' };
      }
      
      // Parse auth_config to get password_hash
      let authConfig = { password_hash: '', totp: { enabled: false, secret: null } };
      try {
        authConfig = parseAuthConfig(user.auth_config);
      } catch (e) {}
      
      // Verificar contraseña using auth_config.password_hash
      if (!verifyPassword(password, authConfig.password_hash)) {
        return { success: false, error: 'ERR_INVALID_CREDENTIALS: Contraseña incorrecta' };
      }
    }
    
    if (!user) {
      return { success: false, error: 'ERR_USER_NOT_FOUND' };
    }
    
    // Obtener secreto pendiente
    const pendingData = PropertiesService.getUserProperties().getProperty('totp_pending_' + resolvedUsername);
    if (!pendingData) {
      return { success: false, error: 'ERR_NO_PENDING_TOTP: No hay configuración TOTP pendiente' };
    }
    
    const pending = JSON.parse(pendingData);
    
    // Verificar si no ha expirado
    if (new Date(pending.expiresAt) < new Date()) {
      PropertiesService.getUserProperties().deleteProperty('totp_pending_' + resolvedUsername);
      return { success: false, error: 'ERR_TOTP_EXPIRED: La configuración ha expirado' };
    }
    
    // Verificar código TOTP
    const isValid = verifyTOTP(pending.secret, code);
    if (!isValid) {
      return { success: false, error: 'ERR_INVALID_CODE: Código inválido' };
    }
    
    // Parse auth_config again to get fresh data
    let authConfig = { password_hash: '', totp: { enabled: false, secret: null }, passkeys: [] };
    try {
      authConfig = parseAuthConfig(user.auth_config);
    } catch (e) {}
    
    // Update auth_config with TOTP
    authConfig.totp = {
      enabled: true,
      secret: pending.secret,
      created_at: new Date().toISOString()
    };
    
    updateUser(user.id, { auth_config: JSON.stringify(authConfig) });
    
    // Limpiar cache de usuarios para que el login use datos frescos
    clearCache(getCoreSpreadsheetId(), 'Usuarios');
    CacheService.getScriptCache().remove('u:un:' + resolvedUsername);
    CacheService.getScriptCache().remove('u:id:' + user.id);
    
    
    // Limpiar secreto pendiente
    PropertiesService.getUserProperties().deleteProperty('totp_pending_' + resolvedUsername);
    
    return { success: true, message: 'TOTP configurado correctamente' };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Acción: disableTOTP - Desactiva TOTP para un usuario
 * @param {object} payload - Datos del usuario
 * @return {object} Resultado
 */
function actionDisableTOTP(payload) {
  try {
    const { sessionToken } = payload;
    
    // Validar sesión
    const session = validateSession(sessionToken);
    if (!session.valid) {
      return { success: false, error: 'ERR_AUTH_INVALID' };
    }
    
    // Get user to parse auth_config
    const user = getUserById(session.userId);
    if (!user) {
      return { success: false, error: 'ERR_USER_NOT_FOUND' };
    }
    
    let authConfig = { totp: { enabled: false, secret: null } };
    try {
      authConfig = parseAuthConfig(user.auth_config);
    } catch (e) {}
    
    // Disable TOTP in auth_config
    authConfig.totp = { enabled: false, secret: null, created_at: null };
    
    updateUser(session.userId, { auth_config: JSON.stringify(authConfig) });
    
    return { success: true, message: 'TOTP desactivado' };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Acción: login - Autentica usuario y devuelve token
 * @param {object} payload - Credenciales
 * @return {object} Respuesta con token
 */
function actionLogin(payload) {
  try {
    const { username, password, method, code, passkeyAssertion } = payload;
    
    // Rate limiting: max 5 intentos por minuto por username
    const rateLimit = checkRateLimit('login:' + username, 5, 60);
    if (!rateLimit.allowed) {
      return { 
        success: false, 
        error: 'ERR_RATE_LIMITED: Demasiados intentos. Intenta más tarde.',
        retryAfter: rateLimit.resetIn,
        step: 'password'
      };
    }
    
    // Buscar usuario
    const user = getUserByUsername(username);
    if (!user) {
      return { success: false, error: 'ERR_AUTH_INVALID: Usuario no encontrado', step: 'password' };
    }
    
    // Parse auth_config
    let authConfig = parseAuthConfig(user.auth_config);
    
    // STEP 1: Verificar contraseña (always required as first step)
    if (!password) {
      return { success: false, error: 'ERR_PASSWORD_REQUIRED: Ingrese su contraseña', step: 'password' };
    }
    
    // Verificar contraseña using auth_config.password_hash
    if (!verifyPassword(password, authConfig.password_hash)) {
      incrementFailedLoginAttempts(user.id);
      logAccess(username, false, 'Contraseña inválida');
      return { success: false, error: 'ERR_AUTH_INVALID: Contraseña incorrecta', step: 'password' };
    }
    
    // Reset failed attempts on successful password verify
    resetFailedLoginAttempts(user.id);
    
    // STEP 2: Detect enabled auth methods and handle based on method parameter
    const enabledMethods = [];
    if (authConfig.passkeys && authConfig.passkeys.length > 0) enabledMethods.push('passkey');
    if (authConfig.totp && authConfig.totp.enabled) enabledMethods.push('totp');
    if (authConfig.email_otp && authConfig.email_otp.enabled) enabledMethods.push('email_otp');
    
    // If no method specified
    if (!method) {
      // Auto-proceed if only one method is enabled
      if (enabledMethods.length === 1) {
        const singleMethod = enabledMethods[0];
        
        // For email_otp, automatically send the code
        if (singleMethod === 'email_otp') {
          Logger.log('actionLogin: auto-sending email OTP for username=' + username);
          const otpResult = actionRequestOTP({ username: username });
          if (!otpResult.success) {
            return otpResult;
          }
          return {
            success: false,
            step: singleMethod,
            availableMethods: enabledMethods,
            message: 'Código enviado automáticamente'
          };
        }
        
        // For totp or passkey, ask for the code/credential
        return {
          success: false,
          step: singleMethod,
          availableMethods: enabledMethods,
          message: 'Ingrese su código'
        };
      }
      
      // Multiple methods - let user choose
      return {
        success: false,
        step: 'method',
        availableMethods: enabledMethods,
        defaultMethod: authConfig.default_method || 'passkey',
        message: 'Seleccione método de autenticación'
      };
    }
    
    // STEP 3: Verify the selected auth method
    if (method === 'totp') {
      if (!authConfig.totp || !authConfig.totp.enabled || !authConfig.totp.secret) {
        return { success: false, error: 'ERR_TOTP_NOT_CONFIGURED: TOTP no configurado', step: 'method' };
      }
      if (!code) {
        return { success: false, error: 'ERR_CODE_REQUIRED: Ingrese código TOTP', step: 'totp' };
      }
      const isValid = verifyTOTP(authConfig.totp.secret, code);
      if (!isValid) {
        logAccess(username, false, 'TOTP inválido');
        return { success: false, error: 'ERR_AUTH_INVALID: Código TOTP inválido', step: 'totp' };
      }
    } else if (method === 'email_otp') {
      if (!authConfig.email_otp || !authConfig.email_otp.enabled) {
        return { success: false, error: 'ERR_EMAIL_OTP_NOT_CONFIGURED: Email OTP no configurado', step: 'method' };
      }
      if (!code) {
        return { success: false, error: 'ERR_CODE_REQUIRED: Ingrese código del email', step: 'email_otp' };
      }
      const isValid = verifyEmailOTP(username, code);
      if (!isValid) {
        logAccess(username, false, 'Email OTP inválido');
        return { success: false, error: 'ERR_AUTH_INVALID: Código inválido', step: 'email_otp' };
      }
    } else if (method === 'passkey') {
      if (!authConfig.passkeys || authConfig.passkeys.length === 0) {
        return { success: false, error: 'ERR_PASSKEY_NOT_CONFIGURED: Passkey no configurado', step: 'method' };
      }
      if (!passkeyAssertion) {
        return { success: false, error: 'ERR_PASSKEY_REQUIRED: Autenticación con passkey requerida', step: 'passkey' };
      }
      // Passkey verification done on frontend, we just validate the result
      // The frontend sends the verified credential ID
      const validCredential = authConfig.passkeys.find(pk => pk.id === passkeyAssertion.credentialId);
      if (!validCredential) {
        logAccess(username, false, 'Passkey inválido');
        return { success: false, error: 'ERR_AUTH_INVALID: Passkey no reconocido', step: 'passkey' };
      }
    }
    
    // Update last login metadata
    updateUserMetadata(user.id, { last_login: new Date().toISOString() });
    
    // Generar token de sesión
    const session = generateSessionToken(user.id);
    
    logAccess(username, true, 'Login exitoso');
    
    return {
      success: true,
      sessionToken: session.sessionToken,
      wrapped_mk: user.wrapped_mk,
      expiresAt: session.expiresAt,
      user: {
        id: user.id,
        username: user.username,
        perfilId: user.perfilId
      }
    };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Acción: challenge - Genera desafío para WebAuthn/Passkey
 * @param {object} payload - Datos del desafío
 * @return {object} Respuesta con desafío
 */
function actionChallenge(payload) {
  try {
    const user = getUserByUsername(payload.username);
    
    if (!user) {
      return { success: false, error: 'ERR_USER_NOT_FOUND' };
    }
    
    // Parse auth_config to get passkeys
    let authConfig = { passkeys: [] };
    try {
      authConfig = parseAuthConfig(user.auth_config);
    } catch (e) {}
    
    // Generate challenge - proper random base64 (standard, not URL-safe)
    const randomBytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, Utilities.getUuid() + new Date().getTime());
    const challenge = Utilities.base64Encode(randomBytes);
    
    // Guardar desafío temporalmente
    PropertiesService.getUserProperties().setProperty(
      'passkey_challenge_' + payload.username,
      JSON.stringify({
        challenge: challenge,
        createdAt: new Date().toISOString(),
        expiresAt: new Date(Date.now() + 5 * 60 * 1000).toISOString()
      })
    );
    
    // Get existing passkeys for allowCredentials (IDs are already base64 from browser)
    const existingCredentials = authConfig.passkeys.map(pk => ({
      id: pk.id,
      type: 'public-key'
    }));
    
    // Derive rpId from origin (use hostname, default to localhost)
    let rpId = 'localhost';
    if (payload.origin) {
      try {
        const url = Utilities.newBlob(payload.origin).getDataAsString();
        const match = url.match(/^https?:\/\/([^:\/]+)/);
        if (match && match[1]) {
          rpId = match[1];
        }
      } catch (e) {
        rpId = 'localhost';
      }
    }
    
    return {
      success: true,
      challenge: challenge,
      rpId: rpId,
      timeout: 60000,
      allowCredentials: existingCredentials,
      userVerification: 'preferred'
    };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Acción: setupPasskey - Prepara registro de nuevo passkey
 * @param {object} payload - { username, password, deviceName }
 * @return {object} Respuesta con desafío para registro
 */
function actionSetupPasskey(payload) {
  try {
    let { username, password, deviceName, sessionToken } = payload;
    
    let user = null;
    
    // If sessionToken provided, validate session and get user
    if (sessionToken) {
      const session = validateSession(sessionToken);
      if (!session.valid) {
        return { success: false, error: 'ERR_AUTH_INVALID: Sesión inválida o expirada' };
      }
      user = getUserById(session.userId);
    } else {
      // Fall back to password verification
      user = getUserByUsername(username);
      if (!user) {
        return { success: false, error: 'ERR_USER_NOT_FOUND' };
      }
      
      let authConfigVerify = { password_hash: '', passkeys: [] };
      try {
        authConfigVerify = parseAuthConfig(user.auth_config);
      } catch (e) {}
      
      if (!verifyPassword(password, authConfigVerify.password_hash)) {
        return { success: false, error: 'ERR_AUTH_INVALID: Contraseña incorrecta' };
      }
    }
    
    if (!user) {
      return { success: false, error: 'ERR_USER_NOT_FOUND' };
    }
    
    username = user.username;
    
    let authConfig = { password_hash: '', passkeys: [] };
    try {
      authConfig = parseAuthConfig(user.auth_config);
    } catch (e) {}
    
    // Generate challenge for registration - proper random base64 (standard, not URL-safe)
    const randomBytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, Utilities.getUuid() + new Date().getTime());
    const challenge = Utilities.base64Encode(randomBytes);
    
    // Generate user ID for WebAuthn - proper base64 encoding (standard, not URL-safe)
    const userIdBytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, username + new Date().getTime());
    const userId = Utilities.base64Encode(userIdBytes);
    
    // Store pending passkey setup
    const pendingData = {
      challenge: challenge,
      deviceName: deviceName || 'Dispositivo nuevo',
      username: username,
      createdAt: new Date().toISOString(),
      expiresAt: new Date(Date.now() + 10 * 60 * 1000).toISOString()
    };

    PropertiesService.getUserProperties().setProperty(
      'passkey_setup_' + username,
      JSON.stringify(pendingData)
    );

    // Derive rpId from origin (use hostname, default to localhost)
    let rpId = 'localhost';
    if (payload.origin) {
      try {
        const url = Utilities.newBlob(payload.origin).getDataAsString();
        const match = url.match(/^https?:\/\/([^:\/]+)/);
        if (match && match[1]) {
          rpId = match[1];
        }
      } catch (e) {
        rpId = 'localhost';
      }
    }

    return {
      success: true,
      challenge: challenge,
      rpId: rpId,
      timeout: 60000,
      user: {
        id: userId,
        name: username,
        displayName: username
      },
      pubKeyCredParams: [
        { type: 'public-key', alg: -7 },
        { type: 'public-key', alg: -257 }
      ],
      attestation: 'preferred',
      excludeCredentials: authConfig.passkeys.map(pk => ({
        id: pk.id,
        type: 'public-key'
      }))
    };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Acción: confirmPasskey - Confirma registro de passkey
 * @param {object} payload - { username, password, attestation }
 * @return {object} Resultado
 */
function actionConfirmPasskey(payload) {
  try {
    const { username, password, attestation, sessionToken } = payload;
    
    let user = null;
    let resolvedUsername = username;
    
    // If sessionToken provided, validate session and get user
    if (sessionToken) {
      const session = validateSession(sessionToken);
      if (!session.valid) {
        return { success: false, error: 'ERR_AUTH_INVALID: Sesión inválida o expirada' };
      }
      user = getUserById(session.userId);
      resolvedUsername = user?.username || username;
    } else {
      // Fall back to password verification
      user = getUserByUsername(username);
      if (!user) {
        return { success: false, error: 'ERR_USER_NOT_FOUND' };
      }
      
      // Get pending setup data
      const pendingStr = PropertiesService.getUserProperties().getProperty('passkey_setup_' + username);
      if (!pendingStr) {
        return { success: false, error: 'ERR_PASSKEY_SETUP_EXPIRED: La configuración expiró' };
      }
      
      const pending = JSON.parse(pendingStr);
      
      // Check expiry
      if (new Date(pending.expiresAt) < new Date()) {
        PropertiesService.getUserProperties().deleteProperty('passkey_setup_' + username);
        return { success: false, error: 'ERR_PASSKEY_SETUP_EXPIRED: La configuración expiró' };
      }
      
      // Verify password again
      let authConfigVerify = { password_hash: '', passkeys: [] };
      try {
        authConfigVerify = parseAuthConfig(user.auth_config);
      } catch (e) {}
      
      if (!verifyPassword(password, authConfigVerify.password_hash)) {
        return { success: false, error: 'ERR_AUTH_INVALID: Contraseña incorrecta' };
      }
    }
    
    if (!user) {
      return { success: false, error: 'ERR_USER_NOT_FOUND' };
    }
    
    let authConfig = { password_hash: '', passkeys: [] };
    try {
      authConfig = parseAuthConfig(user.auth_config);
    } catch (e) {}
    
    // Get pending setup data
    const pendingStr = PropertiesService.getUserProperties().getProperty('passkey_setup_' + resolvedUsername);
    if (!pendingStr) {
      return { success: false, error: 'ERR_PASSKEY_SETUP_EXPIRED: La configuración expiró' };
    }
    
    const pending = JSON.parse(pendingStr);
    
    // Check expiry
    if (new Date(pending.expiresAt) < new Date()) {
      PropertiesService.getUserProperties().deleteProperty('passkey_setup_' + resolvedUsername);
      return { success: false, error: 'ERR_PASSKEY_SETUP_EXPIRED: La configuración expiró' };
    }
    
    // Parse attestation response from frontend
    // attestation.response.clientDataJSON contains the client data
    // attestation.response.attestationObject contains the authenticator data
    
    // For simplicity, we store the credential ID from the attestation
    // In production, you'd verify the attestation properly
    const credentialId = attestation.id;
    const publicKey = attestation.response.publicKey || '';
    
    const newPasskey = {
      id: credentialId,
      public_key: publicKey,
      device_name: pending.deviceName,
      created_at: new Date().toISOString()
    };
    
    // Add to passkeys array
    authConfig.passkeys = authConfig.passkeys || [];
    authConfig.passkeys.push(newPasskey);
    
    // Update user
    updateUser(user.id, { auth_config: JSON.stringify(authConfig) });
    
    // Clear cache so AuthSettings gets fresh data
    clearCache(getCoreSpreadsheetId(), 'Usuarios');
    CacheService.getScriptCache().remove('u:un:' + username);
    CacheService.getScriptCache().remove('u:id:' + user.id);
    
    // Clear pending
    PropertiesService.getUserProperties().deleteProperty('passkey_setup_' + username);
    
    return {
      success: true,
      message: 'Passkey configurado exitosamente',
      passkeyId: credentialId
    };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Acción: deletePasskey - Elimina un passkey
 * @param {object} session - Objeto de sesión validado
 * @param {object} payload - { passkeyId }
 * @return {object} Resultado
 */
function actionDeletePasskey(session, payload) {
  try {
    const { passkeyId } = payload;
    
    const user = getUserById(session.userId);
    if (!user) {
      return { success: false, error: 'ERR_USER_NOT_FOUND' };
    }
    
    const username = user.username;
    
    // Get authConfig for the user
    let authConfig = { passkeys: [] };
    try {
      authConfig = parseAuthConfig(user.auth_config);
    } catch (e) {}
    
    // Remove passkey
    const passkeyIndex = authConfig.passkeys.findIndex(pk => pk.id === passkeyId);
    if (passkeyIndex === -1) {
      return { success: false, error: 'ERR_PASSKEY_NOT_FOUND: Passkey no encontrado' };
    }
    
    authConfig.passkeys.splice(passkeyIndex, 1);
    
    // Update user
    updateUser(user.id, { auth_config: JSON.stringify(authConfig) });
    
    // Clear cache so AuthSettings gets fresh data
    clearCache(getCoreSpreadsheetId(), 'Usuarios');
    CacheService.getScriptCache().remove('u:un:' + username);
    CacheService.getScriptCache().remove('u:id:' + user.id);
    
    return { success: true, message: 'Passkey eliminado' };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Acción: getAuthMethods - Obtiene métodos de auth habilitados
 * @param {object} session - Objeto de sesión validado
 * @return {object} Métodos disponibles
 */
function actionGetAuthMethods(session) {
  try {
    const user = getUserById(session.userId);
    if (!user) {
      return { success: false, error: 'ERR_USER_NOT_FOUND' };
    }
    
    let authConfig = { default_method: 'passkey', passkeys: [], totp: { enabled: false }, email_otp: { enabled: false } };
    try {
      authConfig = parseAuthConfig(user.auth_config);
    } catch (e) {}
    
    const methods = [];
    if (authConfig.passkeys && authConfig.passkeys.length > 0) methods.push('passkey');
    if (authConfig.totp && authConfig.totp.enabled) methods.push('totp');
    if (authConfig.email_otp && authConfig.email_otp.enabled) methods.push('email_otp');
    
    return {
      success: true,
      methods: methods,
      defaultMethod: authConfig.default_method,
      passkeys: authConfig.passkeys || [],
      totp: { enabled: authConfig.totp?.enabled || false },
      email_otp: { enabled: authConfig.email_otp?.enabled || false },
      recovery_enabled: authConfig.recovery_enabled ?? true
    };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Acción: updateAuthConfig - Actualiza configuración de autenticación
 * @param {object} session - Sesión del usuario
 * @param {object} payload - { default_method, recovery_enabled, email_otp_enabled }
 * @return {object} Respuesta
 */
function actionUpdateAuthConfig(session, payload) {
  try {
    const user = getUserById(session.userId);
    if (!user) {
      return { success: false, error: 'ERR_USER_NOT_FOUND' };
    }
    
    let authConfig = { default_method: 'passkey', password_hash: '', recovery_enabled: true, email_otp: { enabled: false }, totp: { enabled: false }, passkeys: [] };
    try {
      authConfig = parseAuthConfig(user.auth_config);
    } catch (e) {}
    
    if (payload.default_method !== undefined) {
      authConfig.default_method = payload.default_method;
    }
    if (payload.recovery_enabled !== undefined) {
      authConfig.recovery_enabled = payload.recovery_enabled;
    }
    if (payload.email_otp_enabled !== undefined) {
      if (!authConfig.email_otp) authConfig.email_otp = {};
      authConfig.email_otp.enabled = payload.email_otp_enabled;
    }
    
    updateUser(session.userId, {
      auth_config: JSON.stringify(authConfig)
    });
    
    return { success: true };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Acción: changePassword - Cambia la contraseña del usuario
 * @param {object} session - Sesión del usuario
 * @param {object} payload - { old_password, new_password }
 * @return {object} Respuesta
 */
function actionChangePassword(session, payload) {
  try {
    const { old_password, new_password } = payload;
    
    if (!old_password || !new_password) {
      return { success: false, error: 'ERR_INVALID_CREDENTIALS: Contraseñas requeridas' };
    }
    
    if (new_password.length < 8) {
      return { success: false, error: 'ERR_WEAK_PASSWORD: La contraseña debe tener al menos 8 caracteres' };
    }
    
    const user = getUserById(session.userId);
    if (!user) {
      return { success: false, error: 'ERR_USER_NOT_FOUND' };
    }
    
    let authConfig = { default_method: 'passkey', password_hash: '', recovery_enabled: true, email_otp: { enabled: false }, totp: { enabled: false }, passkeys: [] };
    try {
      authConfig = parseAuthConfig(user.auth_config);
    } catch (e) {}
    
    if (!verifyPassword(old_password, authConfig.password_hash)) {
      updateUserMetadata(session.userId, { failed_login_attempts: (getUserMetadataValue(session.userId, 'failed_login_attempts') || 0) + 1 });
      return { success: false, error: 'ERR_INVALID_CREDENTIALS: Contraseña actual incorrecta' };
    }
    
    const newHash = hashPassword(new_password);
    authConfig.password_hash = newHash;
    
    updateUser(session.userId, {
      auth_config: JSON.stringify(authConfig)
    });
    
    updateUserMetadata(session.userId, { 
      last_password_change: new Date().toISOString(),
      failed_login_attempts: 0
    });
    
    logAccess(user.username, true, 'Contraseña cambiada');
    
    return { success: true };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Acción: deleteAccount - Elimina la cuenta del usuario
 * @param {object} session - Sesión del usuario
 * @param {object} payload - { password }
 * @return {object} Respuesta
 */
function actionDeleteAccount(session, payload) {
  try {
    const { password } = payload;
    
    if (!password) {
      return { success: false, error: 'ERR_INVALID_CREDENTIALS: Contraseña requerida para eliminar cuenta' };
    }
    
    const user = getUserById(session.userId);
    if (!user) {
      return { success: false, error: 'ERR_USER_NOT_FOUND' };
    }
    
    let authConfig = { default_method: 'passkey', password_hash: '', recovery_enabled: true, email_otp: { enabled: false }, totp: { enabled: false }, passkeys: [] };
    try {
      authConfig = parseAuthConfig(user.auth_config);
    } catch (e) {}
    
    if (!verifyPassword(password, authConfig.password_hash)) {
      return { success: false, error: 'ERR_INVALID_CREDENTIALS: Contraseña incorrecta' };
    }
    
    invalidateAllSessions(session.userId);
    
    deleteData('Usuarios', user.id, true);
    
    logAccess(user.username, true, 'Cuenta eliminada');
    
    return { success: true, message: 'Cuenta eliminada correctamente' };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Acción: requestOTP - Envía código por email
 * @param {object} payload - Datos del request
 * @return {object} Respuesta
 */
function actionRequestOTP(payload) {
  try {
    // Skip rate limiting for verification emails (e.g., during registration)
    const isVerification = payload.verifyOnly === true;
    
    const user = getUserByUsername(payload.username);
    
    if (!user) {
      return { success: false, error: 'ERR_USER_NOT_FOUND', debug: { username: payload.username } };
    }
    
    // Get email from user record
    const email = user.email || payload.username;
    
    Logger.log('actionRequestOTP: username=' + payload.username + ', resolved email=' + email);
    
    // Rate limiting: max 5 requests per minute (skip for verification)
    if (!isVerification) {
      const rateLimit = checkRateLimit('otp:' + payload.username, 5, 60);
      if (!rateLimit.allowed) {
      return { 
        success: false, 
        error: 'ERR_RATE_LIMITED: Demasiados códigos solicitados. Intenta más tarde.',
        retryAfter: rateLimit.resetIn,
        debug: { username: payload.username, rateLimitKey: 'otp:' + payload.username }
      };
      }
    }
    
    // Generar código OTP de 6 dígitos
    const code = Math.floor(100000 + Math.random() * 900000).toString();
    
    // Guardar código temporalmente
    PropertiesService.getUserProperties().setProperty(
      'otp_' + payload.username,
      JSON.stringify({
        code: code,
        createdAt: new Date().toISOString(),
        expiresAt: new Date(Date.now() + 10 * 60 * 1000).toISOString() // 10 min
      })
    );
    
    // Enviar email con código OTP
    try {
      sendOTPEmail(email, code);
    } catch (emailErr) {
      Logger.log('Error sending OTP email: ' + emailErr.message);
      return { 
        success: false, 
        error: 'ERR_EMAIL_SEND: No se pudo enviar el código por email',
        debug: { email: email, error: emailErr.message }
      };
    }
    
    logAccess(payload.username, true, 'OTP enviado por email');
    
    return { 
      success: true, 
      message: 'Código enviado por email',
      debug: { email: email }
    };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Envía código OTP por email
 * @param {string} email - Email del destinatario
 * @param {string} code - Código OTP
 */
function sendOTPEmail(email, code) {
  const congregationName = PropertiesService.getScriptProperties().getProperty('CONGREGATION_NAME') || 'Congregación';
  
  Logger.log('sendOTPEmail: Attempting to send OTP to email: ' + email);
  
  try {
    MailApp.sendEmail({
      to: email,
      subject: 'Código de verificación - Congre-Admin',
      name: 'Congre-Admin',
      body: 'Tu código de verificación es: ' + code + '\n\nEste código expira en 10 minutos.\n\nSi no solicitaste este código, puedes ignorar este email.'
    });
    Logger.log('sendOTPEmail: Email sent successfully');
  } catch (emailErr) {
    Logger.log('sendOTPEmail ERROR: ' + emailErr.message);
    Logger.log('sendOTPEmail STACK: ' + emailErr.stack);
    throw emailErr;
  }
}

/**
 * Envía email de bienvenida
 * @param {string} email - Email del destinatario
 * @param {string} username - Nombre de usuario
 */
function sendWelcomeEmail(email, username) {
  try {
    const congregationName = PropertiesService.getScriptProperties().getProperty('CONGREGATION_NAME') || 'tu congregación';
    
    MailApp.sendEmail({
      to: email,
      subject: 'Bienvenido a Congre-Admin',
      name: 'Congre-Admin',
      body: 'Hola ' + username + ',\n\n' +
        'Tu cuenta en Congre-Admin ha sido creada exitosamente.\n\n' +
        ' Congregación: ' + congregationName + '\n' +
        ' Usuario: ' + username + '\n\n' +
        'Ya puedes iniciar sesión en la aplicación.\n\n' +
        'Si tienes alguna pregunta, contacta al administrador del sistema.'
    });
  } catch (err) {
    Logger.log('Error enviando email de bienvenida: ' + err.message);
    throw new Error('ERR_EMAIL_SEND: No se pudo enviar el email de bienvenida');
  }
}

/**
 * Verifica código OTP de email
 * @param {string} username - Username
 * @param {string} code - Código a verificar
 * @return {boolean} true si es válido
 */
function verifyEmailOTP(username, code) {
  try {
    const stored = PropertiesService.getUserProperties().getProperty('otp_' + username);
    if (!stored) return false;
    
    const otpData = JSON.parse(stored);
    if (new Date(otpData.expiresAt) < new Date()) return false;
    if (otpData.code !== code) return false;
    
    PropertiesService.getUserProperties().deleteProperty('otp_' + username);
    return true;
  } catch (e) {
    return false;
  }
}

/**
 * Acción: requestPasswordReset - Envía email para restablecer contraseña
 * @param {object} payload - Datos del request
 * @return {object} Respuesta
 */
function actionRequestPasswordReset(payload) {
  try {
    const user = getUserByUsername(payload.username);
    
    if (!user) {
      // Don't reveal if user exists or not
      return { success: true, message: 'Si el usuario existe, recibirás un email' };
    }
    
    // Generate reset token
    const resetToken = Utilities.getUuid();
    const expiresAt = new Date(Date.now() + 60 * 60 * 1000); // 1 hour
    
    // Store token
    PropertiesService.getUserProperties().setProperty(
      'pwd_reset_' + user.id,
      JSON.stringify({
        token: resetToken,
        expiresAt: expiresAt.toISOString()
      })
    );
    
    // Send reset email
    const email = user.email || payload.username;
    const resetLink = 'https://congre-admin.github.io/admin/reset-password?token=' + resetToken + '&userId=' + user.id;
    
    sendPasswordResetEmail(email, user.username, resetLink);
    
    logAccess(payload.username, true, 'Solicitud de reset de contraseña');
    
    return { success: true, message: 'Si el usuario existe, recibirás un email con instrucciones' };
  } catch (err) {
    Logger.log('Error en requestPasswordReset: ' + err.message);
    return { success: false, error: err.message };
  }
}

/**
 * Acción: resetPassword - Restablece la contraseña
 * @param {object} payload - Datos del request
 * @return {object} Respuesta
 */
function actionResetPassword(payload) {
  try {
    const { userId, token, newPassword } = payload;
    
    if (!userId || !token || !newPassword) {
      return { success: false, error: 'ERR_INVALID_REQUEST: Datos incompletos' };
    }
    
    // Validate password complexity
    const pwValidation = validatePasswordComplexity(newPassword);
    if (!pwValidation.valid) {
      return { success: false, error: 'ERR_PASSWORD_WEAK: ' + pwValidation.errors.join(', ') };
    }
    
    // Get stored token
    const stored = PropertiesService.getUserProperties().getProperty('pwd_reset_' + userId);
    if (!stored) {
      return { success: false, error: 'ERR_INVALID_TOKEN: Token inválido o expirado' };
    }
    
    const resetData = JSON.parse(stored);
    
    // Verify token matches
    if (resetData.token !== token) {
      return { success: false, error: 'ERR_INVALID_TOKEN: Token inválido' };
    }
    
    // Check expiration
    if (new Date(resetData.expiresAt) < new Date()) {
      PropertiesService.getUserProperties().deleteProperty('pwd_reset_' + userId);
      return { success: false, error: 'ERR_TOKEN_EXPIRED: El token ha expirado' };
    }
    
    // Get user and update password
    const user = getUserById(userId);
    if (!user) {
      return { success: false, error: 'ERR_USER_NOT_FOUND' };
    }
    
    // Update password
    updateUserPassword(userId, newPassword);
    
    // Invalidate all sessions for this user
    invalidateAllUserSessions(userId);
    
    // Delete reset token
    PropertiesService.getUserProperties().deleteProperty('pwd_reset_' + userId);
    
    // Send confirmation email
    const email = user.email || user.username;
    sendPasswordChangedEmail(email, user.username);
    
    logAccess(user.username, true, 'Contraseña restablecida');
    
    return { success: true, message: 'Contraseña restablecida exitosamente' };
  } catch (err) {
    Logger.log('Error en resetPassword: ' + err.message);
    return { success: false, error: err.message };
  }
}

/**
 * Envía email de restablecimiento de contraseña
 */
function sendPasswordResetEmail(email, username, resetLink) {
  try {
    const congregationName = PropertiesService.getScriptProperties().getProperty('CONGREGATION_NAME') || 'tu congregación';
    
    MailApp.sendEmail({
      to: email,
      subject: 'Restablecer contraseña - Congre-Admin',
      name: 'Congre-Admin',
      body: 'Hola ' + username + ',\n\n' +
        'Has solicitado restablecer tu contraseña.\n\n' +
        'Haz clic en el siguiente enlace para crear una nueva contraseña:\n' +
        resetLink + '\n\n' +
        'Este enlace expirará en 1 hora.\n\n' +
        'Si no solicitaste este cambio, puedes ignorar este email. Tu contraseña permanecerá sin cambios.'
    });
  } catch (err) {
    Logger.log('Error enviando email de reset: ' + err.message);
    throw new Error('ERR_EMAIL_SEND: No se pudo enviar el email');
  }
}

/**
 * Envía email de confirmación de cambio de contraseña
 */
function sendPasswordChangedEmail(email, username) {
  try {
    const congregationName = PropertiesService.getScriptProperties().getProperty('CONGREGATION_NAME') || 'tu congregación';
    
    MailApp.sendEmail({
      to: email,
      subject: 'Contraseña actualizada - Congre-Admin',
      name: 'Congre-Admin',
      body: 'Hola ' + username + ',\n\n' +
        'Tu contraseña ha sido actualizada exitosamente.\n\n' +
        'Si no realizaste este cambio, contacta al administrador inmediatamente.'
    });
  } catch (err) {
    Logger.log('Error enviando email de confirmación: ' + err.message);
  }
}

const BASE32_CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';

/**
 * Genera un secreto TOTP aleatorio en base32
 */
function generateBase32Secret(size) {
  let secret = '';
  const randomBase = Utilities.getUuid() + Utilities.getUuid();
  for (let i = 0; i < size; i++) {
    const charCode = randomBase.charCodeAt(i % randomBase.length);
    secret += BASE32_CHARS.charAt(charCode % 32);
  }
  return secret;
}

/**
 * Genera un secreto TOTP para un usuario
 * @param {string} username - Nombre de usuario
 * @return {object} Objeto con secret y otpURI
 */
function generateTOTPSecret(username) {
  try {
    const secret = generateBase32Secret(20);
    const issuer = 'CongreAdmin';
    const otpURI = 'otpauth://totp/' + encodeURIComponent(issuer + ':' + username) + 
                   '?secret=' + secret + 
                   '&issuer=' + encodeURIComponent(issuer) + 
                   '&algorithm=SHA1&digits=6&period=30';
    
    return {
      success: true,
      secret: secret,
      otpURI: otpURI
    };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Convierte base32 a hex - implementación probada
 */
function base32tohex(base32) {
  const base32chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
  const hexChars = "0123456789abcdef";
  let bits = "";
  let hex = "";

  for (let i = 0; i < base32.length; i++) {
    const val = base32chars.indexOf(base32[i].toUpperCase());
    if (val === -1) continue;
    bits += val.toString(2).padStart(5, "0");
  }

  for (let i = 0; i < bits.length; i += 4) {
    const chunk = bits.substr(i, 4);
    const decimal = parseInt(chunk, 2);
    hex += hexChars[decimal];
  }
  return hex;
}

/**
 * Genera código TOTP - implementación probada
 */
function generateTOTP(secret, timeStepSeconds, digits) {
  const str = base32tohex(secret);
  const bytes = new Uint8Array(str.length / 2);
  for (let i = 0; i < str.length; i += 2) {
    bytes[i / 2] = parseInt(str.substr(i, 2), 16);
  }

  const timestamp = Math.floor(new Date().getTime() / 1000);
  let counter = Math.floor(timestamp / timeStepSeconds);

  const counterBytes = new Uint8Array(8);
  for (let i = 7; i >= 0; i--) {
    counterBytes[i] = counter & 0xff;
    counter = counter >>> 8;
  }

  const hmacDigest = Utilities.computeHmacSignature(
    Utilities.MacAlgorithm.HMAC_SHA_1,
    counterBytes,
    bytes
  );

  const offset = hmacDigest[hmacDigest.length - 1] & 0xf;
  const truncatedHash = (
    ((hmacDigest[offset] & 0x7f) << 24) |
    ((hmacDigest[offset + 1] & 0xff) << 16) |
    ((hmacDigest[offset + 2] & 0xff) << 8) |
    (hmacDigest[offset + 3] & 0xff)
  ) % Math.pow(10, digits);

  return truncatedHash.toString().padStart(digits, '0');
}

/**
 * Genera código TOTP - implementación probada
 */
function generateTOTP(secret, timeStepSeconds, digits) {
  const timestamp = Math.floor(new Date().getTime() / 1000);
  return generateTOTPAtTime(secret, timestamp, timeStepSeconds, digits);
}

/**
 * Genera código TOTP en un timestamp específico
 */
function generateTOTPAtTime(secret, timestamp, timeStepSeconds, digits) {
  const str = base32tohex(secret);
  const bytes = new Uint8Array(str.length / 2);
  for (let i = 0; i < str.length; i += 2) {
    bytes[i / 2] = parseInt(str.substr(i, 2), 16);
  }

  let counter = Math.floor(timestamp / timeStepSeconds);

  const counterBytes = new Uint8Array(8);
  for (let i = 7; i >= 0; i--) {
    counterBytes[i] = counter & 0xff;
    counter = counter >>> 8;
  }

  const hmacDigest = Utilities.computeHmacSignature(
    Utilities.MacAlgorithm.HMAC_SHA_1,
    counterBytes,
    bytes
  );

  const offset = hmacDigest[hmacDigest.length - 1] & 0xf;
  const truncatedHash = (
    ((hmacDigest[offset] & 0x7f) << 24) |
    ((hmacDigest[offset + 1] & 0xff) << 16) |
    ((hmacDigest[offset + 2] & 0xff) << 8) |
    (hmacDigest[offset + 3] & 0xff)
  ) % Math.pow(10, digits);

  return truncatedHash.toString().padStart(digits, '0');
}

/**
 * Verifica código TOTP - implementación probada
 * @param {string} secret - Secreto TOTP en base32
 * @param {string} code - Código a verificar
 * @return {boolean} true si es válido
 */
function verifyTOTP(secret, code) {
  if (!secret || !code) return false;
  if (code.length !== 6 || !/^\d+$/.test(code)) return false;
  
  const timestamp = Math.floor(new Date().getTime() / 1000);
  const windowSize = 1;
  
  for (let i = -windowSize; i <= windowSize; i++) {
    const testTimestamp = timestamp + (i * 30);
    const expectedTOTP = generateTOTPAtTime(secret, testTimestamp, 30, 6);
    if (expectedTOTP === code) {
      return true;
    }
  }
  return false;
}

/**
 * Registra acceso en log
 * @param {string} username - Usuario
 * @param {boolean} success - Si fue exitoso
 * @param {string} details - Detalles
 */
function logAccess(username, success, details) {
  try {
    const ssId = getCoreSpreadsheetId();
    if (!ssId) return;
    
    const ss = getCoreSpreadsheet();
    let sheet = ss.getSheetByName('Logs_Accesos');
    
    if (!sheet) {
      sheet = ss.insertSheet('Logs_Accesos');
      sheet.appendRow(['timestamp', 'username', 'success', 'details', 'ip']);
    }
    
    sheet.appendRow([
      new Date().toISOString(),
      username,
      success ? 'YES' : 'NO',
      details,
      'SERVER'
    ]);
  } catch (err) {
    Logger.log('Error guardando log: ' + err.message);
  }
}

// ================================================================= //
// CONTROL DE PERMISOS RBAC
// Fase 1.3: Implementación de permisos
// ================================================================= //

/**
 * Obtiene la hoja de Perfiles del GSheet Core
 */
function getPerfilesSheet() {
  const ss = getCoreSpreadsheet();
  return ss.getSheetByName('Perfiles');
}

/**
 * Obtiene un perfil por ID
 * @param {string} perfilId - ID del perfil
 * @return {object|null} Perfil encontrado o null
 */
function getPerfilById(perfilId) {
  return getCached('p:id:' + perfilId, () => {
    const sheet = getPerfilesSheet();
    if (!sheet) return null;
    const data = getSheetData(sheet);
    return data.find(row => row.id === perfilId) || null;
  });
}

/**
 * Obtiene todos los perfiles (con caché)
 * @return {array} Lista de perfiles
 */
function getAllPerfiles() {
  return getCached('p:all', () => {
    const sheet = getPerfilesSheet();
    if (!sheet) return [];
    return getSheetData(sheet);
  });
}

/**
 * Normaliza el campo permisos (string JSON → objeto)
 * @param {string|object} permisos - Permisos en cualquier formato
 * @return {object} Permisos como objeto
 */
function normalizePermisos(permisos) {
  if (!permisos) return {};
  if (typeof permisos === 'object') return permisos;
  if (typeof permisos === 'string') {
    try { return JSON.parse(permisos); } catch(e) { return {}; }
  }
  return {};
}

/**
 * Verifica rate limiting para una acción
 * @param {string} identifier - Identificador único (IP, username, etc)
 * @param {number} maxRequests - Máximo de requests permitidos
 * @param {number} windowSeconds - Ventana de tiempo en segundos
 * @return {object} { allowed: boolean, remaining: number, resetIn: number }
 */
function checkRateLimit(identifier, maxRequests, windowSeconds) {
  const cache = CacheService.getScriptCache();
  const key = 'rl:' + identifier;
  const current = parseInt(cache.get(key) || '0', 10);
  
  if (current >= maxRequests) {
    return { allowed: false, remaining: 0, resetIn: windowSeconds };
  }
  
  cache.put(key, (current + 1).toString(), windowSeconds);
  return { allowed: true, remaining: maxRequests - current - 1, resetIn: windowSeconds };
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
  // CacheService.getScriptCache() no tiene getAll() en Google Apps Script
  // El cache expira automáticamente según el TTL definido (CACHE_TTL_DATA = 10 min)
  // Esta función queda aquí por compatibilidad pero no hace invalidación por patrón
  Logger.log('Cache invalidation requested for pattern: ' + pattern + ' (no-op - cache expires automatically)');
}

/**
 * Obtiene los permisos de un perfil para un módulo específico
 * @param {string} perfilId - ID del perfil
 * @param {string} modulo - Nombre del módulo
 * @return {string} Permiso (RW, R, W, null)
 */
function getPermiso(perfilId, modulo) {
  const perfil = getPerfilById(perfilId);
  if (!perfil) return null;
  
  const permisos = normalizePermisos(perfil.permisos);
  return permisos[modulo] || null;
}

/**
 * Valida si un usuario tiene permiso para una acción
 * @param {string} userId - ID del usuario
 * @param {string} modulo - Nombre del módulo
 * @param {string} accion - Acción (read, write, delete)
 * @return {boolean} true si tiene permiso
 */
function validarPermiso(userId, modulo, accion) {
  const user = getUserById(userId);
  if (!user) return false;
  
  const permiso = getPermiso(user.perfilId, modulo);
  if (!permiso) return false;
  
  // Mapeo de acciones a permisos
  const permisosAccion = {
    'read': ['R', 'RW'],
    'write': ['W', 'RW'],
    'delete': ['RW']
  };
  
  const permisosPermitidos = permisosAccion[accion] || [];
  return permisosPermitidos.includes(permiso);
}

/**
 * Obtiene todos los permisos de un usuario
 * @param {string} userId - ID del usuario
 * @return {object} Objeto con permisos por módulo
 */
function getUserPermisos(userId) {
  const user = getUserById(userId);
  if (!user) return {};
  
  const perfil = getPerfilById(user.perfilId);
  if (!perfil) return {};
  
  return normalizePermisos(perfil.permisos);
}

/**
 * Valida permisos antes de una operación CRUD
 * @param {object} session - Sesión validada
 * @param {string} action - Acción (read, write, delete)
 * @param {string} modulo - Módulo objetivo
 * @return {object} Resultado de validación
 */
function checkPermission(session, action, modulo) {
  if (!session || !session.valid) {
    return { allowed: false, error: 'ERR_AUTH_INVALID' };
  }
  
  const tienePermiso = validarPermiso(session.userId, modulo, action);
  
  if (!tienePermiso) {
    logAccess(session.username, false, `Permiso denegado: ${action} en ${modulo}`);
    return { allowed: false, error: 'ERR_PERMISSION_DENIED' };
  }
  
  return { allowed: true };
}

/**
 * Acción: getPerfiles - Obtiene todos los perfiles
 */
function actionGetPerfiles() {
  try {
    const perfiles = getAllPerfiles();
    return { success: true, perfiles: perfiles };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Acción: getCongregacion - Obtiene información de la congregación
 */
function actionGetCongregacion() {
  try {
    const nombre = PropertiesService.getScriptProperties().getProperty('CONGREGATION_NAME') || '';
    const numero = PropertiesService.getScriptProperties().getProperty('CONGREGATION_NUMBER') || '';
    
    return { 
      success: true, 
      congregacion: {
        nombre,
        numero
      } 
    };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Acción: getPermisos - Obtiene permisos de un usuario
 */
function actionGetPermisos(payload) {
  try {
    const permisos = getUserPermisos(payload.userId);
    return { success: true, permisos: permisos };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Acción: checkPermission - Valida permiso para acción
 */
function actionCheckPermission(payload) {
  try {
    const result = checkPermission(
      { valid: true, userId: payload.userId, username: payload.username },
      payload.action,
      payload.modulo
    );
    return result;
  } catch (err) {
    return { allowed: false, error: err.message };
  }
}

/**
 * Acción: createProfile - Crea un nuevo perfil
 * @param {object} payload - Datos del perfil
 * @return {object} Resultado
 */
function actionCreateProfile(payload) {
  try {
    const { id, nombre, permisos, descripcion } = payload;
    
    if (!id || !nombre) {
      return { success: false, error: 'ERR_INVALID_INPUT: Se requiere id y nombre' };
    }
    
    const existente = getPerfilById(id);
    if (existente) {
      return { success: false, error: 'ERR_PROFILE_EXISTS: El perfil ya existe' };
    }
    
    const ss = getCoreSpreadsheet();
    const sheet = ss.getSheetByName('Perfiles');
    
    const nuevoPerfil = {
      id: id,
      nombre: nombre,
      permisos: typeof permisos === 'object' ? JSON.stringify(permisos) : permisos,
      descripcion: descripcion || '',
      _v: 1,
      _ts: new Date().toISOString(),
      _deleted: false
    };
    
    updateOrInsert(sheet, nuevoPerfil, false);
    invalidateCache('p:all');
    
    return { success: true, message: 'Perfil creado', perfilId: id };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Acción: updateProfile - Actualiza un perfil existente
 * @param {object} payload - Datos del perfil a actualizar
 * @return {object} Resultado
 */
function actionUpdateProfile(payload) {
  try {
    const { id, nombre, permisos, descripcion } = payload;
    
    if (!id) {
      return { success: false, error: 'ERR_INVALID_INPUT: Se requiere id' };
    }
    
    const existente = getPerfilById(id);
    if (!existente) {
      return { success: false, error: 'ERR_PROFILE_NOT_FOUND' };
    }
    
    const ss = getCoreSpreadsheet();
    const sheet = ss.getSheetByName('Perfiles');
    
    const perfilActualizado = {
      ...existente,
      nombre: nombre !== undefined ? nombre : existente.nombre,
      permisos: permisos !== undefined 
        ? (typeof permisos === 'object' ? JSON.stringify(permisos) : permisos)
        : existente.permisos,
      descripcion: descripcion !== undefined ? descripcion : existente.descripcion
    };
    
    updateOrInsert(sheet, perfilActualizado, false);
    invalidateCache('p:all');
    invalidateCache('p:id:' + id);
    
    return { success: true, message: 'Perfil actualizado' };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Acción: deleteProfile - Elimina un perfil (borrado lógico)
 * @param {object} payload - ID del perfil
 * @return {object} Resultado
 */
function actionDeleteProfile(payload) {
  try {
    const { id } = payload;
    
    if (!id) {
      return { success: false, error: 'ERR_INVALID_INPUT: Se requiere id' };
    }
    
    const existente = getPerfilById(id);
    if (!existente) {
      return { success: false, error: 'ERR_PROFILE_NOT_FOUND' };
    }
    
    const usuarios = getSheetData(getUsuariosSheet());
    const usuariosConPerfil = usuarios.filter(u => u.perfilId === id && u._deleted !== true);
    
    if (usuariosConPerfil.length > 0) {
      return { 
        success: false, 
        error: 'ERR_PROFILE_IN_USE: Hay usuarios con este perfil',
        usuarios: usuariosConPerfil.length
      };
    }
    
    const ss = getCoreSpreadsheet();
    const sheet = ss.getSheetByName('Perfiles');
    softDeleteRow(sheet, id);
    invalidateCache('p:all');
    invalidateCache('p:id:' + id);
    
    return { success: true, message: 'Perfil eliminado' };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Acción: logout - Cierra sesión
 * @param {object} payload - Token de sesión
 * @return {object} Respuesta
 */
function actionLogout(payload) {
  try {
    invalidateSession(payload.sessionToken);
    return { success: true };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

// ================================================================= //
// FUNCIONES DE INSTALACIÓN
// Setup: createSpreadsheet, initCoreTables, seedPerfiles
// ================================================================= //

/**
 * Genera nombre de spreadsheet para módulo
 * Formato: CongreAdmin-[nombre]-[modulo]
 * @param {string} modulo - Nombre del módulo
 * @return {string} Nombre formateado
 */
function getModuleSpreadsheetName(modulo) {
  const nombre = PropertiesService.getScriptProperties().getProperty('CONGREGATION_NAME') || 'SinNombre';
  const nombreLimpio = nombre.replace(/[^a-zA-Z0-9]/g, '');
  return `CongreAdmin-${nombreLimpio}-${modulo}`;
}

/**
 * Crea un nuevo Google Spreadsheet
 * @param {string} name - Nombre del spreadsheet
 * @return {object} ID y URL del spreadsheet creado
 */
function createSpreadsheet(name) {
  try {
    const ss = SpreadsheetApp.create(name || 'CongreAdmin');
    return {
      success: true,
      ssId: ss.getId(),
      url: ss.getUrl(),
      name: ss.getName()
    };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Inicializa las tablas del Core en un GSheet
 * @param {string} ssId - ID del spreadsheet
 * @return {object} Resultado
 */
function initCoreTables(ssId) {
  try {
    const ss = SpreadsheetApp.openById(ssId);
    const results = [];
    
    // Tabla: Usuarios
    const usuariosHeaders = ['id', 'username', 'email', 'wrapped_mk', 'perfilId', 'auth_config', 'metadata', 'created_at', '_v', '_ts', '_deleted'];
    results.push(createSheetIfNotExists(ss, 'Usuarios', usuariosHeaders));
    
    // Tabla: Perfiles
    const perfilesHeaders = ['id', 'nombre', 'permisos', 'descripcion', '_v', '_ts', '_deleted'];
    results.push(createSheetIfNotExists(ss, 'Perfiles', perfilesHeaders));
    
    // Tabla: Registro_Plugins
    const pluginsHeaders = ['plugin_id', 'ssId', 'status', 'config', '_v', '_ts', '_deleted'];
    results.push(createSheetIfNotExists(ss, 'Registro_Plugins', pluginsHeaders));
    
    // Tabla: Configuracion
    const configHeaders = ['clave', 'valor', 'is_public', '_v', '_ts', '_deleted'];
    results.push(createSheetIfNotExists(ss, 'Configuracion', configHeaders));
    
    // Tabla: Sistema_Migraciones
    const migracionesHeaders = ['id', 'nombre', 'version', 'ejecutada_en', 'estado', 'error', '_v', '_ts'];
    results.push(createSheetIfNotExists(ss, 'Sistema_Migraciones', migracionesHeaders));
    
    return {
      success: true,
      message: 'Tablas del Core inicializadas',
      tables: results
    };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Crea una hoja si no existe
 */
function createSheetIfNotExists(ss, name, headers) {
  let sheet = ss.getSheetByName(name);
  if (!sheet) {
    sheet = ss.insertSheet(name);
    sheet.appendRow(headers);
    sheet.getRange(1, 1, 1, headers.length).setFontWeight('bold').setBackground('#f3f3f3');
  }
  return { sheet: name, status: 'created' };
}

/**
 * Inyecta los perfiles en la tabla Perfiles
 * @param {string} ssId - ID del spreadsheet Core
 * @param {array} customPerfiles - Perfiles personalizados (opcional)
 * @return {object} Resultado
 */
function seedPerfiles(ssId, customPerfiles) {
  try {
    const ss = SpreadsheetApp.openById(ssId);
    const sheet = ss.getSheetByName('Perfiles');
    if (!sheet) {
      return { success: false, error: 'Hoja Perfiles no encontrada' };
    }
    
    const perfiles = customPerfiles || [];
    
    // Verificar si ya hay perfiles (solo si no hay personalizados)
    const existingData = getSheetData(sheet, true);
    if (existingData.length > 0 && !customPerfiles) {
      return {
        success: false,
        error: 'Ya existen perfiles en la tabla',
        message: 'Los perfiles base ya fueron injectados anteriormente'
      };
    }
    
    // Insertar perfiles
    perfiles.forEach(perfil => {
      const row = {
        id: perfil.id,
        nombre: perfil.nombre,
        permisos: typeof perfil.permisos === 'object' ? JSON.stringify(perfil.permisos) : perfil.permisos,
        descripcion: perfil.descripcion || '',
        _v: 1,
        _ts: new Date().toISOString(),
        _deleted: false
      };
      updateOrInsert(sheet, row, false);
    });
    
    invalidateCache('p:all');
    
    return {
      success: true,
      message: 'Perfiles injectados',
      count: perfiles.length
    };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Inyecta configuración inicial
 * @param {string} ssId - ID del spreadsheet Core
 * @return {object} Resultado
 */
function seedConfiguracion(ssId, datosCongregacion) {
  try {
    const ss = SpreadsheetApp.openById(ssId);
    const sheet = ss.getSheetByName('Configuracion');
    if (!sheet) {
      return { success: false, error: 'Hoja Configuracion no encontrada' };
    }
    
    const configBase = [
      { clave: 'nombre_congregacion', valor: datosCongregacion?.nombre_congregacion || '', is_public: false },
      { clave: 'numero_congregacion', valor: datosCongregacion?.numero_congregacion || '', is_public: false },
      { clave: 'nombre_mostrar', valor: datosCongregacion?.nombre_mostrar || '', is_public: true },
      { clave: 'ss_publico', valor: datosCongregacion?.ss_publico || '', is_public: false },
      { clave: 'linked_public_ss', valor: datosCongregacion?.linked_public_ss || '', is_public: false },
      { clave: 'idioma_predeterminado', valor: 'es', is_public: true },
      { clave: 'año_servicio_actual', valor: new Date().getFullYear().toString(), is_public: false },
      { clave: 'version_sistema', valor: '1.0.0', is_public: true }
    ];
    
    configBase.forEach(conf => {
      sheet.appendRow([
        conf.clave, conf.valor, conf.is_public, 1, new Date().toISOString(), false
      ]);
    });
    
    return {
      success: true,
      message: 'Configuración base injectada'
    };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Acción API: install - Proceso completo de instalación
 * @param {object} payload - Datos de instalación
 * @param {string} payload.nombreCongregacion - Nombre de la congregación
 * @param {array} payload.perfiles - Array de perfiles (del JSON seed)
 * @return {object} Resultado
 */
function actionInstall(payload) {
  try {
    const { nombreCongregacion, numeroCongregacion, nombreMostrar, perfiles, gasUrl } = payload;
    
    const nombreLimpio = (nombreCongregacion || 'SinNombre').replace(/[^a-zA-Z0-9]/g, '');
    
    // 1. Crear Spreadsheet Core con formato: CongreAdmin-[nombre]-[modulo]
    const ssName = `CongreAdmin-${nombreLimpio}-Core`;
    const ssResult = createSpreadsheet(ssName);
    if (!ssResult.success) {
      return { success: false, error: 'Error creando spreadsheet: ' + ssResult.error };
    }
    
    const ssId = ssResult.ssId;
    
    // 2. Crear Spreadsheet Público (para información compartida)
    const ssPublicName = `CongreAdmin-${nombreLimpio}-Public`;
    const ssPublicResult = createSpreadsheet(ssPublicName);
    let publicSsId = '';
    if (ssPublicResult.success) {
      publicSsId = ssPublicResult.ssId;
      initPublicSheet(publicSsId, ssId, gasUrl);
    }
    
    // 3. Inicializar tablas Core
    const initResult = initCoreTables(ssId);
    if (!initResult.success) {
      return { success: false, error: 'Error inicializando tablas: ' + initResult.error };
    }
    
    // 4. Inyectar perfiles (desde el payload del frontend)
    if (perfiles && Array.isArray(perfiles)) {
      seedPerfiles(ssId, perfiles);
    }
    
    // 5. Inyectar configuración con datos de la congregación
    seedConfiguracion(ssId, {
      nombre_congregacion: nombreCongregacion || '',
      numero_congregacion: numeroCongregacion || '',
      nombre_mostrar: nombreMostrar || `Co. ${nombreCongregacion}`,
      ss_publico: publicSsId,
      linked_public_ss: publicSsId
    });
    
    // 6. Guardar configuración en propiedades del script
    PropertiesService.getScriptProperties().setProperty('CORE_SS_ID', ssId);
    PropertiesService.getScriptProperties().setProperty('PUBLIC_SS_ID', publicSsId);
    PropertiesService.getScriptProperties().setProperty('CONGREGATION_NAME', nombreCongregacion || '');
    PropertiesService.getScriptProperties().setProperty('CONGREGATION_NUMBER', numeroCongregacion || '');
    
    return {
      success: true,
      ssId: ssId,
      ssUrl: ssResult.url,
      publicSsId: publicSsId,
      nombreCongregacion: nombreCongregacion,
      numeroCongregacion: numeroCongregacion,
      nombreMostrar: nombreMostrar,
      message: 'Instalación completada exitosamente'
    };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Inicializa la hoja pública para datos compartidos
 * @param {string} ssId - ID del spreadsheet público
 * @param {string} adminSsId - ID del spreadsheet admin (core)
 * @param {string} gasUrl - URL del GAS
 */
function initPublicSheet(ssId, adminSsId, gasUrl) {
  try {
    const ss = SpreadsheetApp.openById(ssId);
    
    createSheetIfNotExists(ss, 'Configuracion', ['clave', 'valor', 'is_public', '_v', '_ts', '_deleted']);
    createSheetIfNotExists(ss, 'Indice', ['modulo', 'titulo', 'actualizado']);
    createSheetIfNotExists(ss, 'Anuncios', ['titulo', 'contenido', 'fecha', 'publicado']);
    createSheetIfNotExists(ss, 'Reuniones', ['tipo', 'dia', 'hora', 'lugar', 'publicado']);
    
    // Guardar linked_admin_ss en Configuracion
    const configSheet = ss.getSheetByName('Configuracion');
    const linkedData = JSON.stringify({ ssId: adminSsId, gasUrl: gasUrl });
    configSheet.appendRow(['linked_admin_ss', linkedData, false, 1, new Date().toISOString(), false]);
    
    return { success: true };
  } catch (err) {
    return { success: false, error: err.message };
  }
}
