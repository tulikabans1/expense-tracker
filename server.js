const express = require('express');
const multer = require('multer');
const xlsx = require('xlsx');
const XLSXStyle = require('xlsx-js-style');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const Datastore = require('nedb-promises');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');

const app = express();
const upload = multer({ dest: 'uploads/' });

app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// --- Auth & DB setup ---
const DATA_DIR = path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);
const usersDb = Datastore.create({ filename: path.join(DATA_DIR, 'users.db'), autoload: true });
const mappingsDb = Datastore.create({ filename: path.join(DATA_DIR, 'mappings.db'), autoload: true });
const uploadsDb = Datastore.create({ filename: path.join(DATA_DIR, 'uploads.db'), autoload: true });
const familiesDb = Datastore.create({ filename: path.join(DATA_DIR, 'families.db'), autoload: true });
const transferRulesDb = Datastore.create({ filename: path.join(DATA_DIR, 'transfer-rules.db'), autoload: true });
const memberMappingsDb = Datastore.create({ filename: path.join(DATA_DIR, 'member-mappings.db'), autoload: true });
const investmentMappingsDb = Datastore.create({ filename: path.join(DATA_DIR, 'investment-mappings.db'), autoload: true });
usersDb.ensureIndex({ fieldName: 'email', unique: true }).catch(() => {});
const JWT_SECRET = process.env.JWT_SECRET || 'dev-insecure-secret-change-me';

function genId() {
  if (crypto.randomUUID && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  return 'm_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
}

function signToken(user) {
  return jwt.sign({ uid: user._id, email: user.email }, JWT_SECRET, { expiresIn: '7d' });
}

async function authMiddleware(req, res, next) {
  const auth = req.headers['authorization'] || '';
  const [, token] = auth.split(' ');
  if (!token) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const payload = jwt.verify(token, JWT_SECRET);
    req.user = { id: payload.uid, email: payload.email };
    next();
  } catch (e) {
    return res.status(401).json({ error: 'Invalid token' });
  }
}

function tryGetUserFromAuthHeader(req) {
  const auth = req.headers['authorization'] || '';
  const [, token] = auth.split(' ');
  if (!token) return null;
  try {
    const payload = jwt.verify(token, JWT_SECRET);
    return { id: payload.uid, email: payload.email };
  } catch {
    return null;
  }
}

// Load mapping file
function loadGlobalMapping() {
  const mappingPath = path.join(__dirname, 'category-mapping.json');
  if (!fs.existsSync(mappingPath)) return {};
  try {
    const raw = fs.readFileSync(mappingPath, 'utf8');
    return JSON.parse(raw);
  } catch (e) {
    console.error('Failed to load category-mapping.json:', e);
    return {};
  }
}

async function loadUserMapping(userId) {
  if (!userId) return null;
  try {
    const doc = await mappingsDb.findOne({ userId });
    if (!doc) return null;
    if (doc.mapping && typeof doc.mapping === 'object') return doc.mapping; // backward compat
    if (typeof doc.mappingRaw === 'string') {
      try { return JSON.parse(doc.mappingRaw); } catch { return null; }
    }
    return null;
  } catch (e) {
    console.error('Failed to load user mapping:', e);
    return null;
  }
}

async function getEffectiveMappingForRequest(req) {
  const user = tryGetUserFromAuthHeader(req);
  if (user) {
    const um = await loadUserMapping(user.id);
    if (um) return um;
  }
  return loadGlobalMapping();
}

// Member-scoped mappings used for Excel annotation and overlays
async function loadMemberMappingForUser(userId, memberId) {
  if (!userId || !memberId) return null;
  try {
    const doc = await memberMappingsDb.findOne({ userId, memberId });
    if (!doc) return null;
    if (doc.mapping && typeof doc.mapping === 'object') return doc.mapping;
    if (typeof doc.mappingRaw === 'string') {
      try { return JSON.parse(doc.mappingRaw); } catch { return null; }
    }
    return null;
  } catch (e) {
    console.error('Failed to load member mapping for user:', e);
    return null;
  }
}

async function loadInvestmentMappingForUser(userId, memberId) {
  if (!userId || !memberId) return null;
  try {
    const doc = await investmentMappingsDb.findOne({ userId, memberId });
    if (!doc) return null;
    if (doc.mapping && typeof doc.mapping === 'object') return doc.mapping;
    if (typeof doc.mappingRaw === 'string') {
      try { return JSON.parse(doc.mappingRaw); } catch { return null; }
    }
    return null;
  } catch (e) {
    console.error('Failed to load investment mapping for user:', e);
    return null;
  }
}

// Shallow merge of category mappings: later objects override earlier ones
// for the same main category key. This is used only for Excel annotation
// so that member-specific transfer/investment mappings can extend or
// override the base upload mapping without affecting chart behavior.
function mergeCategoryMappings(...mappings) {
  const merged = {};
  for (const m of mappings) {
    if (!m || typeof m !== 'object') continue;
    for (const key of Object.keys(m)) {
      merged[key] = m[key];
    }
  }
  return merged;
}

// Load app config for ignores and transfers
function loadConfig() {
  const cfgPath = path.join(__dirname, 'app-config.json');
  const defaults = { ignoreKeywords: [], ignoreSectionMarkers: [], transferCategories: [] };
  if (!fs.existsSync(cfgPath)) return defaults;
  try {
    const raw = fs.readFileSync(cfgPath, 'utf8');
    const obj = JSON.parse(raw);
    return {
      ignoreKeywords: Array.isArray(obj.ignoreKeywords) ? obj.ignoreKeywords : [],
      ignoreSectionMarkers: Array.isArray(obj.ignoreSectionMarkers) ? obj.ignoreSectionMarkers : [],
      transferCategories: Array.isArray(obj.transferCategories) ? obj.transferCategories : []
    };
  } catch (e) {
    console.error('Failed to load app-config.json:', e);
    return defaults;
  }
}


// Helper: check if description contains any keyword (substring match)
function matchesKeyword(description, keywords) {
  if (!Array.isArray(keywords) || keywords.length === 0) return false;
  const descStr = typeof description === 'string' ? description.toLowerCase() : '';
  return keywords.some(k => descStr.includes(String(k).toLowerCase()));
}

// Helper: section markers (like footers/headers) should typically match only
// when they appear at the *start* of a line or as the whole line. Using a
// plain substring match here can accidentally ignore real transactions such
// as "... /Sent u/YES BANK LIMITED YBS". This helper is therefore stricter
// than matchesKeyword.
function matchesSectionMarker(description, markers) {
  if (!Array.isArray(markers) || markers.length === 0) return false;
  const text = typeof description === 'string' ? description.toLowerCase().trim() : '';
  if (!text) return false;
  return markers.some(m => {
    if (m === undefined || m === null) return false;
    const marker = String(m).toLowerCase().trim();
    if (!marker) return false;
    // Match exact line ("YES BANK LIMITED") or any line that *begins* with
    // the marker (e.g., "transaction codes in your account statement ...").
    return text === marker || text.startsWith(marker);
  });
}

// Helper: check rule (amount based)
function matchesRule(amount, rule) {
  if (!rule) return false;
  if (rule.amountLessThan !== undefined && amount < rule.amountLessThan) return true;
  if (rule.amountGreaterThanOrEqual !== undefined && amount >= rule.amountGreaterThanOrEqual) return true;
  if (rule.amountEqualTo !== undefined && amount === rule.amountEqualTo) return true;
  return false;
}

// Categorize a transaction (main + sub + subsub)
function categorizeTransaction(description, amount, mapping) {
  const descStr = typeof description === 'string' ? description.toLowerCase() : '';
  let best = { score: -1, main: 'Uncategorized', sub: undefined };

  const lenScore = (kw) => Math.min(String(kw).length, 40) / 40; // prefer longer, cap

  for (const mainCat in mapping) {
    const catObj = mapping[mainCat];
    const subs = Array.isArray(catObj.subcategories) ? catObj.subcategories : [];

    // Score main keywords
    let mainScore = -1;
    const isMainExcluded = Array.isArray(catObj.excludeKeywords) && matchesKeyword(descStr, catObj.excludeKeywords);
    if (!isMainExcluded && Array.isArray(catObj.keywords)) {
      for (const kw of catObj.keywords) {
        const k = String(kw).toLowerCase();
        if (descStr.includes(k)) {
          mainScore = Math.max(mainScore, 2 + lenScore(k));
        }
      }
    }

    // Score subcategories
    let subWinner = { sub: undefined, score: -1 };
    for (const sub of subs) {
      // Compute keyword and rule matches
      let kwMatch = false;
      let kwBestScore = -1;
      const isSubExcluded = Array.isArray(sub.excludeKeywords) && matchesKeyword(descStr, sub.excludeKeywords);
      if (!isSubExcluded && Array.isArray(sub.keywords)) {
        for (const kw of sub.keywords) {
          const k = String(kw).toLowerCase();
          if (descStr.includes(k)) {
            kwMatch = true;
            kwBestScore = Math.max(kwBestScore, 3 + lenScore(k));
          }
        }
      }
      const hasRule = !!(sub && sub.rule && (sub.rule.amountLessThan !== undefined || sub.rule.amountGreaterThanOrEqual !== undefined || sub.rule.amountEqualTo !== undefined));
      const ruleMatch = hasRule && matchesRule(amount, sub.rule);
      const ruleWeight = hasRule && sub?.rule?.amountEqualTo !== undefined ? 2.8 : 2.5;

      // Determine scoring mode: default to AND when a *real* rule exists (unless overridden)
      const mode = (typeof sub.ruleMode === 'string') ? sub.ruleMode.toLowerCase() : (hasRule ? 'and' : 'or');
      let candidateScore = -1;
      if (isSubExcluded) {
        candidateScore = -1; // hard exclude when any exclude keyword matches
      } else if (mode === 'and') {
        const hasKeywords = Array.isArray(sub.keywords) && sub.keywords.length > 0;
        if (hasKeywords) {
          if (kwMatch && ruleMatch) {
            // Reward slightly higher than individual matches to favor precise classification
            candidateScore = Math.max(kwBestScore, 3.2) + 0.1; // ensure dominance over base rule weight
          }
        } else {
          // No keywords provided; treat as rule-only to avoid blocking classification
          if (ruleMatch) candidateScore = ruleWeight + 0.05;
        }
      } else {
        // Default OR behavior: take strongest of keyword or rule
        candidateScore = Math.max(kwBestScore, ruleMatch ? ruleWeight : -1);
      }
      if (candidateScore > subWinner.score) {
        subWinner = { sub: sub.name, score: candidateScore };
      }
    }

    const catScore = Math.max(mainScore, subWinner.score);
    if (catScore > best.score) {
      best = { score: catScore, main: mainCat, sub: subWinner.sub };
    }
  }

  return { main: best.main, sub: best.sub };
}


// Parse Excel and categorize (main/sub)
function parseExcel(filePath, mapping, config) {
  const workbook = xlsx.readFile(filePath);
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  const sheetRef = sheet['!ref'];
  if (!sheetRef) {
    console.warn('Sheet has no ref; returning empty result');
    return { expense: {}, income: {}, transfers: {}, transfersOut: {}, transfersIn: {}, summary: { expense: 0, income: 0, transfers: 0, transfersOut: 0, transfersIn: 0 } };
  }
  // Identify header row robustly by scanning for common column names
  const sheetRange = xlsx.utils.decode_range(sheetRef);
  const alias = {
    date: ['date', 'txn date', 'transaction date', 'value date', 'posting date'],
    desc: ['description', 'particulars', 'narration', 'details', 'transaction particulars', 'remarks', 'transaction remarks'],
    debit: ['dr', 'debit', 'withdrawals', 'withdrawal', 'debit amount'],
    credit: ['cr', 'credit', 'deposits', 'deposit', 'credit amount']
  };
  function isHeaderLikeRow(r) {
    let foundDate = false, foundDesc = false, foundDebit = false, foundCredit = false;
    for (let c = sheetRange.s.c; c <= sheetRange.e.c; c++) {
      const cell = sheet[xlsx.utils.encode_cell({ r, c })];
      if (!cell || typeof cell.v !== 'string') continue;
      const val = String(cell.v).trim().toLowerCase();
      if (!val) continue;
      if (alias.date.some(a => val.includes(a))) foundDate = true;
      if (alias.desc.some(a => val.includes(a))) foundDesc = true;
      if (alias.debit.some(a => val.includes(a))) foundDebit = true;
      if (alias.credit.some(a => val.includes(a))) foundCredit = true;
    }
    return (foundDate && foundDesc && (foundDebit || foundCredit));
  }
  let headerRow = -1;
  // Scan the first ~80 rows to find best candidate
  const scanEnd = Math.min(sheetRange.e.r, sheetRange.s.r + 80);
  for (let r = sheetRange.s.r; r <= scanEnd; r++) {
    if (isHeaderLikeRow(r)) { headerRow = r; break; }
  }
  // Fallback: use the first row if nothing matches to avoid hard failure
  if (headerRow === -1) {
    console.warn('Header not detected explicitly; defaulting to first row of sheet.');
    headerRow = sheetRange.s.r;
  }
  const data = xlsx.utils.sheet_to_json(sheet, { range: headerRow, defval: '' });
  const result = { expense: {}, income: {}, transfers: {}, transfersOut: {}, transfersIn: {}, summary: { expense: 0, income: 0, transfers: 0, transfersOut: 0, transfersIn: 0 }, entries: [] };

  // Inspect header names once so we can dynamically detect
  // debit/credit columns such as "Withdrawal Amt." / "Deposit Amt."
  const headerKeys = data.length ? Object.keys(data[0]) : [];
  const autoDebitKeys = [];
  const autoCreditKeys = [];
  const autoAmountKeys = [];
  headerKeys.forEach(key => {
    if (key === undefined || key === null) return;
    const lower = String(key).toLowerCase();
    if (!lower || lower.includes('balance')) return;
    if (lower.includes('withdraw') || lower.includes('wdl') || lower.includes('debit')) {
      autoDebitKeys.push(key);
    }
    if (lower.includes('deposit') || lower.includes('credit')) {
      autoCreditKeys.push(key);
    }
    if (lower.includes('amount') || lower.includes('amt')) {
      autoAmountKeys.push(key);
    }
  });
  const mergeHeaderKeys = (base, extra) => base.concat(extra.filter(k => !base.includes(k)));

  const getDate = (row) => {
    const candidates = ['DATE', 'Date', 'Txn Date', 'Transaction Date', 'Value Date', 'Posting Date'];
    for (const key of candidates) {
      if (row[key] !== undefined && row[key] !== null && String(row[key]).trim() !== '') return row[key];
    }
    // Fallback: try second column
    const keys = Object.keys(row);
    if (keys.length > 1) return row[keys[1]];
    return '';
  };

  // Normalize a wide range of date cell values to ISO YYYY-MM-DD string
  const normalizeDate = (val) => {
    if (val === undefined || val === null || val === '') return '';
    // If it's already a Date
    if (val instanceof Date && !isNaN(val)) {
      const y = val.getFullYear();
      const m = String(val.getMonth() + 1).padStart(2, '0');
      const d = String(val.getDate()).padStart(2, '0');
      return `${y}-${m}-${d}`;
    }
    // Numeric Excel serial (possibly as string)
    const num = Number(val);
    if (isFinite(num) && String(val).trim() !== '' && /^(?:\d+)(?:\.\d+)?$/.test(String(val).trim())) {
      try {
        // Prefer SheetJS parser when available for correctness
        if (xlsx && xlsx.SSF && typeof xlsx.SSF.parse_date_code === 'function') {
          const o = xlsx.SSF.parse_date_code(num);
          if (o && o.y && o.m && o.d) {
            const y = o.y;
            const m = String(o.m).padStart(2, '0');
            const d = String(o.d).padStart(2, '0');
            return `${y}-${m}-${d}`;
          }
        }
      } catch {}
      // Fallback conversion from Excel serial to JS date
      const ms = Math.round((num - 25569) * 86400 * 1000);
      const d = new Date(ms);
      if (!isNaN(d)) {
        const y = d.getUTCFullYear();
        const m = String(d.getUTCMonth() + 1).padStart(2, '0');
        const day = String(d.getUTCDate()).padStart(2, '0');
        return `${y}-${m}-${day}`;
      }
    }
    // String dates: prioritize DD/MM/YYYY, DD/MM/YY and DD-MM-YYYY, DD-MM-YY
    // before falling back to native parsing (which is often MM/DD in Node).
    const s = String(val).trim();
    if (!s) return '';
    // dd/mm/yyyy or dd-mm-yyyy OR dd/mm/yy or dd-mm-yy (Indian-style day-first)
    let m = s.match(/^([0-3]?\d)[\/-]([0-1]?\d)[\/-](\d{2,4})$/);
    if (m) {
      const day = parseInt(m[1], 10);
      const mon = parseInt(m[2], 10) - 1;
      let yr = parseInt(m[3], 10);
      // For 2-digit years like 01/10/24, assume 2000+YY
      if (yr < 100) yr = 2000 + yr;
      const d = new Date(yr, mon, day);
      if (!isNaN(d)) {
        const y = d.getFullYear();
        const mm = String(d.getMonth() + 1).padStart(2, '0');
        const dd = String(d.getDate()).padStart(2, '0');
        return `${y}-${mm}-${dd}`;
      }
    }
    // dd-MMM-yy or dd-MMM-yyyy (e.g., 02-Apr-24 / 02-Apr-2024)
    m = s.match(/^([0-3]?\d)[\- ]([A-Za-z]{3})[\- ](\d{2,4})$/);
    if (m) {
      const day = parseInt(m[1], 10);
      const monStr = m[2].toLowerCase();
      const monMap = { jan:0,feb:1,mar:2,apr:3,may:4,jun:5,jul:6,aug:7,sep:8,oct:9,nov:10,dec:11 };
      const mon = monMap[monStr];
      let yr = parseInt(m[3], 10);
      if (yr < 100) yr = 2000 + yr; // assume 20xx for 2-digit
      if (mon !== undefined) {
        const d = new Date(yr, mon, day);
        if (!isNaN(d)) {
          const y = d.getFullYear();
          const mm = String(d.getMonth() + 1).padStart(2, '0');
          const dd = String(d.getDate()).padStart(2, '0');
          return `${y}-${mm}-${dd}`;
        }
      }
    }
    // yyyy-mm-dd or yyyy/mm/dd
    m = s.match(/^(\d{4})[\-\/](\d{1,2})[\-\/](\d{1,2})$/);
    if (m) {
      const yr = parseInt(m[1], 10);
      const mon = parseInt(m[2], 10) - 1;
      const day = parseInt(m[3], 10);
      const d = new Date(yr, mon, day);
      if (!isNaN(d)) {
        const y = d.getFullYear();
        const mm = String(d.getMonth() + 1).padStart(2, '0');
        const dd = String(d.getDate()).padStart(2, '0');
        return `${y}-${mm}-${dd}`;
      }
    }
    // Finally, try native parse as a last resort
    {
      const d = new Date(s);
      if (!isNaN(d)) {
        const y = d.getFullYear();
        const mm = String(d.getMonth() + 1).padStart(2, '0');
        const dd = String(d.getDate()).padStart(2, '0');
        return `${y}-${mm}-${dd}`;
      }
    }
    // Leave unknown formats as original string
    return s;
  };

  const addTo = (bucket, main, sub, amt) => {
    bucket[main] = (bucket[main] || 0) + amt;
    if (sub) {
      bucket[`${main}::${sub}`] = (bucket[`${main}::${sub}`] || 0) + amt;
    }
  };

  const pickStr = (row, keys) => {
    for (const k of keys) {
      if (row[k] !== undefined && row[k] !== null && String(row[k]).trim() !== '') return row[k];
    }
    return undefined;
  };
  const normalizeAmt = (val) => {
    if (val === undefined || val === null) return NaN;
    const s = String(val).replace(/[^0-9.-]/g, '').trim();
    if (!s) return NaN;
    return Number(s);
  };
  const descKeys = ['Description','PARTICULARS','Narration','NARRATION','Details'];
  const drCrKeys = ['Dr/Cr','DR/CR','CR/DR','Tran Type','Transaction Type','Txn Type','Type'];
  const getDrCrFlag = (row) => {
    const flag = pickStr(row, drCrKeys);
    if (!flag) return null;
    const v = String(flag).trim().toLowerCase();
    if (v === 'dr' || v === 'debit' || v.includes(' dr')) return 'dr';
    if (v === 'cr' || v === 'credit' || v.includes(' cr')) return 'cr';
    return null;
  };
  // Common variants for reference/cheque columns across banks
  const refKeys = [
    'Ref No./Cheque No.', 'Ref No./Cheque No', 'Ref No./Chq No.', 'Ref No./Chq No',
    'Ref No.', 'REF NO', 'Reference', 'Reference No', 'Ref Number', 'Ref No',
    'RefNo', 'REFNO', 'CHEQUE NO', 'Cheque No.', 'Cheque No', 'Ref No/Cheque No'
  ];
  // Support common bank headers, and extend them with any
  // dynamically-detected columns that contain words like
  // "withdrawal" / "debit" or "deposit" / "credit".
  const baseDebitKeys = ['DR','Debit','Withdrawals','Withdrawal','Withdrawals(₹)','Withdrawal(₹)','Debit Amount', 'Withdrawal Amount(INR)'];
  const baseCreditKeys = ['CR','Credit','Deposits','Deposit','Deposits(₹)','Deposit(₹)','Credit Amount', 'Deposit Amount(INR)'];
  const baseAmountKeys = ['Amount','AMOUNT','Amount (INR)','Tran Amount','Transaction Amount','Txn Amount'];
  const debitKeys = mergeHeaderKeys(baseDebitKeys, autoDebitKeys);
  const creditKeys = mergeHeaderKeys(baseCreditKeys, autoCreditKeys);
  const amountKeys = mergeHeaderKeys(baseAmountKeys, autoAmountKeys);
  const isAmountLikeKey = (key) => {
    if (!key) return false;
    const k = String(key).toLowerCase();
    if (k.includes('balance')) return false;
    if (k.includes('amount') || k.includes('amt')) return true;
    if (k.includes('debit') || k.includes('withdrawal')) return true;
    if (k.includes('credit') || k.includes('deposit')) return true;
    return false;
  };

  data.forEach(row => {
    let desc = pickStr(row, descKeys);
    if (desc === undefined) {
      // Fallback: choose the first non-empty textual cell as description
      const keys = Object.keys(row);
      for (const k of keys) {
        const val = row[k];
        if (val !== undefined && val !== null) {
          const s = String(val).trim();
          if (s && /[A-Za-z]/.test(s)) { desc = s; break; }
        }
      }
      if (desc === undefined && keys.length) {
        // As last resort, use first cell value
        desc = row[keys[0]];
      }
    }
    const refText = pickStr(row, refKeys) || '';
    const combinedText = [desc, refText].filter(Boolean).join(' ');
    let dr = normalizeAmt(pickStr(row, debitKeys));
    let cr = normalizeAmt(pickStr(row, creditKeys));

    // Fallback for statements that use a single Amount + Dr/Cr column (e.g. some SBI formats)
    const drCrFlag = getDrCrFlag(row);
    if ((isNaN(dr) || dr <= 0) && (isNaN(cr) || cr <= 0)) {
      let amtAny = normalizeAmt(pickStr(row, amountKeys));
      if ((isNaN(amtAny) || amtAny <= 0)) {
        // As a last resort, scan all numeric-looking cells except obvious text
        const keys = Object.keys(row);
        for (const k of keys) {
          if (!isAmountLikeKey(k)) continue;
          const n = normalizeAmt(row[k]);
          if (!isNaN(n) && n > 0) {
            amtAny = n;
            break;
          }
        }
      }
      if (!isNaN(amtAny) && amtAny > 0) {
        const lowerText = String(combinedText || '').toLowerCase();
        const expenseWords = ['charge','charges','locker','amc','fee','gst','tax','debit card','pos txn','bill payment'];
        const incomeWords = ['interest','salary','refund','reversal','cashback','dividend','credit interest'];
        const hasExpenseWord = expenseWords.some(w => lowerText.includes(w));
        const hasIncomeWord = incomeWords.some(w => lowerText.includes(w));

        if (drCrFlag === 'dr') {
          dr = amtAny;
        } else if (drCrFlag === 'cr') {
          cr = amtAny;
        } else if (hasExpenseWord && !hasIncomeWord) {
          dr = amtAny;
        } else if (hasIncomeWord && !hasExpenseWord) {
          cr = amtAny;
        } else {
          // Default to treating unknown-direction amounts as expense so they are not silently dropped
          dr = amtAny;
        }
      }
    }

    // Ignore entire row if description matches any ignore keyword or section marker
    // Section markers are matched using a stricter "startsWith" rule so that
    // genuine transactions that merely *contain* phrases like "YES BANK LIMITED"
    // are not dropped from the analysis.
    if (matchesKeyword(combinedText, config && config.ignoreKeywords) || matchesSectionMarker(combinedText, config && config.ignoreSectionMarkers)) {
      return;
    }
    // Heuristic: skip rows with non-textual descriptions (totals/footers like "137096.65" or "########")
    const hasLetters = typeof desc === 'string' && /[A-Za-z]/.test(desc);
    if (!hasLetters) {
      return;
    }

    // Guard: if both DR and CR are positive, prefer the larger side to avoid double-counting
    if (desc && !isNaN(dr) && dr > 0 && !isNaN(cr) && cr > 0) {
      if (dr >= cr) {
        const cat = categorizeTransaction(combinedText, dr, mapping);
        if (config && Array.isArray(config.transferCategories) && config.transferCategories.includes(cat.main)) {
          addTo(result.transfers, cat.main, cat.sub, dr);
          addTo(result.transfersOut, cat.main, cat.sub, dr);
          result.summary.transfers += dr;
          result.summary.transfersOut += dr;
          result.entries.push({ date: normalizeDate(getDate(row)), description: desc, text: combinedText, amount: dr, type: 'transfer', direction: 'out', main: cat.main, sub: cat.sub });
        } else {
          addTo(result.expense, cat.main, cat.sub, dr);
          result.summary.expense += dr;
          result.entries.push({ date: normalizeDate(getDate(row)), description: desc, text: combinedText, amount: dr, type: 'expense', main: cat.main, sub: cat.sub });
        }
      } else {
        const cat = categorizeTransaction(combinedText, cr, mapping);
        if (config && Array.isArray(config.transferCategories) && config.transferCategories.includes(cat.main)) {
          addTo(result.transfers, cat.main, cat.sub, cr);
          addTo(result.transfersIn, cat.main, cat.sub, cr);
          result.summary.transfers += cr;
          result.summary.transfersIn += cr;
          result.entries.push({ date: normalizeDate(getDate(row)), description: desc, text: combinedText, amount: cr, type: 'transfer', direction: 'in', main: cat.main, sub: cat.sub });
        } else {
          addTo(result.income, cat.main, cat.sub, cr);
          result.summary.income += cr;
          result.entries.push({ date: normalizeDate(getDate(row)), description: desc, text: combinedText, amount: cr, type: 'income', main: cat.main, sub: cat.sub });
        }
      }
      return; // handled one side; skip the other to avoid double logging
    }

    // Expense (Debit)
    if (desc && !isNaN(dr) && dr > 0) {
      const cat = categorizeTransaction(combinedText, dr, mapping);
      if (config && Array.isArray(config.transferCategories) && config.transferCategories.includes(cat.main)) {
        addTo(result.transfers, cat.main, cat.sub, dr);
        addTo(result.transfersOut, cat.main, cat.sub, dr);
        result.summary.transfers += dr;
        result.summary.transfersOut += dr;
        result.entries.push({ date: normalizeDate(getDate(row)), description: desc, text: combinedText, amount: dr, type: 'transfer', direction: 'out', main: cat.main, sub: cat.sub });
      } else {
        addTo(result.expense, cat.main, cat.sub, dr);
        result.summary.expense += dr;
        result.entries.push({ date: normalizeDate(getDate(row)), description: desc, text: combinedText, amount: dr, type: 'expense', main: cat.main, sub: cat.sub });
      }
    }
    // Income (Credit)
    if (desc && !isNaN(cr) && cr > 0) {
      const cat = categorizeTransaction(combinedText, cr, mapping);
      if (config && Array.isArray(config.transferCategories) && config.transferCategories.includes(cat.main)) {
        addTo(result.transfers, cat.main, cat.sub, cr);
        addTo(result.transfersIn, cat.main, cat.sub, cr);
        result.summary.transfers += cr;
        result.summary.transfersIn += cr;
        result.entries.push({ date: normalizeDate(getDate(row)), description: desc, text: combinedText, amount: cr, type: 'transfer', direction: 'in', main: cat.main, sub: cat.sub });
      } else {
        addTo(result.income, cat.main, cat.sub, cr);
        result.summary.income += cr;
        result.entries.push({ date: normalizeDate(getDate(row)), description: desc, text: combinedText, amount: cr, type: 'income', main: cat.main, sub: cat.sub });
      }
    }
  });
  return result;
}

// Annotate an Excel file with a yellow "Comments" column containing
// "category-subcategory" for each classified transaction. This uses
// the same parsing heuristics as parseExcel but only writes back
// comments without affecting existing aggregation behavior.
function annotateExcelWithComments(filePath, mapping, config) {
  // Use XLSXStyle so that cell background colors are preserved
  const workbook = XLSXStyle.readFile(filePath);
  const sheetName = workbook.SheetNames[0];
  const sheet = workbook.Sheets[sheetName];
  const sheetRef = sheet['!ref'];
  if (!sheetRef) {
    console.warn('Sheet has no ref; skipping annotation');
    return workbook;
  }
  const sheetRange = xlsx.utils.decode_range(sheetRef);
  const alias = {
    date: ['date', 'txn date', 'transaction date', 'value date', 'posting date'],
    desc: ['description', 'particulars', 'narration', 'details', 'transaction particulars', 'remarks', 'transaction remarks'],
    debit: ['dr', 'debit', 'withdrawals', 'withdrawal', 'debit amount'],
    credit: ['cr', 'credit', 'deposits', 'deposit', 'credit amount']
  };
  function isHeaderLikeRow(r) {
    let foundDate = false, foundDesc = false, foundDebit = false, foundCredit = false;
    for (let c = sheetRange.s.c; c <= sheetRange.e.c; c++) {
      const cell = sheet[xlsx.utils.encode_cell({ r, c })];
      if (!cell || typeof cell.v !== 'string') continue;
      const val = String(cell.v).trim().toLowerCase();
      if (!val) continue;
      if (alias.date.some(a => val.includes(a))) foundDate = true;
      if (alias.desc.some(a => val.includes(a))) foundDesc = true;
      if (alias.debit.some(a => val.includes(a))) foundDebit = true;
      if (alias.credit.some(a => val.includes(a))) foundCredit = true;
    }
    return (foundDate && foundDesc && (foundDebit || foundCredit));
  }
  let headerRow = -1;
  const scanEnd = Math.min(sheetRange.e.r, sheetRange.s.r + 80);
  for (let r = sheetRange.s.r; r <= scanEnd; r++) {
    if (isHeaderLikeRow(r)) { headerRow = r; break; }
  }
  if (headerRow === -1) {
    console.warn('Header not detected explicitly; defaulting to first row of sheet (annotation).');
    headerRow = sheetRange.s.r;
  }

  const data = xlsx.utils.sheet_to_json(sheet, { range: headerRow, defval: '' });

  // Inspect header names once for this sheet so we can
  // dynamically pick up columns like "Withdrawal Amt." /
  // "Deposit Amt." as debit/credit/amount columns.
  const headerKeys = data.length ? Object.keys(data[0]) : [];
  const autoDebitKeys = [];
  const autoCreditKeys = [];
  const autoAmountKeys = [];
  headerKeys.forEach(key => {
    if (key === undefined || key === null) return;
    const lower = String(key).toLowerCase();
    if (!lower || lower.includes('balance')) return;
    if (lower.includes('withdraw') || lower.includes('wdl') || lower.includes('debit')) {
      autoDebitKeys.push(key);
    }
    if (lower.includes('deposit') || lower.includes('credit')) {
      autoCreditKeys.push(key);
    }
    if (lower.includes('amount') || lower.includes('amt')) {
      autoAmountKeys.push(key);
    }
  });
  const mergeHeaderKeys = (base, extra) => base.concat(extra.filter(k => !base.includes(k)));

  const pickStr = (row, keys) => {
    for (const k of keys) {
      if (row[k] !== undefined && row[k] !== null && String(row[k]).trim() !== '') return row[k];
    }
    return undefined;
  };
  const normalizeAmt = (val) => {
    if (val === undefined || val === null) return NaN;
    const s = String(val).replace(/[^0-9.-]/g, '').trim();
    if (!s) return NaN;
    return Number(s);
  };
  const descKeys = ['Description','PARTICULARS','Narration','NARRATION','Details'];
  const drCrKeys = ['Dr/Cr','DR/CR','CR/DR','Tran Type','Transaction Type','Txn Type','Type'];
  const getDrCrFlag = (row) => {
    const flag = pickStr(row, drCrKeys);
    if (!flag) return null;
    const v = String(flag).trim().toLowerCase();
    if (v === 'dr' || v === 'debit' || v.includes(' dr')) return 'dr';
    if (v === 'cr' || v === 'credit' || v.includes(' cr')) return 'cr';
    return null;
  };
  const refKeys = [
    'Ref No./Cheque No.', 'Ref No./Cheque No', 'Ref No./Chq No.', 'Ref No./Chq No',
    'Ref No.', 'REF NO', 'Reference', 'Reference No', 'Ref Number', 'Ref No',
    'RefNo', 'REFNO', 'CHEQUE NO', 'Cheque No.', 'Cheque No', 'Ref No/Cheque No'
  ];
  const baseDebitKeys = ['DR','Debit','Withdrawals','Withdrawal','Withdrawals(₹)','Withdrawal(₹)','Debit Amount'];
  const baseCreditKeys = ['CR','Credit','Deposits','Deposit','Deposits(₹)','Deposit(₹)','Credit Amount'];
  const baseAmountKeys = ['Amount','AMOUNT','Amount (INR)','Tran Amount','Transaction Amount','Txn Amount'];
  const debitKeys = mergeHeaderKeys(baseDebitKeys, autoDebitKeys);
  const creditKeys = mergeHeaderKeys(baseCreditKeys, autoCreditKeys);
  const amountKeys = mergeHeaderKeys(baseAmountKeys, autoAmountKeys);
  const isAmountLikeKey = (key) => {
    if (!key) return false;
    const k = String(key).toLowerCase();
    if (k.includes('balance')) return false;
    if (k.includes('amount') || k.includes('amt')) return true;
    if (k.includes('debit') || k.includes('withdrawal')) return true;
    if (k.includes('credit') || k.includes('deposit')) return true;
    return false;
  };

  const classifications = new Array(data.length).fill(null);

  data.forEach((row, idx) => {
    let desc = pickStr(row, descKeys);
    if (desc === undefined) {
      const keys = Object.keys(row);
      for (const k of keys) {
        const val = row[k];
        if (val !== undefined && val !== null) {
          const s = String(val).trim();
          if (s && /[A-Za-z]/.test(s)) { desc = s; break; }
        }
      }
      if (desc === undefined && keys.length) {
        desc = row[keys[0]];
      }
    }
    const refText = pickStr(row, refKeys) || '';
    const combinedText = [desc, refText].filter(Boolean).join(' ');
    let dr = normalizeAmt(pickStr(row, debitKeys));
    let cr = normalizeAmt(pickStr(row, creditKeys));

    const drCrFlag = getDrCrFlag(row);
    if ((isNaN(dr) || dr <= 0) && (isNaN(cr) || cr <= 0)) {
      let amtAny = normalizeAmt(pickStr(row, amountKeys));
      if ((isNaN(amtAny) || amtAny <= 0)) {
        const keys = Object.keys(row);
        for (const k of keys) {
          if (!isAmountLikeKey(k)) continue;
          const n = normalizeAmt(row[k]);
          if (!isNaN(n) && n > 0) {
            amtAny = n;
            break;
          }
        }
      }
      if (!isNaN(amtAny) && amtAny > 0) {
        const lowerText = String(combinedText || '').toLowerCase();
        const expenseWords = ['charge','charges','locker','amc','fee','gst','tax','debit card','pos txn','bill payment'];
        const incomeWords = ['interest','salary','refund','reversal','cashback','dividend','credit interest'];
        const hasExpenseWord = expenseWords.some(w => lowerText.includes(w));
        const hasIncomeWord = incomeWords.some(w => lowerText.includes(w));

        if (drCrFlag === 'dr') {
          dr = amtAny;
        } else if (drCrFlag === 'cr') {
          cr = amtAny;
        } else if (hasExpenseWord && !hasIncomeWord) {
          dr = amtAny;
        } else if (hasIncomeWord && !hasExpenseWord) {
          cr = amtAny;
        } else {
          dr = amtAny;
        }
      }
    }

    // Apply ignore rules (same logic as parseExcel) so that footer/header
    // sections are not annotated.
    if (matchesKeyword(combinedText, config && config.ignoreKeywords) || matchesSectionMarker(combinedText, config && config.ignoreSectionMarkers)) {
      return;
    }

    const hasLetters = typeof desc === 'string' && /[A-Za-z]/.test(desc);
    if (!hasLetters) {
      return;
    }

    const storeCat = (cat) => {
      if (!cat || !cat.main) return;
      classifications[idx] = { main: cat.main, sub: cat.sub || null };
    };

    if (desc && !isNaN(dr) && dr > 0 && !isNaN(cr) && cr > 0) {
      if (dr >= cr) {
        const cat = categorizeTransaction(combinedText, dr, mapping);
        storeCat(cat);
      } else {
        const cat = categorizeTransaction(combinedText, cr, mapping);
        storeCat(cat);
      }
      return;
    }

    if (desc && !isNaN(dr) && dr > 0) {
      const cat = categorizeTransaction(combinedText, dr, mapping);
      storeCat(cat);
    }
    if (desc && !isNaN(cr) && cr > 0) {
      const cat = categorizeTransaction(combinedText, cr, mapping);
      storeCat(cat);
    }

    // If we still don't have a classification (e.g., amount could not be
    // determined), fall back to description-only categorization so that
    // most rows get at least an "Uncategorized" entry instead of blank.
    if (!classifications[idx]) {
      const cat = categorizeTransaction(combinedText, 0, mapping);
      storeCat(cat);
    }
  });
  const categoryCol = sheetRange.e.c + 1;
  const subcategoryCol = sheetRange.e.c + 2;
  const yellowFill = { fill: { patternType: 'solid', fgColor: { rgb: 'FFFF00' } } };

  const catHeaderRef = xlsx.utils.encode_cell({ r: headerRow, c: categoryCol });
  const catHeader = sheet[catHeaderRef] || {};
  catHeader.v = 'Category';
  catHeader.t = 's';
  catHeader.s = Object.assign({}, catHeader.s || {}, yellowFill);
  sheet[catHeaderRef] = catHeader;

  const subHeaderRef = xlsx.utils.encode_cell({ r: headerRow, c: subcategoryCol });
  const subHeader = sheet[subHeaderRef] || {};
  subHeader.v = 'Subcategory';
  subHeader.t = 's';
  subHeader.s = Object.assign({}, subHeader.s || {}, yellowFill);
  sheet[subHeaderRef] = subHeader;

  classifications.forEach((cls, idx) => {
    if (!cls || !cls.main) return;
    const excelRow = headerRow + 1 + idx;
    const catRef = xlsx.utils.encode_cell({ r: excelRow, c: categoryCol });
    const catCell = sheet[catRef] || {};
    catCell.v = cls.main;
    catCell.t = 's';
    catCell.s = Object.assign({}, catCell.s || {}, yellowFill);
    sheet[catRef] = catCell;

    if (cls.sub) {
      const subRef = xlsx.utils.encode_cell({ r: excelRow, c: subcategoryCol });
      const subCell = sheet[subRef] || {};
      subCell.v = cls.sub;
      subCell.t = 's';
      subCell.s = Object.assign({}, subCell.s || {}, yellowFill);
      sheet[subRef] = subCell;
    }
  });

  if (subcategoryCol > sheetRange.e.c) {
    sheet['!ref'] = xlsx.utils.encode_range({
      s: { r: sheetRange.s.r, c: sheetRange.s.c },
      e: { r: sheetRange.e.r, c: subcategoryCol }
    });
  }

  return workbook;
}

app.post('/upload', upload.single('file'), (req, res) => {
  Promise.resolve().then(() => getEffectiveMappingForRequest(req)).then(mapping => {
  const config = loadConfig();
  const result = parseExcel(req.file.path, mapping, config);
  try {
    fs.unlinkSync(req.file.path);
  } catch (e) {
    console.warn('Failed to cleanup upload:', e);
  }
  res.json(result);
  }).catch(err => {
    console.error('Upload error:', err);
    try { fs.unlinkSync(req.file.path); } catch {}
    res.status(500).json({ error: 'Upload failed' });
  });
});

// New endpoint: upload a single Excel file, annotate it with a
// yellow "Comments" column containing "category-subcategory" for
// each transaction, and return the modified workbook for download.
app.post('/annotate-excel', upload.single('file'), async (req, res) => {
  try {
    const baseMapping = await getEffectiveMappingForRequest(req);
    const memberId = (req.query && req.query.memberId) ? String(req.query.memberId).trim() : '';
    let memberMap = null;
    let investmentMap = null;
    if (memberId) {
      const user = tryGetUserFromAuthHeader(req);
      if (user && user.id) {
        // Member-specific transfer mapping and investment mapping
        memberMap = await loadMemberMappingForUser(user.id, memberId);
        investmentMap = await loadInvestmentMappingForUser(user.id, memberId);
      }
    }
    const mapping = mergeCategoryMappings(baseMapping, memberMap, investmentMap);
    const config = loadConfig();
    const workbook = annotateExcelWithComments(req.file.path, mapping, config);
    const buffer = XLSXStyle.write(workbook, { bookType: 'xlsx', type: 'buffer' });
    try { fs.unlinkSync(req.file.path); } catch {}
    const safeName = req.file && req.file.originalname ? String(req.file.originalname).replace(/[^A-Za-z0-9._-]/g, '_') : 'statement.xlsx';
    res.setHeader('Content-Disposition', `attachment; filename="annotated-${safeName}"`);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    return res.send(buffer);
  } catch (e) {
    console.error('Annotate Excel failed:', e);
    try { if (req.file && req.file.path) fs.unlinkSync(req.file.path); } catch {}
    return res.status(500).json({ error: 'Failed to annotate Excel file' });
  }
});

// Aggregate results from multiple parsed files
function aggregateResults(results) {
  const agg = { expense: {}, income: {}, transfers: {}, transfersOut: {}, transfersIn: {}, summary: { expense: 0, income: 0, transfers: 0, transfersOut: 0, transfersIn: 0 }, entries: [] };
  const addBucket = (bucket, key, amt) => {
    bucket[key] = (bucket[key] || 0) + amt;
  };
  for (const r of results) {
    // Merge buckets
    for (const [k, v] of Object.entries(r.expense || {})) addBucket(agg.expense, k, v);
    for (const [k, v] of Object.entries(r.income || {})) addBucket(agg.income, k, v);
    for (const [k, v] of Object.entries(r.transfers || {})) addBucket(agg.transfers, k, v);
    for (const [k, v] of Object.entries(r.transfersOut || {})) addBucket(agg.transfersOut, k, v);
    for (const [k, v] of Object.entries(r.transfersIn || {})) addBucket(agg.transfersIn, k, v);
    // Merge summary
    if (r.summary) {
      agg.summary.expense += r.summary.expense || 0;
      agg.summary.income += r.summary.income || 0;
      agg.summary.transfers += r.summary.transfers || 0;
      agg.summary.transfersOut += r.summary.transfersOut || 0;
      agg.summary.transfersIn += r.summary.transfersIn || 0;
    }
    // Merge entries
    if (Array.isArray(r.entries)) agg.entries = agg.entries.concat(r.entries);
  }
  return agg;
}

// Multi-file upload: parse all and return aggregated report
app.post('/upload-multi', upload.array('files'), async (req, res) => {
  const files = req.files || [];
  if (!files.length) {
    return res.status(400).json({ error: 'No files uploaded.' });
  }
  const mapping = await getEffectiveMappingForRequest(req);
  const config = loadConfig();
  const results = [];
  try {
    for (const f of files) {
      const r = parseExcel(f.path, mapping, config);
      results.push(r);
    }
  } finally {
    // Cleanup temp files regardless of parsing success
    for (const f of files) {
      try { fs.unlinkSync(f.path); } catch (e) { /* ignore */ }
    }
  }
  const agg = aggregateResults(results);
  res.json(agg);
});

// Persist a single uploaded file for the authenticated user and store parsed report
app.post('/my/uploads', authMiddleware, upload.single('file'), async (req, res) => {
  try {
    const userId = req.user.id;
    const mapping = (await loadUserMapping(userId)) || loadGlobalMapping();
    const config = loadConfig();
    const filePath = req.file.path;
    let report = null;
    let parseError = null;
    try {
      report = parseExcel(filePath, mapping, config);
    } catch (e) {
      parseError = e && e.message ? e.message : String(e);
    }
    // Keep file on disk for future reference; store metadata and report summary/entries
    // Store report as raw JSON string to avoid NeDB dot-key issues
    const doc = await uploadsDb.insert({
      userId,
      filename: req.file.filename,
      originalName: req.file.originalname,
      mimeType: req.file.mimetype,
      size: req.file.size,
      createdAt: new Date(),
      report: undefined,
      reportRaw: report ? JSON.stringify(report) : null,
      error: parseError || null
    });
    return res.json({ id: doc._id, originalName: doc.originalName, createdAt: doc.createdAt, report, error: doc.error });
  } catch (e) {
    console.error('Save upload failed:', e);
    // On failure, try to delete temp file if present
    try { if (req.file && req.file.path) fs.unlinkSync(req.file.path); } catch {}
    return res.status(500).json({ error: 'Failed to save upload' });
  }
});

// List uploads saved by authenticated user (basic metadata + summary)
app.get('/my/uploads', authMiddleware, async (req, res) => {
  try {
    const userId = req.user.id;
    const docs = await uploadsDb.find({ userId }).sort({ createdAt: -1 });
    const items = docs.map(d => {
      let summary = { expense: 0, income: 0, transfers: 0, transfersOut: 0, transfersIn: 0 };
      if (d.report && d.report.summary) summary = d.report.summary;
      else if (typeof d.reportRaw === 'string') {
        try { const r = JSON.parse(d.reportRaw); if (r && r.summary) summary = r.summary; } catch {}
      }
      return { id: d._id, originalName: d.originalName, createdAt: d.createdAt, summary, memberId: d.memberId || null };
    });
    return res.json(items);
  } catch (e) {
    console.error('List uploads failed:', e);
    return res.status(500).json({ error: 'Failed to list uploads' });
  }
});

// Get full parsed report for a specific saved upload
app.get('/my/uploads/:id', authMiddleware, async (req, res) => {
  try {
    const doc = await uploadsDb.findOne({ _id: req.params.id, userId: req.user.id });
    if (!doc) return res.status(404).json({ error: 'Upload not found' });
    let report = doc.report || null;
    if (!report && typeof doc.reportRaw === 'string') {
      try { report = JSON.parse(doc.reportRaw); } catch {}
    }
    return res.json({ id: doc._id, originalName: doc.originalName, createdAt: doc.createdAt, report, memberId: doc.memberId || null });
  } catch (e) {
    console.error('Get upload failed:', e);
    return res.status(500).json({ error: 'Failed to get upload' });
  }
});

// Delete a saved upload and remove its file from disk
app.delete('/my/uploads/:id', authMiddleware, async (req, res) => {
  try {
    const doc = await uploadsDb.findOne({ _id: req.params.id, userId: req.user.id });
    if (!doc) return res.status(404).json({ error: 'Upload not found' });
    // Remove file if present
    const filePath = path.join(__dirname, 'uploads', doc.filename);
    try { if (fs.existsSync(filePath)) fs.unlinkSync(filePath); } catch (e) { console.warn('Failed to delete file:', e); }
    await uploadsDb.remove({ _id: doc._id }, {});
    return res.json({ ok: true });
  } catch (e) {
    console.error('Delete upload failed:', e);
    return res.status(500).json({ error: 'Failed to delete upload' });
  }
});

// Batch delete saved uploads for the authenticated user
app.delete('/my/uploads', authMiddleware, async (req, res) => {
  try {
    const ids = (req.body && Array.isArray(req.body.ids)) ? req.body.ids : null;
    if (!ids || !ids.length) {
      return res.status(400).json({ error: 'ids array is required' });
    }
    const docs = await uploadsDb.find({ userId: req.user.id, _id: { $in: ids } });
    for (const doc of docs) {
      const filePath = path.join(__dirname, 'uploads', doc.filename);
      try { if (fs.existsSync(filePath)) fs.unlinkSync(filePath); } catch (e) { console.warn('Failed to delete file:', e); }
      await uploadsDb.remove({ _id: doc._id }, {});
    }
    return res.json({ ok: true, deleted: docs.length });
  } catch (e) {
    console.error('Batch delete uploads failed:', e);
    return res.status(500).json({ error: 'Failed to delete uploads' });
  }
});

// Reparse a saved upload using the latest mapping/config
app.put('/my/uploads/:id/reparse', authMiddleware, async (req, res) => {
  try {
    const userId = req.user.id;
    const doc = await uploadsDb.findOne({ _id: req.params.id, userId });
    if (!doc) return res.status(404).json({ error: 'Upload not found' });
    const filePath = path.join(__dirname, 'uploads', doc.filename);
    if (!fs.existsSync(filePath)) {
      return res.status(404).json({ error: 'Source file missing on server' });
    }
    const mapping = (await loadUserMapping(userId)) || loadGlobalMapping();
    const config = loadConfig();
    let report = null;
    try {
      report = parseExcel(filePath, mapping, config);
    } catch (e) {
      return res.status(500).json({ error: 'Reparse failed: ' + (e && e.message ? e.message : String(e)) });
    }
    // Save reparsed report as raw JSON string to avoid dot-key issues
    await uploadsDb.update({ _id: doc._id }, { $set: { report: undefined, reportRaw: JSON.stringify(report) } });
    return res.json({ ok: true, summary: report && report.summary ? report.summary : { expense: 0, income: 0, transfers: 0, transfersOut: 0, transfersIn: 0 } });
  } catch (e) {
    console.error('Reparse upload failed:', e);
    return res.status(500).json({ error: 'Failed to reparse upload' });
  }
});

// Aggregate all saved uploads for the authenticated user
app.get('/my/expenses', authMiddleware, async (req, res) => {
  try {
    const userId = req.user.id;
    const rawIds = (req.query && req.query.memberIds) ? String(req.query.memberIds) : '';
    const filterIds = rawIds ? rawIds.split(',').map(s => s.trim()).filter(Boolean) : null;
    let query = { userId };
    if (filterIds && filterIds.length) {
      query = { userId, memberId: { $in: filterIds } };
    }
    const docs = await uploadsDb.find(query);
    // Aggregate per-upload reports while tagging each entry with its source memberId
    const agg = { expense: {}, income: {}, transfers: {}, transfersOut: {}, transfersIn: {}, summary: { expense: 0, income: 0, transfers: 0, transfersOut: 0, transfersIn: 0 }, entries: [] };
    const addBucket = (bucket, key, amt) => {
      bucket[key] = (bucket[key] || 0) + amt;
    };
    for (const d of docs) {
      let r = null;
      if (d.report) r = d.report;
      else if (typeof d.reportRaw === 'string') {
        try { r = JSON.parse(d.reportRaw); } catch { r = null; }
      }
      if (!r) continue;
      // Merge numeric buckets and summary, same as aggregateResults
      for (const [k, v] of Object.entries(r.expense || {})) addBucket(agg.expense, k, v);
      for (const [k, v] of Object.entries(r.income || {})) addBucket(agg.income, k, v);
      for (const [k, v] of Object.entries(r.transfers || {})) addBucket(agg.transfers, k, v);
      for (const [k, v] of Object.entries(r.transfersOut || {})) addBucket(agg.transfersOut, k, v);
      for (const [k, v] of Object.entries(r.transfersIn || {})) addBucket(agg.transfersIn, k, v);
      if (r.summary) {
        agg.summary.expense += r.summary.expense || 0;
        agg.summary.income += r.summary.income || 0;
        agg.summary.transfers += r.summary.transfers || 0;
        agg.summary.transfersOut += r.summary.transfersOut || 0;
        agg.summary.transfersIn += r.summary.transfersIn || 0;
      }
      // Tag each entry with its source memberId so that
      // multi-member overlays can stay member-specific.
      const memberId = d.memberId || null;
      if (Array.isArray(r.entries)) {
        for (const e of r.entries) {
          const entryMemberId = e && Object.prototype.hasOwnProperty.call(e, 'memberId') ? e.memberId : memberId;
          agg.entries.push(Object.assign({}, e, { memberId: entryMemberId }));
        }
      }
    }
    return res.json(agg);
  } catch (e) {
    console.error('Aggregate saved expenses failed:', e);
    return res.status(500).json({ error: 'Failed to aggregate expenses' });
  }
});

// Aggregate selected saved uploads for the authenticated user
app.post('/my/uploads/aggregate', authMiddleware, async (req, res) => {
  try {
    const ids = (req.body && Array.isArray(req.body.ids)) ? req.body.ids : null;
    if (!ids || !ids.length) {
      return res.status(400).json({ error: 'ids array is required' });
    }
    const docs = await uploadsDb.find({ userId: req.user.id, _id: { $in: ids } });
    const reports = docs.map(d => {
      if (d.report) return d.report;
      if (typeof d.reportRaw === 'string') { try { return JSON.parse(d.reportRaw); } catch { return null; } }
      return null;
    }).filter(Boolean);
    const agg = aggregateResults(reports);
    return res.json(agg);
  } catch (e) {
    console.error('Aggregate selected uploads failed:', e);
    return res.status(500).json({ error: 'Failed to aggregate selected uploads' });
  }
});

// Save multiple uploaded files to the authenticated user's profile
app.post('/my/uploads-multi', authMiddleware, upload.array('files'), async (req, res) => {
  const files = req.files || [];
  if (!files.length) return res.status(400).json({ error: 'No files uploaded.' });
  try {
    const userId = req.user.id;
    const mapping = (await loadUserMapping(userId)) || loadGlobalMapping();
    const config = loadConfig();
    const items = [];
    const errors = [];
    for (const f of files) {
      let report = null; let parseError = null;
      try {
        report = parseExcel(f.path, mapping, config);
      } catch (e) {
        parseError = e && e.message ? e.message : String(e);
      }
      const doc = await uploadsDb.insert({
        userId,
        filename: f.filename,
        originalName: f.originalname,
        mimeType: f.mimetype,
        size: f.size,
        createdAt: new Date(),
        report: undefined,
        reportRaw: report ? JSON.stringify(report) : null,
        error: parseError || null
      });
      if (parseError) errors.push({ file: f.originalname, error: parseError });
      items.push({ id: doc._id, originalName: doc.originalName, createdAt: doc.createdAt, summary: report && report.summary ? report.summary : { expense: 0, income: 0, transfers: 0, transfersOut: 0, transfersIn: 0 }, error: parseError || null });
    }
    return res.json({ items, errors });
  } catch (e) {
    console.error('Save multi uploads failed:', e);
    return res.status(500).json({ error: 'Failed to save uploads' });
  }
});

// Get mapping; if authenticated return user's mapping, else global default
app.get('/mapping', async (req, res) => {
  try {
    const user = tryGetUserFromAuthHeader(req);
    if (user) {
      const um = await loadUserMapping(user.id);
      if (um) return res.json(um);
    }
    const gm = loadGlobalMapping();
    return res.json(gm);
  } catch (e) {
    console.error('Failed to get mapping:', e);
    return res.status(500).json({ error: 'Failed to get mapping' });
  }
});

function validateMappingSchema(incoming) {
  const errors = [];
  const warnings = [];
  if (!incoming || typeof incoming !== 'object' || Array.isArray(incoming)) {
    errors.push('Root must be an object.');
    return { errors, warnings };
  }

  const keywordLocations = new Map(); // kw -> [{main, sub}]

  for (const mainCat of Object.keys(incoming)) {
    const catObj = incoming[mainCat];
    if (!catObj || typeof catObj !== 'object') {
      errors.push(`Category '${mainCat}' must be an object.`);
      continue;
    }
    if (catObj.shortName !== undefined && typeof catObj.shortName !== 'string') {
      errors.push(`Category '${mainCat}': shortName must be a string.`);
    }
    if (catObj.keywords !== undefined && !Array.isArray(catObj.keywords)) {
      errors.push(`Category '${mainCat}': keywords must be an array.`);
    }
    const catKeywords = Array.isArray(catObj.keywords) ? catObj.keywords : [];
    for (const kw of catKeywords) {
      if (typeof kw !== 'string') {
        errors.push(`Category '${mainCat}': keyword entries must be strings.`);
        break;
      }
      const k = kw.toLowerCase();
      const arr = keywordLocations.get(k) || [];
      arr.push({ main: mainCat });
      keywordLocations.set(k, arr);
    }
    if (catObj.excludeKeywords !== undefined && !Array.isArray(catObj.excludeKeywords)) {
      errors.push(`Category '${mainCat}': excludeKeywords must be an array.`);
    } else if (Array.isArray(catObj.excludeKeywords)) {
      for (const kw of catObj.excludeKeywords) {
        if (typeof kw !== 'string') {
          errors.push(`Category '${mainCat}': excludeKeywords entries must be strings.`);
          break;
        }
      }
    }

    const subs = Array.isArray(catObj.subcategories) ? catObj.subcategories : (catObj.subcategories === undefined ? [] : null);
    if (subs === null) {
      errors.push(`Category '${mainCat}': subcategories must be an array.`);
      continue;
    }
    const subNameSet = new Set();
    for (let i = 0; i < subs.length; i++) {
      const sub = subs[i];
      if (!sub || typeof sub !== 'object') {
        errors.push(`Category '${mainCat}': subcategory at index ${i} must be an object.`);
        continue;
      }
       if (typeof sub.name !== 'string' || !sub.name.trim()) {
        errors.push(`Category '${mainCat}': subcategory at index ${i} must have a non-empty 'name'.`);
      }
      if (sub.shortName !== undefined && typeof sub.shortName !== 'string') {
        errors.push(`Category '${mainCat}/${sub.name || i}': shortName must be a string.`);
      }
      if (sub.keywords !== undefined && !Array.isArray(sub.keywords)) {
        errors.push(`Category '${mainCat}/${sub.name || i}': keywords must be an array.`);
      }
      const subKeywords = Array.isArray(sub.keywords) ? sub.keywords : [];
      for (const kw of subKeywords) {
        if (typeof kw !== 'string') {
          errors.push(`Category '${mainCat}/${sub.name || i}': keyword entries must be strings.`);
          break;
        }
        const k = kw.toLowerCase();
        const arr = keywordLocations.get(k) || [];
        arr.push({ main: mainCat, sub: sub.name || String(i) });
        keywordLocations.set(k, arr);
      }
      if (sub.excludeKeywords !== undefined && !Array.isArray(sub.excludeKeywords)) {
        errors.push(`Category '${mainCat}/${sub.name || i}': excludeKeywords must be an array.`);
      } else if (Array.isArray(sub.excludeKeywords)) {
        for (const kw of sub.excludeKeywords) {
          if (typeof kw !== 'string') {
            errors.push(`Category '${mainCat}/${sub.name || i}': excludeKeywords entries must be strings.`);
            break;
          }
        }
      }
      if (sub.rule !== undefined) {
        if (typeof sub.rule !== 'object' || Array.isArray(sub.rule)) {
          errors.push(`Category '${mainCat}/${sub.name || i}': rule must be an object.`);
        } else {
          for (const key of Object.keys(sub.rule)) {
            if (!['amountLessThan', 'amountGreaterThanOrEqual', 'amountEqualTo'].includes(key)) {
              warnings.push(`Category '${mainCat}/${sub.name || i}': unknown rule '${key}' will be ignored.`);
            } else if (typeof sub.rule[key] !== 'number') {
              errors.push(`Category '${mainCat}/${sub.name || i}': rule '${key}' must be a number.`);
            }
          }
        }
      }
      if (sub.ruleMode !== undefined) {
        if (typeof sub.ruleMode !== 'string' || !['and', 'or'].includes(sub.ruleMode.toLowerCase())) {
          warnings.push(`Category '${mainCat}/${sub.name || i}': ruleMode should be 'and' or 'or'.`);
        }
      }
      const lower = (sub.name || '').toLowerCase();
      if (lower) {
        if (subNameSet.has(lower)) {
          errors.push(`Category '${mainCat}': duplicate subcategory name '${sub.name}'.`);
        }
        subNameSet.add(lower);
      }
    }
  }

  // Keyword conflicts across categories/subcategories
  for (const [kw, locs] of keywordLocations.entries()) {
    if (locs.length > 1) {
      warnings.push({ keyword: kw, locations: locs });
    }
  }

  return { errors, warnings };
}
// Update mapping (persist category-mapping.json)
// Save mapping for the authenticated user
app.put('/mapping', authMiddleware, async (req, res) => {
  const incoming = req.body;
  const { errors, warnings } = validateMappingSchema(incoming);
  if (errors.length) {
    return res.status(400).json({ error: 'Validation failed', errors });
  }
  try {
    const userId = req.user.id;
    const existing = await mappingsDb.findOne({ userId });
    const mappingRaw = JSON.stringify(incoming);
    if (existing) {
      await mappingsDb.update({ _id: existing._id }, { $set: { mapping: undefined, mappingRaw, updatedAt: new Date() } }, { multi: false });
    } else {
      await mappingsDb.insert({ userId, mappingRaw, createdAt: new Date(), updatedAt: new Date() });
    }
    return res.json({ ok: true, warnings });
  } catch (e) {
    console.error('Failed to save user mapping:', e);
    return res.status(500).json({ error: 'Failed to save mapping.' });
  }
});

// --- Auth routes ---
app.post('/auth/register', async (req, res) => {
  try {
    const { email, password, name } = req.body || {};
    if (!email || !password) return res.status(400).json({ error: 'Email and password required' });
    const normEmail = String(email).trim().toLowerCase();
    if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(normEmail)) return res.status(400).json({ error: 'Invalid email' });
    if (String(password).length < 6) return res.status(400).json({ error: 'Password too short (min 6)' });
    const salt = bcrypt.genSaltSync(10);
    const hash = bcrypt.hashSync(password, salt);
    const user = await usersDb.insert({ email: normEmail, name: (name ? String(name).trim() : undefined), passwordHash: hash, createdAt: new Date() });
    const token = signToken(user);
    return res.json({ token, user: { id: user._id, email: user.email, name: user.name || null } });
  } catch (e) {
    if (e && e.errorType === 'uniqueViolated') {
      return res.status(409).json({ error: 'Email already registered' });
    }
    console.error('Register failed:', e);
    return res.status(500).json({ error: 'Registration failed' });
  }
});

app.post('/auth/login', async (req, res) => {
  try {
    const { email, password } = req.body || {};
    if (!email || !password) return res.status(400).json({ error: 'Email and password required' });
    const normEmail = String(email).trim().toLowerCase();
    const user = await usersDb.findOne({ email: normEmail });
    if (!user) return res.status(401).json({ error: 'Invalid credentials' });
    const ok = bcrypt.compareSync(String(password), user.passwordHash);
    if (!ok) return res.status(401).json({ error: 'Invalid credentials' });
    const token = signToken(user);
    return res.json({ token, user: { id: user._id, email: user.email, name: user.name || null } });
  } catch (e) {
    console.error('Login failed:', e);
    return res.status(500).json({ error: 'Login failed' });
  }
});

// --- Member-specific Transfer Rules ---
// Schema: one document per (userId, memberId) pair
// { userId, memberId, rules: [ { keyword: string, name: string } ], createdAt, updatedAt }

function validateTransferRules(rules) {
  const errors = [];
  if (!Array.isArray(rules)) return { errors: ['rules must be an array'] };
  for (let i = 0; i < rules.length; i++) {
    const r = rules[i];
    if (!r || typeof r !== 'object') { errors.push(`rules[${i}] must be an object`); continue; }
    if (typeof r.keyword !== 'string' || !r.keyword.trim()) errors.push(`rules[${i}].keyword must be a non-empty string`);
    if (typeof r.name !== 'string' || !r.name.trim()) errors.push(`rules[${i}].name must be a non-empty string`);
  }
  return { errors };
}

// Get transfer rules for a member
app.get('/my/transfer-rules/:memberId', authMiddleware, async (req, res) => {
  try {
    const memberId = String(req.params.memberId || '').trim();
    if (!memberId) return res.status(400).json({ error: 'memberId is required' });
    let doc = await transferRulesDb.findOne({ userId: req.user.id, memberId });
    if (!doc) {
      // Initialize empty rules doc lazily
      doc = await transferRulesDb.insert({ userId: req.user.id, memberId, rules: [], createdAt: new Date(), updatedAt: new Date() });
    }
    return res.json({ memberId, rules: Array.isArray(doc.rules) ? doc.rules : [] });
  } catch (e) {
    console.error('Get transfer rules failed:', e);
    return res.status(500).json({ error: 'Failed to get transfer rules' });
  }
});

// --- Member-specific Transfer Mapping (visual editor style) ---
// Schema matches category-mapping.json
app.get('/my/member-mapping/:memberId', authMiddleware, async (req, res) => {
  try {
    const memberId = String(req.params.memberId || '').trim();
    if (!memberId) return res.status(400).json({ error: 'memberId is required' });
    let doc = await memberMappingsDb.findOne({ userId: req.user.id, memberId });
    if (!doc) return res.json({});
    if (doc.mapping && typeof doc.mapping === 'object') return res.json(doc.mapping); // backward compat
    if (typeof doc.mappingRaw === 'string') {
      try { return res.json(JSON.parse(doc.mappingRaw)); } catch { return res.json({}); }
    }
    return res.json({});
  } catch (e) {
    console.error('Get member mapping failed:', e);
    return res.status(500).json({ error: 'Failed to get member mapping' });
  }
});

app.put('/my/member-mapping/:memberId', authMiddleware, async (req, res) => {
  try {
    const memberId = String(req.params.memberId || '').trim();
    if (!memberId) return res.status(400).json({ error: 'memberId is required' });
    const incoming = req.body;
    const { errors, warnings } = validateMappingSchema(incoming);
    if (errors.length) return res.status(400).json({ error: 'Validation failed', errors });
    const existing = await memberMappingsDb.findOne({ userId: req.user.id, memberId });
    if (existing) {
      await memberMappingsDb.update({ _id: existing._id }, { $set: { mapping: undefined, mappingRaw: JSON.stringify(incoming), updatedAt: new Date() } });
    } else {
      await memberMappingsDb.insert({ userId: req.user.id, memberId, mappingRaw: JSON.stringify(incoming), createdAt: new Date(), updatedAt: new Date() });
    }
    return res.json({ ok: true, warnings });
  } catch (e) {
    console.error('Save member mapping failed:', e);
    return res.status(500).json({ error: 'Failed to save member mapping' });
  }
});

// --- Member-specific Investment Mapping (visual editor style) ---
// Schema matches category-mapping.json
app.get('/my/investment-mapping/:memberId', authMiddleware, async (req, res) => {
  try {
    const memberId = String(req.params.memberId || '').trim();
    if (!memberId) return res.status(400).json({ error: 'memberId is required' });
    let doc = await investmentMappingsDb.findOne({ userId: req.user.id, memberId });
    if (!doc) return res.json({});
    if (doc.mapping && typeof doc.mapping === 'object') return res.json(doc.mapping);
    if (typeof doc.mappingRaw === 'string') {
      try { return res.json(JSON.parse(doc.mappingRaw)); } catch { return res.json({}); }
    }
    return res.json({});
  } catch (e) {
    console.error('Get investment mapping failed:', e);
    return res.status(500).json({ error: 'Failed to get investment mapping' });
  }
});

app.put('/my/investment-mapping/:memberId', authMiddleware, async (req, res) => {
  try {
    const memberId = String(req.params.memberId || '').trim();
    if (!memberId) return res.status(400).json({ error: 'memberId is required' });
    const incoming = req.body;
    const { errors, warnings } = validateMappingSchema(incoming);
    if (errors.length) return res.status(400).json({ error: 'Validation failed', errors });
    const existing = await investmentMappingsDb.findOne({ userId: req.user.id, memberId });
    if (existing) {
      await investmentMappingsDb.update({ _id: existing._id }, { $set: { mapping: undefined, mappingRaw: JSON.stringify(incoming), updatedAt: new Date() } });
    } else {
      await investmentMappingsDb.insert({ userId: req.user.id, memberId, mappingRaw: JSON.stringify(incoming), createdAt: new Date(), updatedAt: new Date() });
    }
    return res.json({ ok: true, warnings });
  } catch (e) {
    console.error('Save investment mapping failed:', e);
    return res.status(500).json({ error: 'Failed to save investment mapping' });
  }
});

// Replace transfer rules for a member
app.put('/my/transfer-rules/:memberId', authMiddleware, async (req, res) => {
  try {
    const memberId = String(req.params.memberId || '').trim();
    if (!memberId) return res.status(400).json({ error: 'memberId is required' });
    const rules = (req.body && req.body.rules) ? req.body.rules : [];
    const { errors } = validateTransferRules(rules);
    if (errors.length) return res.status(400).json({ error: 'Validation failed', errors });
    const existing = await transferRulesDb.findOne({ userId: req.user.id, memberId });
    if (existing) {
      await transferRulesDb.update({ _id: existing._id }, { $set: { rules, updatedAt: new Date() } });
    } else {
      await transferRulesDb.insert({ userId: req.user.id, memberId, rules, createdAt: new Date(), updatedAt: new Date() });
    }
    return res.json({ ok: true });
  } catch (e) {
    console.error('Save transfer rules failed:', e);
    return res.status(500).json({ error: 'Failed to save transfer rules' });
  }
});

// Profile: get current user
app.get('/me', authMiddleware, async (req, res) => {
  try {
    const user = await usersDb.findOne({ _id: req.user.id });
    if (!user) return res.status(404).json({ error: 'User not found' });
    return res.json({ id: user._id, email: user.email, name: user.name || '' });
  } catch (e) {
    console.error('/me failed:', e);
    return res.status(500).json({ error: 'Failed to load profile' });
  }
});

// Profile: update name
app.put('/me', authMiddleware, async (req, res) => {
  try {
    const name = (req.body && req.body.name !== undefined) ? String(req.body.name).trim() : undefined;
    if (name === undefined) return res.status(400).json({ error: 'Name is required' });
    if (name.length > 60) return res.status(400).json({ error: 'Name too long (max 60)' });
    await usersDb.update({ _id: req.user.id }, { $set: { name } });
    const user = await usersDb.findOne({ _id: req.user.id });
    return res.json({ id: user._id, email: user.email, name: user.name || '' });
  } catch (e) {
    console.error('Update /me failed:', e);
    return res.status(500).json({ error: 'Failed to update profile' });
  }
});

// Auth: change password
app.post('/auth/change-password', authMiddleware, async (req, res) => {
  try {
    const { currentPassword, newPassword } = req.body || {};
    if (!currentPassword || !newPassword) return res.status(400).json({ error: 'Both current and new passwords are required' });
    if (String(newPassword).length < 6) return res.status(400).json({ error: 'New password too short (min 6)' });
    const user = await usersDb.findOne({ _id: req.user.id });
    if (!user) return res.status(404).json({ error: 'User not found' });
    const ok = bcrypt.compareSync(String(currentPassword), user.passwordHash);
    if (!ok) return res.status(401).json({ error: 'Current password is incorrect' });
    const salt = bcrypt.genSaltSync(10);
    const hash = bcrypt.hashSync(String(newPassword), salt);
    await usersDb.update({ _id: req.user.id }, { $set: { passwordHash: hash } });
    return res.json({ ok: true });
  } catch (e) {
    console.error('Change password failed:', e);
    return res.status(500).json({ error: 'Failed to change password' });
  }
});

// --- Family profiles ---
// Get or initialize family for current user
app.get('/my/family', authMiddleware, async (req, res) => {
  try {
    let fam = await familiesDb.findOne({ userId: req.user.id });
    if (!fam) {
      fam = await familiesDb.insert({ userId: req.user.id, members: [], primaryId: null, createdAt: new Date(), updatedAt: new Date() });
    }
    const members = (fam.members || []).map(m => ({ id: m.id, name: m.name, isPrimary: m.id === fam.primaryId }));
    return res.json({ members, primaryId: fam.primaryId });
  } catch (e) {
    console.error('Get family failed:', e);
    return res.status(500).json({ error: 'Failed to get family' });
  }
});

// Add a member
app.post('/my/family/members', authMiddleware, async (req, res) => {
  try {
    const name = (req.body && req.body.name) ? String(req.body.name).trim() : '';
    if (!name) return res.status(400).json({ error: 'Name is required' });
    let fam = await familiesDb.findOne({ userId: req.user.id });
    if (!fam) { fam = await familiesDb.insert({ userId: req.user.id, members: [], primaryId: null, createdAt: new Date(), updatedAt: new Date() }); }
    const id = genId();
    const members = Array.isArray(fam.members) ? fam.members.slice() : [];
    members.push({ id, name });
    const primaryId = fam.primaryId || id; // first member becomes primary by default
    await familiesDb.update({ _id: fam._id }, { $set: { members, primaryId, updatedAt: new Date() } });
    return res.json({ id, name, isPrimary: id === primaryId });
  } catch (e) {
    console.error('Add member failed:', e);
    return res.status(500).json({ error: 'Failed to add member' });
  }
});

// Update a member (name and/or set as primary)
app.put('/my/family/members/:id', authMiddleware, async (req, res) => {
  try {
    let fam = await familiesDb.findOne({ userId: req.user.id });
    if (!fam) return res.status(404).json({ error: 'Family not found' });
    const id = req.params.id;
    const name = (req.body && req.body.name !== undefined) ? String(req.body.name).trim() : undefined;
    const setPrimary = (req.body && req.body.isPrimary === true) ? true : false;
    const members = Array.isArray(fam.members) ? fam.members.slice() : [];
    const idx = members.findIndex(m => m.id === id);
    if (idx === -1) return res.status(404).json({ error: 'Member not found' });
    if (name !== undefined) members[idx].name = name;
    const primaryId = setPrimary ? id : fam.primaryId;
    await familiesDb.update({ _id: fam._id }, { $set: { members, primaryId, updatedAt: new Date() } });
    return res.json({ ok: true });
  } catch (e) {
    console.error('Update member failed:', e);
    return res.status(500).json({ error: 'Failed to update member' });
  }
});

// Delete a member
app.delete('/my/family/members/:id', authMiddleware, async (req, res) => {
  try {
    let fam = await familiesDb.findOne({ userId: req.user.id });
    if (!fam) return res.status(404).json({ error: 'Family not found' });
    const id = req.params.id;
    const members = Array.isArray(fam.members) ? fam.members.slice() : [];
    const idx = members.findIndex(m => m.id === id);
    if (idx === -1) return res.status(404).json({ error: 'Member not found' });
    members.splice(idx, 1);
    let primaryId = fam.primaryId;
    if (primaryId === id) {
      primaryId = members.length ? members[0].id : null;
    }
    await familiesDb.update({ _id: fam._id }, { $set: { members, primaryId, updatedAt: new Date() } });
    return res.json({ ok: true });
  } catch (e) {
    console.error('Delete member failed:', e);
    return res.status(500).json({ error: 'Failed to delete member' });
  }
});

// Assign a saved upload to a member
app.put('/my/uploads/:id/member', authMiddleware, async (req, res) => {
  try {
    const memberId = (req.body && req.body.memberId) ? String(req.body.memberId).trim() : null;
    const doc = await uploadsDb.findOne({ _id: req.params.id, userId: req.user.id });
    if (!doc) return res.status(404).json({ error: 'Upload not found' });
    // Validate member exists (if provided)
    if (memberId) {
      const fam = await familiesDb.findOne({ userId: req.user.id });
      const exists = fam && Array.isArray(fam.members) && fam.members.some(m => m.id === memberId);
      if (!exists) return res.status(400).json({ error: 'Invalid memberId' });
    }
    await uploadsDb.update({ _id: doc._id }, { $set: { memberId: memberId || null } });
    return res.json({ ok: true });
  } catch (e) {
    console.error('Set upload member failed:', e);
    return res.status(500).json({ error: 'Failed to set upload member' });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});