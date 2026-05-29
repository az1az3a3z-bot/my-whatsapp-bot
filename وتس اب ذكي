// ╔══════════════════════════════════════════════════╗
// ║  🤖 DOMA BOT PRO - V5.4.2 (Safe & Resilient)   ║
// ║  👑 المطور: عبد الرحمن                          ║
// ║  ⚡ Enterprise Messaging Service - Final Complete ║
// ╚══════════════════════════════════════════════════╝

const os = require('os');
process.env.UV_THREADPOOL_SIZE = String(Math.min(os.cpus().length * 2, 16));

const express = require('express');
const helmet = require('helmet');
const compression = require('compression');
const rateLimit = require('express-rate-limit');
const { LRUCache } = require('lru-cache');
const { 
    default: makeWASocket,
    DisconnectReason,
    fetchLatestBaileysVersion,
    makeCacheableSignalKeyStore,
    initAuthCreds,
    downloadMediaMessage
} = require('@whiskeysockets/baileys');
const { MongoClient } = require('mongodb');
const { default: PQueue } = require('p-queue');
const pino = require('pino');
const QRCode = require('qrcode');
const sharp = require('sharp');
const { monitorEventLoopDelay } = require('perf_hooks');

// ═══════════════════════════════════════════════════
// 🔧 إعدادات Sharp
// ═══════════════════════════════════════════════════
sharp.cache(false);
sharp.concurrency(2);

// ═══════════════════════════════════════════════════
// 📝 Structured Logging
// ═══════════════════════════════════════════════════
const appLogger = pino({
    level: process.env.LOG_LEVEL || 'info',
    transport: process.env.NODE_ENV !== 'production' ? {
        target: 'pino-pretty',
        options: { colorize: true }
    } : undefined
});
const silentLogger = pino({ level: 'silent' });

// ═══════════════════════════════════════════════════
// 🛡️ إيقاف طارئ (مع مؤقتات آمنة)
// ═══════════════════════════════════════════════════
let httpServer;
const activeTimers = new Set();

function setSafeTimeout(fn, ms) {
    const id = setTimeout(() => { activeTimers.delete(id); fn(); }, ms);
    activeTimers.add(id);
    id.unref?.();
    return id;
}

function setSafeInterval(fn, ms) {
    const id = setInterval(fn, ms);
    activeTimers.add(id);
    id.unref?.();
    return id;
}

function clearAllTimers() {
    for (const id of activeTimers) { clearTimeout(id); clearInterval(id); }
    activeTimers.clear();
}

// ✅ حفظ آمن للـ creds (مع نسخ الحقول الثنائية)
let pendingCreds = null;
let credsFlushRunning = false;
async function flushCreds() {
    if (credsFlushRunning || !pendingCreds) return;
    credsFlushRunning = true;
    try {
        while (pendingCreds) {
            const credsToSave = pendingCreds;
            pendingCreds = null;
            await ensureDb();
            await db.collection('creds').updateOne(
                { _id: 'creds' },
                { $set: { value: credsToSave } },
                { upsert: true, writeConcern: { w: 'majority', j: true } }
            );
        }
    } finally {
        credsFlushRunning = false;
    }
}

// ✅ نظام تحديثات المستخدمين: فصل تام بين الكتابة المباشرة وإعادة المحاولة
const pendingUsers = new Map();
const failedUserUpdates = new Map(); // <-- مخزن منفصل للمحاولات الفاشلة

async function flushUserUpdates() {
    // دمج التحديثات الفاشلة السابقة (إن وجدت) مع الدفعة الحالية
    if (failedUserUpdates.size > 0) {
        for (const [number, data] of failedUserUpdates) {
            const existing = pendingUsers.get(number) || {};
            pendingUsers.set(number, { ...existing, ...data });
        }
        failedUserUpdates.clear();
    }

    if (pendingUsers.size === 0) return;

    // أخذ نسخة من البيانات الحالية للكتابة
    const snapshot = new Map(pendingUsers);
    pendingUsers.clear(); // تفريغ الأصلية فوراً لاستقبال تحديثات جديدة

    const bulkOps = [];
    for (const [number, data] of snapshot.entries()) {
        bulkOps.push({
            updateOne: {
                filter: { _id: number },
                update: { $set: data, $setOnInsert: { firstSeen: new Date().toISOString() } },
                upsert: true
            }
        });
    }

    // الكتابة إلى قاعدة البيانات على دفعات
    for (let i = 0; i < bulkOps.length; i += 1000) {
        const chunk = bulkOps.slice(i, i + 1000);
        try {
            await db.collection('users').bulkWrite(chunk, { ordered: false, maxTimeMS: 10000 });
        } catch (e) {
            appLogger.error(e, `فشل كتابة دفعة من تحديثات المستخدمين (من ${i} إلى ${i + chunk.length})`);
            // ✅ في حالة الفشل، نعيد هذه الدفعة إلى مخزن "إعادة المحاولة" المنفصل
            for (const op of chunk) {
                const num = op.updateOne.filter._id;
                const setData = op.updateOne.update.$set;
                const existingFailed = failedUserUpdates.get(num) || {};
                failedUserUpdates.set(num, { ...existingFailed, ...setData });
            }
        }
    }
}
setSafeInterval(flushUserUpdates, 5000);

async function emergencyShutdown(code = 1) {
    appLogger.fatal('🚨 Emergency shutdown');
    socketDraining = true;
    reconnectState = -1;
    await flushCreds();
    await flushUserUpdates(); // محاولة أخيرة لتفريغ كل التحديثات
    clearAllTimers();
    try { queue.pause(); } catch {}
    try { broadcastQueue.pause(); } catch {}
    try { mediaQueue.pause(); } catch {}
    try { sock?.ws?.close(); } catch {}
    try { await client.close(); } catch {}
    try { httpServer?.close(); } catch {}
    setTimeout(() => process.exit(code), 1000).unref();
}

process.on('uncaughtException', async (err) => {
    appLogger.fatal(err, '💥 Uncaught Exception');
    await emergencyShutdown(1);
});

// ✅ معالجة ذكية للـ unhandledRejection: لا تقتل العملية إلا في الحالات القصوى
process.on('unhandledRejection', async (reason) => {
    appLogger.error(reason, '💥 Unhandled Rejection');

    // فقط ReferenceError أو أخطاء تحمل كلمة FATAL تؤدي لإيقاف طارئ
    if (reason instanceof ReferenceError) {
        await emergencyShutdown(1);
        return;
    }

    if (String(reason?.message || '').toUpperCase().includes('FATAL')) {
        await emergencyShutdown(1);
    }
    // أي خطأ آخر: النظام يستمر في العمل (degraded mode)
});

async function gracefulShutdown() {
    appLogger.info('🛑 Graceful shutdown...');
    socketDraining = true;
    reconnectState = -1;
    await flushCreds();
    await flushUserUpdates(); // محاولة أخيرة لتفريغ كل التحديثات
    clearAllTimers();
    try {
        await Promise.race([
            Promise.all([queue.onIdle(), broadcastQueue.onIdle(), mediaQueue.onIdle()]),
            new Promise(r => setTimeout(r, 15000).unref())
        ]);
    } catch {}
    try { sock?.ws?.close(); } catch {}
    try { await client.close(); } catch {}
    setTimeout(() => process.exit(0), 1000).unref();
}
process.on('SIGINT', gracefulShutdown);
process.on('SIGTERM', gracefulShutdown);

// ═══════════════════════════════════════════════════
// ⚙️ الإعدادات الأساسية
// ═══════════════════════════════════════════════════
const OWNER_NUMBER = "584164041083";
const BOT_NUMBER = "260752535332";
const ADMIN_NUMBERS = ["584164041083"];
const OWNER_NAME = "عبد الرحمن";
const BOT_NAME = "ᘓɹ̇⎽🥂ᓄ҉ᓗᩭ⅃ใ ລ⎽ɹ̣⎽ᘓ";
const ADMIN_KEYWORD = "عبدو";
const MENU_IMAGE_URL = "https://i.ibb.co/vChK2Y35/IMG.jpg";
const MAX_MEDIA_SIZE = 15 * 1024 * 1024;
const SEND_INTERVAL = 1500;
const MAX_BROADCAST_USERS = 500;
const BROADCAST_BATCH_SIZE = 10;
const BROADCAST_BATCH_DELAY = 1500;
const MAX_QUEUE_SIZE = 250;
const FLOOD_WINDOW_MS = 1000;
const MAX_FLOOD_PER_WINDOW = 50;
const MAX_TEXT_LENGTH = 5000;

// LRU Caches
const processedMessages = new LRUCache({ max: 5000, ttl: 1000 * 60 * 10 });
const groupMetaCache = new LRUCache({ max: 500, ttl: 1000 * 60 * 5 });
const keyCache = new LRUCache({
    maxSize: 50 * 1024 * 1024,
    sizeCalculation: (v) => {
        try { return Buffer.byteLength(JSON.stringify(v)); } catch { return 1024; }
    },
    ttl: 1000 * 60 * 5
});
const jidQueues = new Map();
const spamTrackers = new Map();
const imageCooldown = new Map();
const waitingBroadcast = new Set();
const commandCooldowns = new Map();
const BROADCAST_IDS = new LRUCache({ max: 5000, ttl: 1000 * 60 * 60 });
let settingsCache = null;
let settingsCacheTime = 0;
const SETTINGS_CACHE_TTL = 30000;

const metrics = {
    incomingMessages: 0,
    outgoingMessages: 0,
    failedMessages: 0,
    imagesProcessed: 0,
    spamBlocked: 0
};

// Token Bucket عالمي
const GLOBAL_RATE = {
    capacity: 20,
    tokens: 20,
    refillRate: 20,
    lastRefill: performance.now()
};

function consumeGlobalToken() {
    const now = performance.now();
    const elapsed = (now - GLOBAL_RATE.lastRefill) / 1000;
    GLOBAL_RATE.tokens = Math.min(GLOBAL_RATE.capacity, GLOBAL_RATE.tokens + elapsed * GLOBAL_RATE.refillRate);
    GLOBAL_RATE.lastRefill = now;
    if (GLOBAL_RATE.tokens < 1) return false;
    GLOBAL_RATE.tokens -= 1;
    return true;
}

// ✅ نافذة فيضان بحلقة دائرية (ring-buffer)
const FLOOD_BUFFER_SIZE = 2048;
const floodBuffer = new Uint32Array(FLOOD_BUFFER_SIZE);
let floodIndex = 0;
let floodCount = 0;

function checkGlobalFlood() {
    const now = Date.now();
    const currentSlot = Math.floor(now / FLOOD_WINDOW_MS);
    
    // إزالة الطوابع القديمة (مؤشر دائري)
    while (floodCount > 0 && Math.floor(Date.now() / FLOOD_WINDOW_MS) - currentSlot > FLOOD_BUFFER_SIZE) {
        const oldIndex = (floodIndex - floodCount + FLOOD_BUFFER_SIZE) % FLOOD_BUFFER_SIZE;
        floodBuffer[oldIndex] = 0;
        floodCount--;
    }
    
    // إضافة الطابع الحالي
    const targetSlot = currentSlot % FLOOD_BUFFER_SIZE;
    if (floodBuffer[targetSlot] < MAX_FLOOD_PER_WINDOW) {
        floodBuffer[targetSlot]++;
        floodCount++;
        floodIndex = (targetSlot + 1) % FLOOD_BUFFER_SIZE;
        return false;
    }
    return true;
}

let socketDraining = false;

// طوابير
const queue = new PQueue({ interval: 1000, intervalCap: 6, concurrency: 3, timeout: 30000, throwOnTimeout: true });
queue.on('error', err => appLogger.error(err, 'Queue error'));
const broadcastQueue = new PQueue({ interval: 1000, intervalCap: 20, concurrency: 10, timeout: 30000, throwOnTimeout: true });
broadcastQueue.on('error', err => appLogger.error(err, 'Broadcast Queue error'));
const mediaQueue = new PQueue({ concurrency: 1, timeout: 30000, throwOnTimeout: true });
mediaQueue.on('error', err => appLogger.error(err, 'Media queue error'));

let lastQueueProgress = Date.now();
queue.on('active', () => { lastQueueProgress = Date.now(); });
broadcastQueue.on('active', () => { lastQueueProgress = Date.now(); });

function getJidQueue(jid) {
    if (!jidQueues.has(jid)) {
        const jidQueue = new PQueue({
            concurrency: 1,
            interval: SEND_INTERVAL,
            intervalCap: 1,
            timeout: 30000,
            throwOnTimeout: true
        });
        jidQueue.on('idle', () => {
            // تنظيف آمن عند الخمول
            if (jidQueue.size === 0 && jidQueue.pending === 0) {
                jidQueues.delete(jid);
            }
        });
        jidQueues.set(jid, jidQueue);
    }
    return jidQueues.get(jid);
}

function sendMessage(jid, content, options = {}) {
    if (!consumeGlobalToken()) return Promise.reject(new Error('Global rate limit'));
    if (socketDraining) return Promise.reject(new Error('Socket draining'));
    return getJidQueue(jid).add(() => safeSend(jid, content, options));
}
function enqueue(task, targetQueue = queue) {
    if (socketDraining) return;
    return targetQueue.add(task).catch(err => appLogger.error(err, 'Queue task failed'));
}

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/doma_bot';

// ═══════════════════════════════════════════════════
// 🗄️ MongoDB (مع قاطع دائرة مكتمل)
// ═══════════════════════════════════════════════════
const client = new MongoClient(MONGODB_URI, {
    maxPoolSize: 20, minPoolSize: 1,
    serverSelectionTimeoutMS: 10000, socketTimeoutMS: 45000,
    retryWrites: true, writeConcern: { w: 'majority', j: true }
});
let db, dbConnected = false, dbDownUntil = 0, dbConnecting = null, indexesEnsured = false;
let dbCircuitOpenUntil = 0;
let dbSlowCount = 0;

client.on('close', () => { dbConnected = false; appLogger.warn('⚠️ Mongo disconnected'); });

async function connectToDatabase() {
    if (dbConnected) return;
    if (Date.now() < dbDownUntil) return;
    if (dbConnecting) return dbConnecting;
    dbConnecting = (async () => {
        try {
            await client.connect();
            db = client.db('doma_bot');
            dbConnected = true;
            dbDownUntil = 0;
            if (!indexesEnsured) { await ensureIndexes(); indexesEnsured = true; }
        } catch (err) { appLogger.error(err, '❌ فشل الاتصال بـ MongoDB'); dbDownUntil = Date.now() + 30000; }
        finally { dbConnecting = null; }
    })();
    return dbConnecting;
}

async function ensureIndexes() {
    try { await db.collection('users').createIndex({ lastActive: 1 }); await db.collection('imageLog').createIndex({ time: -1 }); } catch {}
}

async function ensureDb() { if (!dbConnected || !db) await connectToDatabase(); if (!db) throw new Error('Database unavailable'); }

async function withDbRead(operation) {
    if (Date.now() < dbCircuitOpenUntil) return null;
    try {
        const result = await Promise.race([
            operation(),
            new Promise((_, reject) => setTimeout(() => reject(new Error('DB slow')), 5000).unref())
        ]);
        dbSlowCount = 0;
        return result;
    } catch {
        dbSlowCount++;
        if (dbSlowCount >= 5) {
            dbCircuitOpenUntil = Date.now() + 30000;
            dbSlowCount = 0;
            appLogger.error('💥 DB circuit breaker activated');
        }
        return null;
    }
}

async function withDbWrite(operation) {
    if (Date.now() < dbCircuitOpenUntil) throw new Error('DB circuit open');
    try {
        const result = await Promise.race([
            operation(),
            new Promise((_, reject) => setTimeout(() => reject(new Error('DB slow')), 5000).unref())
        ]);
        dbSlowCount = 0;
        return result;
    } catch {
        dbSlowCount++;
        if (dbSlowCount >= 5) {
            dbCircuitOpenUntil = Date.now() + 30000;
            dbSlowCount = 0;
            appLogger.error('💥 DB circuit breaker activated');
        }
        throw new Error('DB write failed');
    }
}

async function getUser(number) { return withDbRead(() => db.collection('users').findOne({ _id: number }, { maxTimeMS: 5000 })); }
function queueUserUpdate(number, data) {
    const existing = pendingUsers.get(number) || {};
    pendingUsers.set(number, { ...existing, ...data });
}
async function getGroup(jid) { return withDbRead(() => db.collection('groups').findOne({ _id: jid }, { maxTimeMS: 5000 })); }
async function updateGroup(jid, data) { return withDbWrite(() => db.collection('groups').updateOne({ _id: jid }, { $set: data }, { upsert: true, maxTimeMS: 5000 })); }
async function getWarnings(number) { const doc = await withDbRead(() => db.collection('warnings').findOne({ _id: number }, { maxTimeMS: 5000 })); return doc?.count || 0; }
async function setWarnings(number, count) { return withDbWrite(() => db.collection('warnings').updateOne({ _id: number }, { $set: { count } }, { upsert: true, maxTimeMS: 5000 })); }
async function isBanned(number) { return !!(await withDbRead(() => db.collection('banned').findOne({ _id: number }, { maxTimeMS: 5000 }))); }
async function banUser(number) { return withDbWrite(() => db.collection('banned').updateOne({ _id: number }, { $set: { time: new Date().toISOString() } }, { upsert: true, maxTimeMS: 5000 })); }
async function unbanUser(number) { return withDbWrite(() => db.collection('banned').deleteOne({ _id: number }, { maxTimeMS: 5000 })); }
async function logImage(user, number) { await withDbWrite(() => db.collection('imageLog').insertOne({ user, number, time: new Date().toISOString() }, { maxTimeMS: 5000 })); metrics.imagesProcessed++; }
async function getImageLog(limit = 10) { return withDbRead(() => db.collection('imageLog').find().sort({ time: -1 }).limit(limit).toArray()) || []; }

async function getSettings() {
    const now = Date.now();
    if (settingsCache && (now - settingsCacheTime) < SETTINGS_CACHE_TTL) return settingsCache;
    const doc = await withDbRead(() => db.collection('settings').findOne({ _id: 'main' }, { maxTimeMS: 5000 }));
    settingsCache = doc || { paidMode: false };
    settingsCacheTime = now;
    return settingsCache;
}
async function setSettings(data) {
    settingsCache = null;
    return withDbWrite(() => db.collection('settings').updateOne({ _id: 'main' }, { $set: data }, { upsert: true, maxTimeMS: 5000 }));
}

// KeyStore مع تخزين مؤقت محلي بحجم محسوب
function createKeyStore() {
    return makeCacheableSignalKeyStore({
        get: async (type, id) => {
            const keyId = `${type}-${id}`;
            const cached = keyCache.get(keyId);
            if (cached) return cached;
            await ensureDb();
            const doc = await db.collection('keys').findOne({ _id: keyId });
            const value = doc?.value;
            if (value !== undefined) keyCache.set(keyId, value);
            return value;
        },
        set: async (data) => {
            await ensureDb();
            if (!data || typeof data !== 'object') return;
            const bulkOps = [];
            for (const [type, ids] of Object.entries(data)) {
                if (typeof ids === 'object' && ids !== null) {
                    for (const [id, value] of Object.entries(ids)) {
                        const keyId = `${type}-${id}`;
                        if (value) {
                            bulkOps.push({ updateOne: { filter: { _id: keyId }, update: { $set: { value } }, upsert: true } });
                            keyCache.set(keyId, value);
                        } else {
                            bulkOps.push({ deleteOne: { filter: { _id: keyId } } });
                            keyCache.delete(keyId);
                        }
                    }
                }
            }
            if (bulkOps.length > 0) await db.collection('keys').bulkWrite(bulkOps, { ordered: false });
        },
        clear: async () => {
            await ensureDb();
            await db.collection('keys').deleteMany({});
            keyCache.clear();
        }
    }, silentLogger);
}

// ═══════════════════════════════════════════════════
// 🔧 دوال وثوابت (مع تحسين regex)
// ═══════════════════════════════════════════════════
const BADWORDS = [
    'كس', 'طيز', 'منيوك', 'متناك', 'شرموط', 'قحبه', 'زب', 'بزاز',
    'كس امك', 'كس اختك', 'احا', 'خول', 'عرص', 'لبوه', 'منيوج',
    'fuck', 'shit', 'bitch', 'ass', 'dick', 'pussy', 'whore', 'bastard',
    'nigga', 'faggot', 'slut', 'cunt', 'motherfucker', 'nigger'
];
const escapedBadwords = BADWORDS.map(w => w.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
const badwordsRegex = BADWORDS.length ? new RegExp(`(^|\\s|[^\\p{L}\\p{N}])(${escapedBadwords.join('|')})(?=$|\\s|[^\\p{L}\\p{N}])`, 'iu') : null;
const linkRegex = /\b((https?:\/\/)?(www\.)?(wa\.me|wa․me|t\.me|[\w-]+\.(com|net|org|io|gg|co)))\b/i;

function cleanTextForBadwords(text) {
    return text
        .replace(/[\u064B-\u065F\u0610-\u061A\u06D6-\u06ED]/g, '')
        .normalize('NFKC')
        .replace(/(.)\1+/g, '$1')
        .toLowerCase();
}

const smartReplies = {
    'اسمك': [`أنا ${BOT_NAME} في خدمتك!`, 'اسمي عبد الرحمن، بوت ذكي'],
    'مساء': [
        'مساء التوكل عَلى مَن لا يُخيْب الرَجآء به ربي إجعَل حيَآاتِي و حيَآة من أحب نُور على نُور \n*مسـْــْ࿔ـْــْاء الـْــْ࿔ـْــْورد🌸*',
        'مساء الهدوء والاجواِء الجميلهہ َۈالامنيات الصغيرهَ المتطايرهہ نحو السمااِء مساِء ڳل شيء..جَميل ♡♡♡.. ﻣ̑ﺳـُـاء Ιﻟخيرات -❥'
    ],
    'صباح': [
        '*اَللَّهُـــــــــــــــمَّ :* \n.                   أرحم\n                 نفوسنــا\n             وتولاها بحفظك\n         وارزقُنــا  من فيض\n      كرمك سعـادةً  لا تنْقطع\n  و اشرح  صدورنا و يسر أمورنا\nاللهم  صبّحنا  صباحاً  تنشرح  فيه\n    الصدور ، و تقبل فيه  التوبة\n        و تتسع  فيه   الأرزاق\n           ياخير  من  سُئل\n               وأكرم  من\n                 أعطى\n   \n  *صَـبَـآحُ الـخَير*🌹',
        'صباح النور والسرور! ☀️'
    ],
    'عامل ايه': ['الحمد لله، بخير ونعمة! 😊', 'تمام، في خدمتك!'],
    'شكرا': ['العفو يا غالي! 🌹', 'لا شكر على واجب!'],
    'باي': ['مع السلامة! 👋', 'الله يحفظك!'],
    'احبك': ['وأنا كمان بحبك! ❤️', 'حبيبي تسلم! 🫶'],
};
const WELCOME_MESSAGES = [
    "🌟 نورت الجروب يا {name}!",
    "🎉 أهلاً وسهلاً {name} في الجروب!",
    "💫 مرحباً {name}، نورتنا!"
];

function isOwner(senderNumber) { return senderNumber.replace(/\D/g, '') === OWNER_NUMBER.replace(/\D/g, ''); }
function isAdmin(senderNumber) {
    const cleanNum = senderNumber.replace(/\D/g, '');
    if (cleanNum === OWNER_NUMBER.replace(/\D/g, '')) return true;
    return ADMIN_NUMBERS.some(admin => cleanNum === admin.replace(/\D/g, ''));
}
function getSenderNumber(msg) {
    const participant = msg.key.participant || '';
    const remoteJid = msg.key.remoteJid || '';
    let raw = (participant || remoteJid).split('@')[0];
    let cleaned = raw.replace(/\D/g, '');
    for (const admin of ADMIN_NUMBERS) if (cleaned === admin.replace(/\D/g, '')) return admin;
    return cleaned;
}
function isValidJid(number) { return /^\d{7,13}$/.test(number); }

// ✅ تعريف المتغير والدالة الخاصة بقاطع واتساب
let waCircuitOpenUntil = 0;
function isWaCircuitOpen() {
    return Date.now() < waCircuitOpenUntil;
}

// ✅ دالة الإرسال الآمن (مع exponential jitter)
async function safeSend(jid, content, options = {}, retries = 2) {
    if (!sock?.user || socketDraining) throw new Error('Socket unavailable');
    if (isWaCircuitOpen()) throw new Error('WhatsApp circuit breaker active');
    
    for (let i = 0; i <= retries; i++) {
        try {
            const result = await Promise.race([
                sock.sendMessage(jid, content, options),
                new Promise((_, reject) => setTimeout(() => reject(new Error('Send timeout')), 15000).unref())
            ]);
            metrics.outgoingMessages++;
            return result;
        } catch (err) {
            if (reconnectErrors.some(e => err?.message?.includes(e)) && !socketReady) {
                waCircuitOpenUntil = Date.now() + 30000;
                scheduleReconnect();
            }
            if (i === retries) { metrics.failedMessages++; throw err; }
            // تأخير أسي مع تشويش
            const delay = Math.min(1000 * 2 ** i, 10000) + Math.random() * 500;
            await new Promise(r => setTimeout(r, delay).unref());
        }
    }
}

// ═══════════════════════════════════════════════════
// 🧠 مراقبة الأداء وتنظيف الموارد
// ═══════════════════════════════════════════════════
setSafeInterval(() => {
    const now = Date.now();
    for (const [k, v] of spamTrackers) { if (now - v.lastReset > 60000) spamTrackers.delete(k); }
    for (const [k, v] of imageCooldown) { if (now - v > 5 * 60000) imageCooldown.delete(k); }
    for (const [k, v] of commandCooldowns) { if (now - v > 60000) commandCooldowns.delete(k); }
}, 60000);

const eventLoopMonitor = monitorEventLoopDelay({ resolution: 20 });
eventLoopMonitor.enable();
setSafeInterval(() => {
    const lag = eventLoopMonitor.mean / 1e6;
    const p99 = eventLoopMonitor.percentile(99) / 1e6;
    const max = eventLoopMonitor.max / 1e6;
    if (lag > 5000 || p99 > 10000 || max > 15000) appLogger.error({ lag, p99, max }, 'High event loop lag');
    eventLoopMonitor.reset();
}, 10000);

setSafeInterval(() => {
    if (queue.pending > 0 && Date.now() - lastQueueProgress > 60000) appLogger.error('Queue stalled');
    if (broadcastQueue.pending > 0 && Date.now() - lastQueueProgress > 60000) appLogger.error('Broadcast queue stalled');
}, 30000);

setSafeInterval(() => {
    const mem = process.memoryUsage();
    const heapUsed = Math.round(mem.heapUsed / 1024 / 1024);
    appLogger.info({ rss: Math.round(mem.rss / 1024 / 1024), heapUsed }, 'Memory');
    if (heapUsed > 800) { appLogger.error('💥 High heap'); emergencyShutdown(1); }
}, 60000);

// ═══════════════════════════════════════════════════
// 🤖 بدء البوت (V5.4.2 – الإصدار الآمن والمرن)
// ═══════════════════════════════════════════════════
let reconnectState = 0;
let reconnectDelay = 5000, reconnectAttempts = 0, reconnectTimer = null;
let reconnectInProgress = false;
let sock, socketReady = false, lastSocketActivity = Date.now();
const reconnectErrors = ['Connection Closed', 'Timed Out', 'stream errored'];
let lastBroadcastTime = 0;
let startingSocket = null;
let bootFailures = 0, lastBootFailureTime = 0, bootCooldownTimer = null;

function handleBootFailure() {
    const now = Date.now();
    if (now - lastBootFailureTime > 60000) bootFailures = 0;
    bootFailures++; lastBootFailureTime = now;
    if (bootFailures > 5 && !bootCooldownTimer) {
        appLogger.error('💥 كثرة فشل التشغيل، إيقاف لمدة 5 دقائق');
        bootCooldownTimer = setSafeTimeout(() => { bootCooldownTimer = null; bootFailures = 0; startBot().catch(() => {}); }, 5 * 60 * 1000);
        return true;
    }
    return false;
}

function scheduleReconnect() {
    if (reconnectState === 1 || reconnectState === 3 || reconnectState === -1 || startingSocket) return;
    if (reconnectTimer) return;
    if (reconnectInProgress) return;
    reconnectInProgress = true;
    reconnectState = 3;
    reconnectTimer = setSafeTimeout(async () => {
        reconnectTimer = null;
        await performReconnect();
        reconnectInProgress = false;
    }, reconnectDelay);
}

async function performReconnect() {
    try {
        reconnectAttempts++;
        await Promise.race([startBot(), new Promise((_, reject) => setTimeout(() => reject(new Error('Start timeout')), 30000).unref())]);
        reconnectDelay = 5000; reconnectAttempts = 0;
    } catch (err) {
        appLogger.error(err, `❌ فشل إعادة الاتصال (محاولة ${reconnectAttempts})`);
        if (reconnectAttempts > 20) { appLogger.error('💥 تجاوز الحد الأقصى'); emergencyShutdown(1); }
        reconnectDelay = Math.min(reconnectDelay * 2, 60000);
    } finally {
        reconnectState = 0;
        if (!socketReady && reconnectAttempts <= 20) {
            reconnectTimer = setSafeTimeout(async () => {
                reconnectTimer = null;
                await performReconnect();
            }, reconnectDelay);
        }
    }
}

setSafeInterval(async () => {
    if (!sock?.user || socketDraining) return;
    if (Date.now() - lastSocketActivity > 120000) {
        try { await sock.sendPresenceUpdate('available'); lastSocketActivity = Date.now(); } catch { scheduleReconnect(); }
    }
}, 60000);

let credsSaveTimeout = null;
async function debouncedSaveCreds(creds) {
    pendingCreds = {
        ...creds,
        noiseKey: creds.noiseKey ? Buffer.from(creds.noiseKey) : undefined
    };
    if (credsSaveTimeout) return;
    credsSaveTimeout = setSafeTimeout(async () => {
        credsSaveTimeout = null;
        if (pendingCreds) await flushCreds();
    }, 5000);
}

async function getGroupMeta(jid) {
    const cached = groupMetaCache.get(jid);
    if (cached) return cached;
    if (!sock?.user) throw new Error('Socket unavailable');
    try {
        const meta = await sock.groupMetadata(jid);
        groupMetaCache.set(jid, meta);
        return meta;
    } catch (err) {
        if (cached) return cached;
        throw err;
    }
}

function addWaitingBroadcast(sender) {
    waitingBroadcast.add(sender);
    setSafeTimeout(() => {
        waitingBroadcast.delete(sender);
    }, 120000);
}

function normalizeJid(jid) {
    return jid?.split(':')[0];
}

async function startBot() {
    if (startingSocket || reconnectState === -1) return;
    startingSocket = (async () => {
        try {
            await ensureDb();
            const credsDoc = await db.collection('creds').findOne({ _id: 'creds' }); let creds = credsDoc?.value;
            if (!creds) { creds = initAuthCreds(); appLogger.info('🆕 جلسة جديدة.'); }
            const { version } = await fetchLatestBaileysVersion();
            if (sock) {
                try {
                    sock.ev.removeAllListeners();
                    sock.ws?.close();
                    sock.end?.();
                } catch {}
                sock = null;
            }
            sock = makeWASocket({ version, auth: { creds, keys: createKeyStore() }, printQRInTerminal: false, logger: silentLogger, browser: ['DOMA BOT PRO', 'Chrome', '10.0.0'], markOnlineOnConnect: false, syncFullHistory: false });

            sock.ev.on('creds.update', async (newCreds) => {
                Object.assign(creds, newCreds);
                pendingCreds = {
                    ...creds,
                    noiseKey: creds.noiseKey ? Buffer.from(creds.noiseKey) : undefined
                };
                if (newCreds?.registered || newCreds?.me) await flushCreds();
                else debouncedSaveCreds(creds);
                lastSocketActivity = Date.now();
            });

            sock.ev.on('connection.update', async (update) => {
                const { connection, lastDisconnect, qr } = update;
                if (qr) { appLogger.info('📱 امسح QR:'); try { appLogger.info(await QRCode.toString(qr, { type: 'terminal', small: true })); } catch {} }
                if (connection === 'open') {
                    reconnectState = 2; socketReady = true; reconnectInProgress = false;
                    if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
                    appLogger.info(`✅ ${BOT_NAME} جاهز!`);
                }
                if (connection === 'close') {
                    reconnectState = 0; socketReady = false;
                    if (lastDisconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut) { appLogger.info('🔄 إعادة اتصال...'); scheduleReconnect(); }
                    else { appLogger.info('❌ تسجيل خروج. تنظيف البيانات...'); await db.collection('creds').deleteMany({}); await db.collection('keys').deleteMany({}); await emergencyShutdown(1); }
                }
                lastSocketActivity = Date.now();
            });

            sock.ev.on('groups.update', updates => { for (const u of updates) groupMetaCache.delete(u.id); });
            sock.ev.on('group-participants.update', update => { groupMetaCache.delete(update.id); });

            // 👥 الترحيب
            sock.ev.on('group-participants.update', async (update) => {
                const { id, participants, action } = update; if (action !== 'add') return;
                const group = await getGroup(id); if (group && !group.welcome) return;
                for (const participant of participants) {
                    if (participant === sock.user?.id) continue;
                    const memberNumber = String(participant).split('@')[0];
                    const welcomeMsg = WELCOME_MESSAGES[Math.floor(Math.random() * WELCOME_MESSAGES.length)].replace('{name}', '@' + memberNumber);
                    try {
                        const pp = await sock.profilePictureUrl(participant, 'image');
                        void sendMessage(id, { image: { url: pp }, caption: welcomeMsg, mentions: [String(participant)] }).catch(e => appLogger.warn(e));
                    } catch { void sendMessage(id, { text: welcomeMsg, mentions: [String(participant)] }).catch(e => appLogger.warn(e)); }
                    queueUserUpdate(memberNumber, { name: memberNumber });
                }
            });

            // 📩 معالجة الرسائل (مع نسخ صغيرة للذاكرة)
            sock.ev.on('messages.upsert', async ({ messages }) => {
                const totalPressure = queue.size + queue.pending + broadcastQueue.size + broadcastQueue.pending + mediaQueue.size + mediaQueue.pending;
                if (totalPressure >= MAX_QUEUE_SIZE) return;
                if (messages.length > 100) { appLogger.warn('Large batch dropped'); return; }

                for (const msg of messages) {
                    try {
                        if (!msg?.message || msg.key.fromMe) continue;
                        lastSocketActivity = Date.now(); metrics.incomingMessages++;

                        if (checkGlobalFlood()) { appLogger.warn('⚠️ Global flood'); continue; }
                        if (processedMessages.has(msg.key.id)) continue;
                        processedMessages.set(msg.key.id, true);

                        const sender = msg.key.remoteJid; const senderNumber = getSenderNumber(msg); const isGroup = sender.endsWith('@g.us');
                        const pushName = msg.pushName || 'مستخدم'; const adminCheck = isAdmin(senderNumber);
                        let text = ''; if (msg.message.conversation) text = msg.message.conversation; else if (msg.message.extendedTextMessage) text = msg.message.extendedTextMessage.text || ''; else if (msg.message.imageMessage) text = msg.message.imageMessage.caption || '';
                        if (typeof text !== 'string' || !senderNumber) continue;
                        if (text.length > MAX_TEXT_LENGTH) continue;
                        if (await isBanned(senderNumber)) continue;

                        const now = Date.now();
                        let tracker = spamTrackers.get(senderNumber);
                        if (!tracker || now - tracker.lastReset > 10000) { tracker = { count: 0, lastReset: now }; spamTrackers.set(senderNumber, tracker); }
                        tracker.count++;

                        if (totalPressure >= MAX_QUEUE_SIZE) {
                            if (!isGroup) await safeSend(sender, { text: '⚠️ البوت مشغول حالياً، حاول لاحقاً' }).catch(() => {});
                            continue;
                        }

                        const user = await getUser(senderNumber);
                        queueUserUpdate(senderNumber, { name: pushName, messageCount: (user?.messageCount || 0) + 1, lastActive: new Date().toISOString() });
                        appLogger.info(`[${senderNumber}]: ${text.substring(0, 60)}`);

                        // ✅ نظام البث (waitingBroadcast)
                        if (adminCheck && waitingBroadcast.has(senderNumber)) {
                            waitingBroadcast.delete(senderNumber);
                            lastBroadcastTime = Date.now();
                            const broadcastMsg = text;
                            void enqueue(async () => {
                                try {
                                    const variants = [`📢 ${broadcastMsg}`, `📣 ${broadcastMsg}`, `🔥 ${broadcastMsg}`];
                                    const cursor = db.collection('users').find().project({ _id: 1 });
                                    let success = 0, failed = 0, total = 0;
                                    let batch = [];
                                    for await (const user of cursor) {
                                        if (total >= MAX_BROADCAST_USERS) break;
                                        if (isValidJid(user._id)) {
                                            batch.push(user);
                                            total++;
                                            if (batch.length >= BROADCAST_BATCH_SIZE) {
                                                await Promise.allSettled(batch.map(u =>
                                                    sendMessage(`${u._id}@s.whatsapp.net`, { text: variants[Math.floor(Math.random() * variants.length)] }).then(() => success++).catch(() => failed++)
                                                ));
                                                batch = [];
                                                await new Promise(r => setTimeout(r, BROADCAST_BATCH_DELAY).unref());
                                                if (total % 100 === 0) void sendMessage(sender, { text: `📊 تقدم النشر: ${total} مستخدم...` });
                                            }
                                        }
                                    }
                                    if (batch.length > 0) {
                                        await Promise.allSettled(batch.map(u =>
                                            sendMessage(`${u._id}@s.whatsapp.net`, { text: variants[Math.floor(Math.random() * variants.length)] }).then(() => success++).catch(() => failed++)
                                        ));
                                    }
                                    await sendMessage(sender, { text: `✅ اكتمل النشر إلى ${total}.\n✔️ نجاح: ${success}\n❌ فشل: ${failed}` });
                                } catch (err) { appLogger.error(err, 'Broadcast failed'); void sendMessage(sender, { text: '❌ فشل النشر.' }); }
                            }, broadcastQueue);
                            return;
                        }

                        // 👑 أوامر الأدمن
                        if (adminCheck) {
                            const nowCmd = Date.now();
                            const heavyCommands = ['نشر خبر', 'تقرير', 'سجل الصور', 'الأرقام', 'الجروبات'];
                            if (heavyCommands.includes(text)) {
                                const lastUsed = commandCooldowns.get(senderNumber) || 0;
                                if (nowCmd - lastUsed < 10000) { await sendMessage(sender, { text: '⏳ انتظر 10 ثواني' }); return; }
                                commandCooldowns.set(senderNumber, nowCmd);
                            }

                            if (text === 'نشر خبر') {
                                if (Date.now() - lastBroadcastTime < 5 * 60 * 1000) { await sendMessage(sender, { text: '⏳ انتظر 5 دقائق' }); return; }
                                addWaitingBroadcast(senderNumber);
                                await sendMessage(sender, { text: '📢 أرسل رسالة النشر الآن' });
                                return;
                            }
                            if (text === 'الأرقام') {
                                const users = await db.collection('users').find().project({ _id: 1 }).toArray();
                                const list = users.length ? users.map((u,i)=>`${i+1}. +${u._id}`).join('\n') : 'لا توجد';
                                await sendMessage(sender, { text: `📞 الأرقام:\n${list}` }); return;
                            }
                            if (text === 'الجروبات') {
                                const groups = await db.collection('groups').find().project({ _id: 1, name: 1 }).toArray();
                                const list = groups.length ? groups.map((g,i)=>`${i+1}. ${g.name||'جروب'}`).join('\n') : 'لا توجد';
                                await sendMessage(sender, { text: `👥 الجروبات:\n${list}` }); return;
                            }
                            if (text.startsWith('حظر ')) {
                                const target = text.replace(/[^0-9]/g,'');
                                if (!target) return;
                                if (target === BOT_NUMBER || target === OWNER_NUMBER || ADMIN_NUMBERS.includes(target)) {
                                    await sendMessage(sender, { text: '⛔ لا يمكن حظر هذا الرقم.' });
                                    return;
                                }
                                if (!(await isBanned(target))) {
                                    await banUser(target);
                                    await sendMessage(sender, { text: `🚫 تم حظر +${target}` });
                                } else await sendMessage(sender, { text: '⚠️ الرقم محظور بالفعل' });
                                return;
                            }
                            if (text.startsWith('فك ')) {
                                const target = text.replace(/[^0-9]/g,'');
                                if (!target) return;
                                await unbanUser(target);
                                await sendMessage(sender, { text: `✅ تم فك حظر +${target}` });
                                return;
                            }
                            if (text === 'سجل الصور') {
                                const log = await getImageLog(10);
                                const report = log.length ? log.map((e,i)=>`${i+1}. ${e.user} (${e.number})`).join('\n') : 'لا توجد';
                                await sendMessage(sender, { text: `📸 آخر الصور:\n${report}` }); return;
                            }
                            if (text === 'تقرير') {
                                const warns = await db.collection('warnings').find().toArray();
                                const report = warns.length ? warns.map(w=>`- ${w._id}: ${w.count}`).join('\n') : 'لا توجد';
                                await sendMessage(sender, { text: `📊 تقرير التحذيرات:\n${report}` }); return;
                            }
                            if (text === 'تفعيل الدفع') { await setSettings({ paidMode: true }); await sendMessage(sender, { text: '💰 تم تفعيل وضع الدفع' }); return; }
                            if (text === 'تعطيل الدفع') { await setSettings({ paidMode: false }); await sendMessage(sender, { text: '💸 تم تعطيل وضع الدفع' }); return; }
                            if (text === 'إيقاف البوت') {
                                if (!isOwner(senderNumber)) { await sendMessage(sender, { text: '⛔ المالك فقط يمكنه إيقاف البوت.' }); return; }
                                await sendMessage(sender, { text: '⏹️ جاري الإيقاف...' }); await emergencyShutdown(0);
                            }
                        }

                        // 📚 لوحة الأعضاء
                        if (text === 'المكتبة' || text === '.menu' || text === 'اوامر') {
                            const cap = `╭━━〔 *✨ الأوامــر* 〕━━╮\n┃ 🤖 ${BOT_NAME}\n┃ ⊛ صورة - تعديل الصور\n┃ ⊛ رقمي - معلوماتك\n┃ ⊛ المطور - مطور البوت\n╰━━━━━━━━━━━━━━╯`;
                            try { await sendMessage(sender, { image: { url: MENU_IMAGE_URL }, caption: cap }); } catch { await sendMessage(sender, { text: cap }); }
                            return;
                        }

                        // 📸 تعديل الصور (مع timeout تنزيل وتصحيح دوران الصورة)
                        if (text === 'صورة') { await sendMessage(sender, { text: '📸 أرسل الصورة مع "تعديل"' }); return; }
                        if (text === 'تعديل' && msg.message.imageMessage) {
                            const size = msg.message.imageMessage?.fileLength || 0;
                            if (!size || size <= 0 || size > MAX_MEDIA_SIZE) { await sendMessage(sender, { text: '❌ الصورة غير صالحة.' }); return; }
                            const lastImage = imageCooldown.get(senderNumber) || 0;
                            if (Date.now() - lastImage < 15000) { await sendMessage(sender, { text: '⏳ انتظر.' }); return; }
                            imageCooldown.set(senderNumber, Date.now());
                            await sendMessage(sender, { text: '⏳ جاري التحسين...' });
                            try {
                                const media = await Promise.race([
                                    downloadMediaMessage(msg, 'buffer', {}, { logger: silentLogger, reuploadRequest: sock.updateMediaMessage.bind(sock) }),
                                    new Promise((_, reject) => setTimeout(() => reject(new Error('Media download timeout')), 20000).unref())
                                ]);
                                if (media.length > MAX_MEDIA_SIZE) throw new Error('Media too large');
                                // ✅ إضافة rotate() لتصحيح الاتجاه تلقائياً قبل resize
                                const enhanced = await sharp(media, { limitInputPixels: 12000000, failOn: 'truncated' })
                                    .rotate() // <-- تصحيح EXIF orientation
                                    .resize({ width: 800, withoutEnlargement: true })
                                    .jpeg({ quality: 90 })
                                    .sharpen()
                                    .modulate({ brightness: 1.1, saturation: 1.2 })
                                    .toBuffer();
                                await sendMessage(sender, { image: enhanced, caption: '✨ تم تحسين الصورة!' });
                                await logImage(pushName, senderNumber);
                            } catch { await sendMessage(sender, { text: '❌ فشل المعالجة.' }); }
                            return;
                        }

                        if (text === 'رقمي' || text === '.myid') { await sendMessage(sender, { text: `📱 رقمك: +${senderNumber}` }); return; }
                        if (text === 'المطور' || text === '.dev') { await sendMessage(sender, { text: `👑 المطور: ${OWNER_NAME}\n📱 البوت: +${BOT_NUMBER}` }); return; }

                        // 🛡️ حماية الجروبات (مع تطبيع JID)
                        if (isGroup) {
                            const groupId = sender;
                            let group = await getGroup(groupId);
                            if (!group) {
                                try {
                                    const meta = await getGroupMeta(groupId);
                                    group = { name: meta.subject, welcome: true, links: true, badwords: true, maxWarn: 3, enabled: true };
                                    await updateGroup(groupId, group);
                                } catch { continue; }
                            }
                            if (group.enabled === false && !adminCheck) continue;

                            let meta = null;
                            async function getMeta() {
                                if (!meta) meta = await getGroupMeta(groupId);
                                return meta;
                            }

                            const participantId = normalizeJid(msg.key.participant || sender);
                            const isGroupAdmin = (await getMeta())?.participants?.some(p => normalizeJid(p.id) === participantId && p.admin);
                            const botIsAdmin = (await getMeta())?.participants?.some(p => normalizeJid(p.id) === normalizeJid(sock.user?.id) && p.admin);

                            if (group.links && linkRegex.test(text)) {
                                if (isGroupAdmin || adminCheck) continue;
                                if (botIsAdmin) { await sock.sendMessage(groupId, { delete: { remoteJid: groupId, fromMe: false, id: msg.key.id, participant: msg.key.participant || sender } }).catch(() => {}); }
                                const count = await getWarnings(senderNumber) + 1; await setWarnings(senderNumber, count);
                                if (count >= (group.maxWarn || 3)) {
                                    if (participantId && participantId.includes('@')) await sock.groupParticipantsUpdate(groupId, [msg.key.participant || sender], 'remove').catch(() => {});
                                    await sendMessage(groupId, { text: '🚫 طرد (روابط)', mentions: [String(msg.key.participant || sender)] });
                                } else await sendMessage(groupId, { text: `⚠️ إنذار ${count}/${group.maxWarn}`, mentions: [String(msg.key.participant || sender)] });
                                continue;
                            }

                            if (group.badwords && badwordsRegex) {
                                const cleaned = cleanTextForBadwords(text);
                                if (badwordsRegex.test(cleaned)) {
                                    if (isGroupAdmin || adminCheck) continue;
                                    if (botIsAdmin) { await sock.sendMessage(groupId, { delete: { remoteJid: groupId, fromMe: false, id: msg.key.id, participant: msg.key.participant || sender } }).catch(() => {}); }
                                    const count = await getWarnings(senderNumber) + 1; await setWarnings(senderNumber, count);
                                    if (count >= (group.maxWarn || 3)) {
                                        if (participantId && participantId.includes('@')) await sock.groupParticipantsUpdate(groupId, [msg.key.participant || sender], 'remove').catch(() => {});
                                        await sendMessage(groupId, { text: '🚫 طرد (شتائم)', mentions: [String(msg.key.participant || sender)] });
                                    } else await sendMessage(groupId, { text: `⚠️ إنذار ${count}/${group.maxWarn}`, mentions: [String(msg.key.participant || sender)] });
                                    continue;
                                }
                            }

                            if (['ترحيب', 'روابط', 'شتائم'].includes(text) || text.startsWith('طرد')) {
                                if (!isGroupAdmin && !adminCheck) { await sendMessage(groupId, { text: '⛔ للأدمن فقط.' }); continue; }
                                if (text === 'ترحيب') { group.welcome = !group.welcome; await updateGroup(groupId, { welcome: group.welcome }); await sendMessage(groupId, { text: `الترحيب ${group.welcome?'✅':'❌'}` }); }
                                else if (text === 'روابط') { group.links = !group.links; await updateGroup(groupId, { links: group.links }); await sendMessage(groupId, { text: `الروابط ${group.links?'✅':'❌'}` }); }
                                else if (text === 'شتائم') { group.badwords = !group.badwords; await updateGroup(groupId, { badwords: group.badwords }); await sendMessage(groupId, { text: `الشتائم ${group.badwords?'✅':'❌'}` }); }
                                else if (text.startsWith('طرد')) {
                                    const mentioned = msg.message?.extendedTextMessage?.contextInfo?.mentionedJid;
                                    if (mentioned?.length) { await sock.groupParticipantsUpdate(groupId, mentioned, 'remove').catch(()=>{}); await sendMessage(groupId, { text: '✅ تم الطرد', mentions: mentioned.map(String) }); }
                                    else await sendMessage(groupId, { text: '⚠️ اعمل منشن للعضو' });
                                }
                                continue;
                            }
                        }

                        // 🧠 الردود الذكية
                        for (const [keyword, replies] of Object.entries(smartReplies)) {
                            if (text.includes(keyword)) { await sendMessage(sender, { text: replies[Math.floor(Math.random() * replies.length)] }); return; }
                        }
                        if (!isGroup && text.length > 3) { await sendMessage(sender, { text: `🤖 اكتب "اوامر" لعرض القائمة` }); }

                    } catch (err) { appLogger.error(err, '⚠️ خطأ في معالجة الرسالة'); metrics.failedMessages++; }
                }
            });
        } finally { startingSocket = null; }
    })();
    return startingSocket;
}

// ═══════════════════════════════════════════════════
// 🌐 خادم الويب (مع تخزين مؤقت للصحة - سلبي)
// ═══════════════════════════════════════════════════
const app = express();
app.set('trust proxy', 1); app.use(helmet()); app.use(compression()); app.use(rateLimit({ windowMs: 60 * 1000, max: 100 }));
app.get('/live', (req, res) => res.send('OK'));

let lastSuccessfulDbPing = 0;
let lastHealthCheck = 0;
let cachedHealth = { status: 'degraded', mongoLatency: 0 };
let healthCheckPromise = null;

app.get('/health', async (req, res) => {
    const now = Date.now();
    if (now - lastHealthCheck < 10000) {
        return res.json(cachedHealth);
    }
    if (!healthCheckPromise) {
        healthCheckPromise = (async () => {
            try {
                const mongoAlive = (now - lastSuccessfulDbPing) < 60000;
                if (!mongoAlive) {
                    try {
                        const start = Date.now();
                        await ensureDb(); await db.command({ ping: 1 });
                        lastSuccessfulDbPing = Date.now();
                    } catch {
                        lastSuccessfulDbPing = 0;
                    }
                }
                const mem = process.memoryUsage();
                const heapMB = Math.round(mem.heapUsed / 1024 / 1024);
                const lag = eventLoopMonitor.mean / 1e6;
                const p99 = eventLoopMonitor.percentile(99) / 1e6;
                let status = 'ok';
                if (!socketReady || reconnectState !== 2) status = 'error';
                else if (heapMB > 700 || p99 > 10000 || !mongoAlive) status = 'degraded';
                cachedHealth = {
                    status,
                    memory: { rss: Math.round(mem.rss / 1024 / 1024), heap: heapMB },
                    eventLoopLag: Math.round(lag),
                    eventLoopP99: Math.round(p99),
                    mongoLatency: 0,
                    reconnectAttempts,
                    queue: { size: queue.size, pending: queue.pending },
                    uptime: process.uptime()
                };
                lastHealthCheck = now;
                return cachedHealth;
            } catch {
                return { status: 'error' };
            } finally {
                healthCheckPromise = null;
            }
        })();
    }
    const result = await healthCheckPromise;
    res.status(result.status === 'ok' ? 200 : 503).json(result);
});

// تحديث lastSuccessfulDbPing في الخلفية كل 30 ثانية
setSafeInterval(async () => {
    try {
        await ensureDb();
        await db.command({ ping: 1 });
        lastSuccessfulDbPing = Date.now();
    } catch {}
}, 30000);

app.get('/metrics', (req, res) => {
    res.set('Content-Type', 'text/plain');
    res.send(`# HELP doma_bot_messages_total Total messages processed.
# TYPE doma_bot_messages_total counter
doma_bot_messages_total{type="incoming"} ${metrics.incomingMessages}
doma_bot_messages_total{type="outgoing"} ${metrics.outgoingMessages}
doma_bot_messages_total{type="failed"} ${metrics.failedMessages}
# HELP doma_bot_images_total Total images processed.
# TYPE doma_bot_images_total counter
doma_bot_images_total ${metrics.imagesProcessed}
# HELP doma_bot_spam_blocked_total Total spam blocked.
# TYPE doma_bot_spam_blocked_total counter
doma_bot_spam_blocked_total ${metrics.spamBlocked}
# HELP doma_bot_queue_size Current queue size.
# TYPE doma_bot_queue_size gauge
doma_bot_queue_size{queue="main"} ${queue.size}
doma_bot_queue_size{queue="broadcast"} ${broadcastQueue.size}
doma_bot_queue_size{queue="media"} ${mediaQueue.size}
# HELP doma_bot_uptime_seconds Process uptime in seconds.
# TYPE doma_bot_uptime_seconds gauge
doma_bot_uptime_seconds ${process.uptime()}
`);
});
app.get('/', (req, res) => res.send('DOMA BOT PRO V5.4.2 is running...'));
httpServer = app.listen(process.env.PORT || 3000, () => appLogger.info('🌐 Web server on'));
httpServer.requestTimeout = 30000;

startBot().catch(err => { appLogger.error(err, '❌ خطأ في بدء البوت'); if (handleBootFailure()) return; process.exit(1); });
