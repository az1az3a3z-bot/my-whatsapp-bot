// ╔══════════════════════════════════════════════════╗
// ║  🤖 DOMA BOT PRO V5.4.2 - No Database Edition  ║
// ║  👑 المطور: عبد الرحمن                          ║
// ║  ⚡ Enterprise Messaging Service - JSON Storage ║
// ╚══════════════════════════════════════════════════╝

const os = require('os');
process.env.UV_THREADPOOL_SIZE = String(Math.min(os.cpus().length * 2, 16));

const express = require('express');
const { LRUCache } = require('lru-cache');
const {
    default: makeWASocket,
    DisconnectReason,
    fetchLatestBaileysVersion,
    makeCacheableSignalKeyStore,
    initAuthCreds,
    downloadMediaMessage
} = require('@whiskeysockets/baileys');
const { default: PQueue } = require('p-queue');
const pino = require('pino');
const QRCode = require('qrcode');
const sharp = require('sharp');
const fs = require('fs-extra');
const path = require('path');

// ═══════════════════════════════════════════════════
// 🔧 إعدادات
// ═══════════════════════════════════════════════════
sharp.cache(false);
sharp.concurrency(2);

const appLogger = pino({
    level: process.env.LOG_LEVEL || 'info',
    transport: process.env.NODE_ENV !== 'production' ? {
        target: 'pino-pretty',
        options: { colorize: true }
    } : undefined
});
const silentLogger = pino({ level: 'silent' });

// ═══════════════════════════════════════════════════
// 🗄️ قاعدة بيانات JSON محلية
// ═══════════════════════════════════════════════════
const DATA_DIR = path.join(__dirname, 'data');
const DB = {
    creds: null,
    keys: new Map(),
    users: new Map(),
    groups: new Map(),
    warnings: new Map(),
    banned: new Set(),
    settings: { paidMode: false },
    imageLog: []
};

async function loadData() {
    await fs.ensureDir(DATA_DIR);
    const files = ['creds.json', 'users.json', 'groups.json', 'warnings.json', 'banned.json', 'settings.json', 'imageLog.json'];
    for (const f of files) {
        const filePath = path.join(DATA_DIR, f);
        if (await fs.pathExists(filePath)) {
            const data = JSON.parse(await fs.readFile(filePath, 'utf-8'));
            if (f === 'creds.json') DB.creds = data;
            else if (f === 'users.json') DB.users = new Map(Object.entries(data));
            else if (f === 'groups.json') DB.groups = new Map(Object.entries(data));
            else if (f === 'warnings.json') DB.warnings = new Map(Object.entries(data));
            else if (f === 'banned.json') DB.banned = new Set(data);
            else if (f === 'settings.json') DB.settings = data;
            else if (f === 'imageLog.json') DB.imageLog = data;
        }
    }
}

async function saveAllData() {
    await fs.ensureDir(DATA_DIR);
    await fs.writeFile(path.join(DATA_DIR, 'creds.json'), JSON.stringify(DB.creds || {}));
    await fs.writeFile(path.join(DATA_DIR, 'users.json'), JSON.stringify(Object.fromEntries(DB.users)));
    await fs.writeFile(path.join(DATA_DIR, 'groups.json'), JSON.stringify(Object.fromEntries(DB.groups)));
    await fs.writeFile(path.join(DATA_DIR, 'warnings.json'), JSON.stringify(Object.fromEntries(DB.warnings)));
    await fs.writeFile(path.join(DATA_DIR, 'banned.json'), JSON.stringify([...DB.banned]));
    await fs.writeFile(path.join(DATA_DIR, 'settings.json'), JSON.stringify(DB.settings));
    await fs.writeFile(path.join(DATA_DIR, 'imageLog.json'), JSON.stringify(DB.imageLog.slice(-50)));
}
setInterval(saveAllData, 30000);

// CRUD
async function getUser(number) { return DB.users.get(number) || null; }
function queueUserUpdate(number, data) { DB.users.set(number, { ...(DB.users.get(number) || {}), ...data }); }
async function getGroup(jid) { return DB.groups.get(jid) || null; }
async function updateGroup(jid, data) { DB.groups.set(jid, { ...(DB.groups.get(jid) || {}), ...data }); }
async function getWarnings(number) { return DB.warnings.get(number) || 0; }
async function setWarnings(number, count) { DB.warnings.set(number, count); }
async function isBanned(number) { return DB.banned.has(number); }
async function banUser(number) { DB.banned.add(number); }
async function unbanUser(number) { DB.banned.delete(number); }
async function logImage(user, number) { DB.imageLog.push({ user, number, time: new Date().toISOString() }); if (DB.imageLog.length > 100) DB.imageLog = DB.imageLog.slice(-50); }
async function getImageLog(limit = 10) { return DB.imageLog.slice(-limit).reverse(); }
async function getSettings() { return DB.settings; }
async function setSettings(data) { DB.settings = { ...DB.settings, ...data }; }

function createKeyStore() {
    return makeCacheableSignalKeyStore({
        get: async (type, id) => DB.keys.get(`${type}-${id}`),
        set: async (data) => {
            for (const [type, ids] of Object.entries(data)) {
                for (const [id, value] of Object.entries(ids)) {
                    if (value) DB.keys.set(`${type}-${id}`, value);
                    else DB.keys.delete(`${type}-${id}`);
                }
            }
        },
        clear: async () => DB.keys.clear()
    }, silentLogger);
}

// ═══════════════════════════════════════════════════
// ⚙️ الثوابت والإعدادات
// ═══════════════════════════════════════════════════
const OWNER_NUMBER = "584164041083";
const BOT_NUMBER = "967700421534";
const ADMIN_NUMBERS = ["584164041083"];
const OWNER_NAME = "عبد الرحمن";
const BOT_NAME = "DOMA BOT PRO";
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

const processedMessages = new LRUCache({ max: 5000, ttl: 1000 * 60 * 10 });
const groupMetaCache = new LRUCache({ max: 500, ttl: 1000 * 60 * 5 });
const jidQueues = new Map();
const imageCooldown = new Map();
const waitingBroadcast = new Set();
const commandCooldowns = new Map();
const metrics = { incomingMessages: 0, outgoingMessages: 0, failedMessages: 0, imagesProcessed: 0, spamBlocked: 0 };

const GLOBAL_RATE = { capacity: 20, tokens: 20, refillRate: 20, lastRefill: performance.now() };
function consumeGlobalToken() {
    const now = performance.now();
    GLOBAL_RATE.tokens = Math.min(GLOBAL_RATE.capacity, GLOBAL_RATE.tokens + (now - GLOBAL_RATE.lastRefill) / 1000 * GLOBAL_RATE.refillRate);
    GLOBAL_RATE.lastRefill = now;
    if (GLOBAL_RATE.tokens < 1) return false;
    GLOBAL_RATE.tokens -= 1;
    return true;
}

const FLOOD_BUFFER_SIZE = 2048;
const floodBuffer = new Uint32Array(FLOOD_BUFFER_SIZE);
let floodIndex = 0, floodCount = 0;
function checkGlobalFlood() {
    const now = Date.now();
    const currentSlot = Math.floor(now / FLOOD_WINDOW_MS);
    while (floodCount > 0 && Math.floor(Date.now() / FLOOD_WINDOW_MS) - currentSlot > FLOOD_BUFFER_SIZE) {
        floodBuffer[(floodIndex - floodCount + FLOOD_BUFFER_SIZE) % FLOOD_BUFFER_SIZE] = 0;
        floodCount--;
    }
    const targetSlot = currentSlot % FLOOD_BUFFER_SIZE;
    if (floodBuffer[targetSlot] < MAX_FLOOD_PER_WINDOW) { floodBuffer[targetSlot]++; floodCount++; floodIndex = (targetSlot + 1) % FLOOD_BUFFER_SIZE; return false; }
    return true;
}

let socketDraining = false;
const queue = new PQueue({ interval: 1000, intervalCap: 6, concurrency: 3, timeout: 30000, throwOnTimeout: true });
const broadcastQueue = new PQueue({ interval: 1000, intervalCap: 20, concurrency: 10, timeout: 30000, throwOnTimeout: true });
const mediaQueue = new PQueue({ concurrency: 1, timeout: 30000, throwOnTimeout: true });

function getJidQueue(jid) {
    if (!jidQueues.has(jid)) {
        const q = new PQueue({ concurrency: 1, interval: SEND_INTERVAL, intervalCap: 1, timeout: 30000, throwOnTimeout: true });
        q.on('idle', () => jidQueues.delete(jid));
        jidQueues.set(jid, q);
    }
    return jidQueues.get(jid);
}

function sendMessage(jid, content, options = {}) {
    if (!consumeGlobalToken()) return Promise.reject(new Error('Rate limit'));
    if (socketDraining) return Promise.reject(new Error('Draining'));
    return getJidQueue(jid).add(() => safeSend(jid, content, options));
}

async function safeSend(jid, content, options = {}, retries = 2) {
    if (!sock?.user || socketDraining) throw new Error('Socket down');
    for (let i = 0; i <= retries; i++) {
        try {
            const result = await sock.sendMessage(jid, content, options);
            metrics.outgoingMessages++;
            return result;
        } catch (err) {
            if (i === retries) { metrics.failedMessages++; throw err; }
            await new Promise(r => setTimeout(r, 1000 * (i + 1)));
        }
    }
}

// ═══════════════════════════════════════════════════
// 🧠 الذكاء والردود
// ═══════════════════════════════════════════════════
const BADWORDS = ['كس', 'طيز', 'منيوك', 'متناك', 'شرموط', 'قحبه', 'زب', 'بزاز', 'fuck', 'shit', 'bitch'];
const badwordsRegex = new RegExp(`\\b(${BADWORDS.join('|')})\\b`, 'i');
const linkRegex = /https?:\/\/\S+|www\.\S+|chat\.whatsapp\.com\/\S+/i;

const smartReplies = {
    'السلام': ['وعليكم السلام!', 'وعليكم السلام ورحمة الله!'],
    'هلا': ['هلا والله!', 'هلا بيك!'],
    'صباح': ['صباح النور!', 'صباح الخير!'],
    'مساء': ['مساء النور!', 'مساء الخير!'],
    'شكرا': ['العفو!', 'لا شكر على واجب!'],
    'باي': ['مع السلامة!', 'الله يحفظك!']
};
const WELCOME_MESSAGES = ["🌟 نورت الجروب يا {name}!", "🎉 أهلاً {name}!", "💫 مرحباً {name}!"];

function isAdmin(senderNumber) { return ADMIN_NUMBERS.includes(senderNumber.replace(/\D/g, '')); }
function getSenderNumber(msg) { return (msg.key.participant || msg.key.remoteJid || '').split('@')[0].replace(/\D/g, ''); }

async function getGroupMeta(jid) {
    if (groupMetaCache.has(jid)) return groupMetaCache.get(jid);
    const meta = await sock.groupMetadata(jid);
    groupMetaCache.set(jid, meta);
    return meta;
}

// ═══════════════════════════════════════════════════
// 🤖 بوت
// ═══════════════════════════════════════════════════
let sock, socketReady = false, reconnectState = 0, reconnectAttempts = 0, reconnectTimer = null, startingSocket = null;

function scheduleReconnect() {
    if (reconnectState === -1 || reconnectTimer) return;
    reconnectTimer = setTimeout(async () => {
        reconnectTimer = null;
        try {
            reconnectAttempts++;
            await startBot();
            reconnectAttempts = 0;
        } catch { if (reconnectAttempts < 20) scheduleReconnect(); }
    }, Math.min(5000 * reconnectAttempts, 60000));
}

async function startBot() {
    if (startingSocket) return;
    startingSocket = (async () => {
        await loadData();
        let creds = DB.creds || initAuthCreds();
        const { version } = await fetchLatestBaileysVersion();
        if (sock) { try { sock.ws?.close(); } catch {} }
        sock = makeWASocket({
            version,
            auth: { creds, keys: createKeyStore() },
            printQRInTerminal: false,
            logger: silentLogger,
            browser: ['DOMA BOT PRO', 'Chrome', '10.0.0'],
            markOnlineOnConnect: false,
            syncFullHistory: false
        });

        sock.ev.on('creds.update', (newCreds) => { Object.assign(creds, newCreds); DB.creds = creds; });
        sock.ev.on('connection.update', async ({ connection, lastDisconnect, qr }) => {
            if (qr) { console.log(await QRCode.toString(qr, { type: 'terminal', small: true })); }
            if (connection === 'open') { socketReady = true; reconnectState = 2; console.log('✅ متصل!'); }
            if (connection === 'close') {
                socketReady = false;
                if (lastDisconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut) scheduleReconnect();
                else { DB.creds = null; await saveAllData(); process.exit(1); }
            }
        });

        sock.ev.on('group-participants.update', async ({ id, participants, action }) => {
            if (action !== 'add') return;
            const group = await getGroup(id);
            if (group && !group.welcome) return;
            for (const p of participants) {
                if (p === sock.user?.id) continue;
                const msg = WELCOME_MESSAGES[Math.floor(Math.random() * WELCOME_MESSAGES.length)].replace('{name}', '@' + String(p).split('@')[0]);
                sendMessage(id, { text: msg, mentions: [p] });
            }
        });

        sock.ev.on('messages.upsert', async ({ messages }) => {
            for (const msg of messages) {
                if (!msg.message || msg.key.fromMe || processedMessages.has(msg.key.id)) continue;
                processedMessages.set(msg.key.id, true);
                metrics.incomingMessages++;
                if (checkGlobalFlood()) continue;

                const sender = msg.key.remoteJid;
                const senderNumber = getSenderNumber(msg);
                const isGroup = sender.endsWith('@g.us');
                const adminCheck = isAdmin(senderNumber);
                let text = msg.message.conversation || msg.message.extendedTextMessage?.text || msg.message.imageMessage?.caption || '';

                if (!text) continue;
                if (await isBanned(senderNumber)) continue;
                queueUserUpdate(senderNumber, { name: msg.pushName || 'مستخدم', lastActive: new Date().toISOString() });

                // إذاعة
                if (adminCheck && waitingBroadcast.has(senderNumber)) {
                    waitingBroadcast.delete(senderNumber);
                    const users = [...DB.users.keys()].slice(0, MAX_BROADCAST_USERS);
                    let success = 0;
                    for (const u of users) { try { await sendMessage(u + '@s.whatsapp.net', { text }); success++; } catch {} }
                    sendMessage(sender, { text: `✅ تم الإرسال إلى ${success}/${users.length}` });
                    continue;
                }

                if (adminCheck) {
                    if (text === ADMIN_KEYWORD) { sendMessage(sender, { image: { url: MENU_IMAGE_URL }, caption: `👑 أهلاً ${OWNER_NAME}\n📊 المستخدمين: ${DB.users.size}` }); continue; }
                    if (text === 'نشر خبر') { waitingBroadcast.add(senderNumber); sendMessage(sender, { text: '📢 أرسل الرسالة' }); continue; }
                    if (text === 'الأرقام') { sendMessage(sender, { text: [...DB.users.keys()].map((n,i) => `${i+1}. +${n}`).join('\n') || 'لا يوجد' }); continue; }
                    if (text.startsWith('حظر ')) { const t = text.split(' ')[1]; await banUser(t); sendMessage(sender, { text: `🚫 تم حظر ${t}` }); continue; }
                    if (text.startsWith('فك ')) { const t = text.split(' ')[1]; await unbanUser(t); sendMessage(sender, { text: `✅ تم فك حظر ${t}` }); continue; }
                }

                if (text === 'المكتبة' || text === 'اوامر') { sendMessage(sender, { image: { url: MENU_IMAGE_URL }, caption: '📚 الأوامر: رقمي, المطور, صورة' }); continue; }
                if (text === 'رقمي') { sendMessage(sender, { text: `📱 +${senderNumber}` }); continue; }
                if (text === 'المطور') { sendMessage(sender, { text: `👑 ${OWNER_NAME}\n📱 +${BOT_NUMBER}` }); continue; }

                // تحسين الصور
                if (text === 'تعديل' && msg.message.imageMessage) {
                    try {
                        const buf = await downloadMediaMessage(msg, 'buffer', {});
                        const enhanced = await sharp(buf).resize(800).sharpen().modulate({ brightness: 1.1 }).jpeg({ quality: 90 }).toBuffer();
                        sendMessage(sender, { image: enhanced, caption: '✨ تم التحسين' });
                        logImage(msg.pushName, senderNumber);
                    } catch { sendMessage(sender, { text: '❌ فشل' }); }
                    continue;
                }

                // إدارة الجروبات
                if (isGroup) {
                    const groupId = sender;
                    let group = await getGroup(groupId);
                    if (!group) { group = { welcome: true, links: true, badwords: true, maxWarn: 3 }; await updateGroup(groupId, group); }
                    if (group.links && linkRegex.test(text)) { try { await sock.sendMessage(groupId, { delete: msg.key }); } catch {} continue; }
                    if (group.badwords && badwordsRegex.test(text)) { try { await sock.sendMessage(groupId, { delete: msg.key }); } catch {} continue; }
                }

                // ردود ذكية
                for (const [k, v] of Object.entries(smartReplies)) {
                    if (text.includes(k)) { sendMessage(sender, { text: v[Math.floor(Math.random() * v.length)] }); return; }
                }
            }
        });
    })();
    return startingSocket;
}

// ═══════════════════════════════════════════════════
// 🌐 خادم ويب
// ═══════════════════════════════════════════════════
const app = express();
app.get('/', (_, res) => res.send('DOMA BOT PRO V5.4.2 running'));
app.listen(process.env.PORT || 3000, () => console.log('🌐 Web server on'));

startBot();
