process.on('uncaughtException', err => console.log('خطأ:', err.message));
process.on('unhandledRejection', err => console.log('خطأ:', err.message));

process.on('SIGINT', () => {
    saveDB();
    process.exit(0);
});
process.on('SIGTERM', () => {
    saveDB();
    process.exit(0);
});

const { default: makeWASocket, useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion } = require('@whiskeysockets/baileys');
const pino = require('pino');
const QRCode = require('qrcode');
const fs = require('fs');

const ADMIN_NUMBERS = ["584164041083", "141451090509918"];
const BOT_NUMBER = "260752535332";
const ADMIN_KEYWORD = "عبدو";
const MENU_IMAGE_URL = "https://i.ibb.co/vChK2Y35/IMG.jpg";
const DB_FILE = 'doma_db.json';
const WARNING_MSG = "⚠️ 𓂋𓍿𓀀𓏥𓃀𓅱𓏏𓇋𓈖𓂧𓅱𓅓𓄿𓏏𓇋𓈖𓎼𓂧𓄿𓏏𓄿 𓃀𓅱𓏏𓇋𓈖𓂧𓅱𓅓𓄿𓏏𓇋𓈖𓎼𓂧𓄿𓏏𓄿";

// قاعدة بيانات
let db = {
    users: {},
    groups: {},
    warnings: {},
    banned: [],
    stats: { messages: 0, commands: 0 },
    broadcastWaiting: null,
    tempAction: null
};

function loadDB() { try { if (fs.existsSync(DB_FILE)) db = JSON.parse(fs.readFileSync(DB_FILE)); } catch {} }
function saveDB() { fs.writeFileSync(DB_FILE, JSON.stringify(db)); }
loadDB();

setInterval(saveDB, 10000);

const BADWORDS = [
    'كس', 'طيز', 'منيوك', 'متناك', 'شرموط', 'قحبه', 'زب', 'بزاز',
    'كس امك', 'كس اختك', 'احا', 'خول', 'عرص', 'لبوه', 'منيوج',
    'fuck', 'shit', 'bitch', 'ass', 'dick', 'pussy', 'whore', 'bastard',
    'nigga', 'faggot', 'slut', 'cunt', 'motherfucker', 'nigger'
];

const replies = {
    salam: [
        'وعليكم السلام 🤍\nانا عبد الرحمن ابن مصر 🇪🇬\nبوت وظيفتي الوحيد الترحيب بالأعضاء وتحذير الأعضاء.\nعشان اتفعل ضفني في جروبك فقط ✨'
    ],
    warn: ['⚠️ ممنوع الإزعاج', '⚠️ التزم بالقوانين']
};

async function startBot() {
    console.log('⏳ جاري تشغيل البوت...');
    
    const { state, saveCreds } = await useMultiFileAuthState('auth_info');
    const { version } = await fetchLatestBaileysVersion();

    const sock = makeWASocket({
        version,
        auth: state,
        logger: pino({ level: 'silent' }),
        browser: ['Ubuntu', 'Chrome', '20.0.0'],
        markOnlineOnConnect: false,
        syncFullHistory: false,
        printQRInTerminal: false,
        connectTimeoutMs: 60000,
        defaultQueryTimeoutMs: 60000,
        keepAliveIntervalMs: 15000,
        retryRequestDelayMs: 3000,
        emitOwnEvents: false,
        fireInitQueries: false,
        generateHighQualityLinkPreview: false,
        getMessage: async () => ({ conversation: 'DOMA BOT' })
    });

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update;
        if (qr) {
            console.log('\n📱 امسح كود QR للدخول:\n');
            try { console.log(await QRCode.toString(qr, { type: 'terminal', small: true })); } catch {}
            console.log('\n📲 أو: واتساب > الأجهزة المرتبطة > ربط جهاز\n');
        }
        if (connection === 'open') console.log('✅ البوت متصل وجاهز!');
        if (connection === 'close') {
            const code = lastDisconnect?.error?.output?.statusCode;
            if (code !== DisconnectReason.loggedOut) {
                console.log('🔄 إعادة الاتصال...');
                if (!global.reconnecting) {
                    global.reconnecting = true;
                    setTimeout(async () => {
                        try {
                            await startBot();
                        } finally {
                            global.reconnecting = false;
                        }
                    }, 10000);
                }
            }
        }
    });

    // 👥 الترحيب + المغادرة
    sock.ev.on('group-participants.update', async (update) => {
        const { id, participants, action } = update;
        
        if (action === 'add') {
            for (const p of participants) {
                if (p === sock.user?.id) continue;
                const number = String(p).split('@')[0];
                const cleanNumber = number.replace(/\D/g, '');
                const welcomeMsg = `🌟 خش جنب اخوتك يا @${cleanNumber}!\n\n` +
                    `📱 رقمك: +${cleanNumber}\n\n` +
                    `⚠️ *تنبيه مهم:*\n` +
                    `أنا بوت وظيفتي الترحيب بالأعضاء وتحذيرهم من المخالفات.\n` +
                    `لتفعيل البوت في جروبك، أضفني كأدمن.\n\n` +
                    `🤖 *عبد الرحمن - ابن مصر*\n\n` +
                    `${WARNING_MSG}`;
                try {
                    const pp = await sock.profilePictureUrl(p, 'image');
                    await sock.sendMessage(id, { image: { url: pp }, caption: welcomeMsg, mentions: [p] });
                } catch {
                    await sock.sendMessage(id, { text: welcomeMsg, mentions: [p] });
                }
                if (!db.users[cleanNumber]) db.users[cleanNumber] = { name: cleanNumber, firstSeen: new Date().toISOString(), messages: 0 };
            }
        }
        
        if (action === 'remove') {
            for (const p of participants) {
                if (p === sock.user?.id) continue;
                const number = String(p).split('@')[0];
                const cleanNumber = number.replace(/\D/g, '');
                const leaveMsg = `🚶 واحد ريحنا من صداعو @${cleanNumber}!`;
                try {
                    const pp = await sock.profilePictureUrl(p, 'image');
                    await sock.sendMessage(id, { image: { url: pp }, caption: leaveMsg, mentions: [p] });
                } catch {
                    await sock.sendMessage(id, { text: leaveMsg, mentions: [p] });
                }
            }
        }
    });

    // 📩 معالجة الرسائل
    sock.ev.on('messages.upsert', async ({ messages }) => {
        const msg = messages[0];
        if (!msg.message || msg.key.fromMe) return;
        
        const sender = msg.key.remoteJid;
        const senderNumber = (msg.key.participant || msg.key.remoteJid || '').split('@')[0].replace(/\D/g, '');
        const isGroup = sender.endsWith('@g.us');
        const isAdmin = ADMIN_NUMBERS.some(a => a.replace(/\D/g, '') === senderNumber);
        
        let text = msg.message.conversation || msg.message.extendedTextMessage?.text || msg.message.imageMessage?.caption || '';
        if (!text) return;

        console.log(`[${senderNumber}]: ${text}`);

        if (!db.users[senderNumber]) db.users[senderNumber] = { name: msg.pushName || '', firstSeen: new Date().toISOString(), messages: 0 };
        db.users[senderNumber].messages++;
        db.stats.messages++;

        // لوحة الأدمن
        if (isAdmin && text === ADMIN_KEYWORD) {
            db.stats.commands++;
            const panel = `╔══════════════════════════════╗
║   👑 لوحة تحكم الأدمن        ║
║   🤖 DOMA BOT PRO            ║
╠══════════════════════════════╣
║ 📊 المستخدمين: ${Object.keys(db.users).length} ║
║ 👥 الجروبات: ${Object.keys(db.groups).length}  ║
║ 🚫 المحظورين: ${db.banned.length} ║
║ 💬 الرسائل: ${db.stats.messages} ║
╠══════════════════════════════╣
║ • عبدو - القائمة             ║
║ • الأرقام - عرض الأرقام     ║
║ • الجروبات - عرض الجروبات   ║
║ • حظر / فك - إدارة          ║
║ • إذاعة - إرسال للكل        ║
║ • سحب - سحب أعضاء الجروب    ║
║ • تشغيل - تفعيل البوت       ║
║ • ايقاف - إيقاف البوت       ║
║ • اطفي - إيقاف البوت نهائي  ║
╚══════════════════════════════╝\n\n${WARNING_MSG}`;
            await sock.sendMessage(sender, { image: { url: MENU_IMAGE_URL }, caption: panel });
            return;
        }

        // أوامر الأدمن
        if (isAdmin) {
            if (text === 'إذاعة') { db.broadcastWaiting = senderNumber; await sock.sendMessage(sender, { text: '📢 أرسل رسالة الإذاعة الآن:' }); return; }
            if (db.broadcastWaiting === senderNumber) {
                db.broadcastWaiting = null;
                const users = Object.keys(db.users).slice(0, 200);
                let sent = 0;
                for (const u of users) {
                    try { 
                        await sock.sendMessage(u + '@s.whatsapp.net', { text: text }); 
                        sent++; 
                    } catch {}
                    await new Promise(r => setTimeout(r, 2000));
                }
                await sock.sendMessage(sender, { text: `✅ تم الإرسال إلى ${sent} مستخدم` });
                return;
            }
            if (text === 'الأرقام') {
                const list = Object.keys(db.users).map((n,i) => `${i+1}. +${n}`).join('\n') || 'لا يوجد';
                await sock.sendMessage(sender, { text: `📞 الأرقام:\n${list}` });
                return;
            }
            if (text.startsWith('حظر ')) {
                const t = text.split(' ')[1]?.replace(/\D/g, '');
                if (t && !db.banned.includes(t)) { db.banned.push(t); await sock.sendMessage(sender, { text: `🚫 تم حظر +${t}` }); }
                return;
            }
            if (text.startsWith('فك ')) {
                const t = text.split(' ')[1]?.replace(/\D/g, '');
                db.banned = db.banned.filter(n => n !== t);
                await sock.sendMessage(sender, { text: `✅ تم فك حظر +${t}` });
                return;
            }
            if (text === 'اطفي') { saveDB(); await sock.sendMessage(sender, { text: '⏹️ جاري إيقاف البوت...' }); process.exit(0); }
            if (text === 'تشغيل') { if (isGroup) { db.groups[sender] = true; saveDB(); await sock.sendMessage(sender, { text: '✅ تم تشغيل البوت في الجروب' }); } return; }
            
            if (text === 'ايقاف') {
                if (isGroup) {
                    delete db.groups[sender];
                    saveDB();
                    await sock.sendMessage(sender, { text: '⛔ تم إيقاف البوت في الجروب' });
                } else {
                    const groups = Object.entries(db.groups).filter(([id, active]) => active);
                    if (groups.length === 0) {
                        await sock.sendMessage(sender, { text: '📋 *لا توجد جروبات مفعلة*' });
                    } else {
                        let list = '📋 *اختر رقم الجروب لإيقافه:*\n\n';
                        for (let i = 0; i < groups.length; i++) {
                            const [id] = groups[i];
                            try {
                                const meta = await sock.groupMetadata(id);
                                list += `${i + 1}. ${meta.subject}\n`;
                            } catch {
                                list += `${i + 1}. ${id}\n`;
                            }
                        }
                        list += '\n*اكتب الرقم فقط*';
                        await sock.sendMessage(sender, { text: list });
                        db.tempAction = { action: 'stop', sender: senderNumber };
                    }
                }
                return;
            }
            
            if (text === 'سحب') {
                const groups = Object.entries(db.groups).filter(([id, active]) => active);
                if (groups.length === 0) {
                    await sock.sendMessage(sender, { text: '📋 *لا توجد جروبات مفعلة*' });
                } else {
                    let list = '📋 *اختر رقم الجروب لسحب الأعضاء:*\n\n';
                    for (let i = 0; i < groups.length; i++) {
                        const [id] = groups[i];
                        try {
                            const meta = await sock.groupMetadata(id);
                            list += `${i + 1}. ${meta.subject}\n`;
                        } catch {
                            list += `${i + 1}. ${id}\n`;
                        }
                    }
                    list += '\n*اكتب الرقم فقط*';
                    await sock.sendMessage(sender, { text: list });
                    db.tempAction = { action: 'pull', sender: senderNumber };
                }
                return;
            }
        }

        // معالجة اختيار الجروب
        if (db.tempAction && db.tempAction.sender === senderNumber) {
            const num = parseInt(text);
            const groups = Object.entries(db.groups).filter(([id, active]) => active);
            
            if (num > 0 && num <= groups.length) {
                const [groupId] = groups[num - 1];
                
                if (db.tempAction.action === 'stop') {
                    delete db.groups[groupId];
                    saveDB();
                    db.tempAction = null;
                    try {
                        const meta = await sock.groupMetadata(groupId);
                        await sock.sendMessage(sender, { text: `⛔ تم إيقاف البوت في جروب: ${meta.subject}` });
                    } catch {
                        await sock.sendMessage(sender, { text: '⛔ تم إيقاف البوت في الجروب' });
                    }
                }
                
                if (db.tempAction.action === 'pull') {
                    try {
                        const meta = await sock.groupMetadata(groupId);
                        const members = meta.participants.map((p, i) => `${i + 1}. +${p.id.split('@')[0]}`).join('\n');
                        
                        const safeName = meta.subject.replace(/[\\\/:*?"<>|]/g, '_');
                        const fileName = `اعضاء_${safeName}.txt`;
                        fs.writeFileSync(fileName, `أعضاء جروب: ${meta.subject}\n${'='.repeat(30)}\n\n${members}`);
                        
                        await sock.sendMessage(sender, {
                            document: { url: fileName },
                            fileName: fileName,
                            caption: `📋 تم سحب ${meta.participants.length} عضو من جروب: ${meta.subject}`
                        });
                        
                        setTimeout(() => { try { fs.unlinkSync(fileName); } catch {} }, 5000);
                    } catch (e) {
                        await sock.sendMessage(sender, { text: '❌ فشل سحب الأعضاء' });
                    }
                    db.tempAction = null;
                }
            } else {
                await sock.sendMessage(sender, { text: '❌ رقم غير صحيح' });
            }
            return;
        }

        // تحذير الشتائم والروابط مع مسح الرسالة والطرد التلقائي
        if (isGroup && db.groups[sender]) {
            const hasBadword = BADWORDS.some(word => text.toLowerCase().includes(word));
            const hasLink = /https?:\/\/\S+|chat\.whatsapp\.com\/\S+/i.test(text);
            if (hasBadword || hasLink) {
                try {
                    await sock.sendMessage(sender, { delete: msg.key });
                } catch {}
                
                db.warnings[senderNumber] = (db.warnings[senderNumber] || 0) + 1;
                const count = db.warnings[senderNumber];
                
                if (count >= 3) {
                    try {
                        const participantId = msg.key.participant || (senderNumber + '@s.whatsapp.net');
                        await sock.groupParticipantsUpdate(sender, [participantId], 'remove');
                        await sock.sendMessage(sender, { text: `🚫 *تم طرد العضو تلقائياً*\nالسبب: ${count} تحذيرات - مخالفة القوانين\n\n${WARNING_MSG}` });
                    } catch {
                        await sock.sendMessage(sender, { text: `⚠️ *${count}/3 تحذيرات*\nالبوت مش أدمن، يرجى طرد العضو يدوياً\n\n${WARNING_MSG}` });
                    }
                } else {
                    await sock.sendMessage(sender, { text: `⚠️ *تحذير ${count}/3*\n${replies.warn[0]}\n\nيمنع استخدام الكلمات المحظورة أو الروابط!\n\n${WARNING_MSG}` });
                }
                return;
            }
        }

        // فلتر الجروبات
        const mentions = msg.message?.extendedTextMessage?.contextInfo?.mentionedJid || [];
        const botJid = sock.user.id.split(':')[0] + '@s.whatsapp.net';
        const isMentioned = mentions.includes(botJid);
        const quoted = msg.message?.extendedTextMessage?.contextInfo?.participant || '';
        const isReplyToBot = quoted.startsWith(sock.user.id.split(':')[0]);

        if (isGroup) {
            if (!db.groups[sender]) return;
            if (!isMentioned && !isReplyToBot && !isAdmin) return;
        }

        // الرد على السلام فقط
        if (text.includes('السلام عليكم') || text.includes('سلام عليكم') || text.includes('السلام') || text.includes('سلام')) {
            await sock.sendMessage(sender, { text: replies.salam[0] });
        }
    });
}

if (!global.reconnecting) {
    global.reconnecting = true;
    startBot().finally(() => {
        global.reconnecting = false;
    });
                                                                     }
