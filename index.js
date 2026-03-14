const { default: makeWASocket, useMultiFileAuthState, Browsers, delay } = require("@whiskeysockets/baileys");
const readline = require('readline');

async function start() {
    const { state, saveCreds } = await useMultiFileAuthState('auth_info');
    const sock = makeWASocket({
        auth: state,
        browser: Browsers.ubuntu('Chrome'),
        printQRInTerminal: false
    });

    if (!sock.authState.creds.registered) {
        const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
        console.log("\n--- السيرفر استعد، مستني رقمك ---");
        rl.question('دخل رقمك بـ 20 (مثلاً: 201012345678): ', async (num) => {
            await delay(5000);
            try {
                const code = await sock.requestPairingCode(num.trim());
                console.log("\n✅ كود الربط أهو: " + code);
            } catch (e) { console.log("\n❌ السيرفر تقيل، جرب كمان شوية."); }
            rl.close();
        });
    }
    sock.ev.on('creds.update', saveCreds);
}
start();
