'use strict';

// ---------------------------------------------------------------------------
// ha-wa-bridge – Baileys edition (no Puppeteer / Chromium required)
//
// Exposes the same WebSocket API as the whatsapp-web.js bridge so the HA
// integration works with either implementation without any changes.
//
// Key differences vs the original bridge:
//  • Auth : multi-file credential store (no browser profile / cache)
//  • QR   : raw string emitted directly by Baileys, forwarded as-is
//  • JIDs : Baileys uses @s.whatsapp.net for personal chats; we normalise
//            to @c.us on output to stay compatible with existing HA automations
//  • send_event : WhatsApp Scheduled Events are not yet exposed by Baileys;
//            the command is accepted but logs a warning and is silently ignored
// ---------------------------------------------------------------------------

const baileys = require('@whiskeysockets/baileys');
const makeWASocket = baileys.default;
const { useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion } = baileys;
const { Boom } = require('@hapi/boom');
const { WebSocketServer } = require('ws');
const qrcode = require('qrcode');
const pino = require('pino');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

// ── Config ───────────────────────────────────────────────────────────────────
let configOptions = {};
try {
    if (fs.existsSync('/data/options.json')) {
        configOptions = JSON.parse(fs.readFileSync('/data/options.json', 'utf8'));
    }
} catch (err) {
    console.error('Error reading options.json:', err);
}

const detectOwnMessages = configOptions.detect_own_messages
    || process.env.DETECT_OWN_MESSAGES === 'true'
    || false;

const incomingMode = (
    configOptions.incoming_messages_mode
    || process.env.INCOMING_MESSAGES_MODE
    || 'all'
).toLowerCase();

const incomingLogLevel = (
    configOptions.incoming_message_log_level
    || process.env.INCOMING_MESSAGE_LOG_LEVEL
    || 'FULL'
).toUpperCase();

let allowedGroups = configOptions.allowed_groups || process.env.ALLOWED_GROUPS || [];
if (typeof allowedGroups === 'string') {
    allowedGroups = allowedGroups.split(',').map(g => g.trim()).filter(Boolean);
}
const allowedGroupsLower = allowedGroups.map(g => g.toLowerCase());

let allowedNumbers = configOptions.allowed_numbers || process.env.ALLOWED_NUMBERS || [];
if (typeof allowedNumbers === 'string') {
    allowedNumbers = allowedNumbers.split(',').map(n => n.trim()).filter(Boolean);
}
// Plain phone numbers (no suffix) – compared with extractNumber(jid)
const allowedNumbersSet = new Set(allowedNumbers);

console.log(`[Baileys bridge] Incoming messages mode: ${incomingMode}`);
console.log(`[Baileys bridge] Incoming message log level: ${incomingLogLevel}`);
if (allowedGroupsLower.length > 0) {
    console.log(`[Baileys bridge] Allowed groups filter: ${allowedGroups.join(', ')}`);
}
if (allowedNumbersSet.size > 0) {
    console.log(`[Baileys bridge] Allowed numbers filter: ${allowedNumbers.join(', ')}`);
}

// ── JID helpers ──────────────────────────────────────────────────────────────

/**
 * Normalise a Baileys JID (@s.whatsapp.net) to the @c.us format that the
 * original bridge emitted, so existing HA automations comparing `from` keep
 * working without modification.
 */
function normalizeJidForHA(jid) {
    if (!jid) return jid;
    return jid.replace('@s.whatsapp.net', '@c.us');
}

function toPersonalJid(number) {
    if (!number) return number;
    if (number.includes('@')) return number;
    return `${number}@s.whatsapp.net`;
}

function toGroupJid(id) {
    if (!id) return id;
    if (id.includes('@')) return id;
    return `${id}@g.us`;
}

/** Strip JID suffix and device-part to get a plain phone number. */
function extractNumber(jid) {
    if (!jid) return '';
    return jid.split('@')[0].split(':')[0];
}

function isGroupJid(jid) {
    return typeof jid === 'string' && jid.endsWith('@g.us');
}

// ── Incoming-log helper ──────────────────────────────────────────────────────
function logIncomingData(type, data, rawObj) {
    if (incomingLogLevel === 'NONE') return;
    if (incomingLogLevel === 'COMPACT') {
        const sender = data.from || data.voter || 'unknown';
        const group = data.isGroup
            ? ` (Group: ${data.chatName})`
            : (data.group_id ? ` (Group ID: ${data.group_id})` : '');
        console.log(`[${type}] received from ${sender}${group}`);
    } else {
        console.log(`[${type}] RECEIVED`, rawObj);
    }
}

// ── Memory reporter (used to compare against the original bridge) ────────────
let lastMemorySignature = null;
function reportMemory() {
    const m = process.memoryUsage();
    const rssMb = Math.round(m.rss / 1024 / 1024);
    const heapUsedMb = Math.round(m.heapUsed / 1024 / 1024);
    const heapTotalMb = Math.round(m.heapTotal / 1024 / 1024);
    const externalMb = Math.round(m.external / 1024 / 1024);
    const memorySignature = `${rssMb}|${heapUsedMb}|${heapTotalMb}|${externalMb}`;
    if (memorySignature === lastMemorySignature) return;
    lastMemorySignature = memorySignature;
    console.log(
        `[MEMORY] RSS=${rssMb}MB` +
        `  Heap=${heapUsedMb}/${heapTotalMb}MB` +
        `  External=${externalMb}MB`
    );
}
// Store the reference so it can be cancelled if needed (e.g. in tests)
const memoryReportInterval = setInterval(reportMemory, 60_000);
memoryReportInterval.unref(); // Don't prevent process exit

// ── WebSocket server ─────────────────────────────────────────────────────────
const PORT = parseInt(process.env.PORT || '3000', 10);
const wss = new WebSocketServer({ port: PORT });
console.log(`WebSocket server started on port ${PORT}`);

let isReady = false;
let lastQr = null;
/** @type {ReturnType<typeof makeWASocket> | null} */
let sock = null;

function broadcast(data) {
    const payload = JSON.stringify(data);
    wss.clients.forEach(ws => {
        if (ws.readyState === 1) ws.send(payload);
    });
}

// ── Group metadata cache ─────────────────────────────────────────────────────
// Populated at startup and kept up-to-date by groups.upsert / groups.update.
// Avoids fetching all chats on every send-to-group-by-name request.
const groupCache = new Map(); // groupJid → GroupMetadata

async function getGroupName(groupJid) {
    const cached = groupCache.get(groupJid);
    if (cached?.subject) return cached.subject;
    if (!sock) return '';
    try {
        const meta = await sock.groupMetadata(groupJid);
        groupCache.set(groupJid, meta);
        return meta.subject || '';
    } catch {
        return '';
    }
}

// ── Poll message store ───────────────────────────────────────────────────────
// When we send a poll we record question + options so we can resolve the
// SHA-256 option hashes that Baileys returns in poll-vote updates.
const pollStore = new Map(); // messageId → { question, options: string[] }

function sha256(buf) {
    return crypto.createHash('sha256').update(buf).digest();
}

function resolveVoteOptions(pollEntry, selectedHashBuffers) {
    if (!pollEntry || !Array.isArray(selectedHashBuffers) || selectedHashBuffers.length === 0) {
        return [];
    }
    return pollEntry.options
        .filter(opt => {
            const optHash = sha256(Buffer.from(opt, 'utf-8'));
            return selectedHashBuffers.some(h => {
                const hBuf = Buffer.isBuffer(h) ? h : Buffer.from(h);
                return hBuf.equals(optHash);
            });
        })
        .map(name => ({ name }));
}

// ── Incoming-message filter ───────────────────────────────────────────────────
function shouldForwardMessage(fromJid, isGroup, chatName) {
    if (incomingMode === 'disabled') return false;
    if (incomingMode === 'groups_only' && !isGroup) return false;

    if (incomingMode === 'numbers_only') {
        if (isGroup) return false;
        if (!allowedNumbersSet.has(extractNumber(fromJid))) return false;
    }

    if (allowedGroupsLower.length > 0) {
        if (!isGroup) return false;
        if (!allowedGroupsLower.includes((chatName || '').toLowerCase())) return false;
    }

    if (allowedNumbersSet.size > 0 && incomingMode !== 'numbers_only') {
        if (isGroup) return false;
        if (!allowedNumbersSet.has(extractNumber(fromJid))) return false;
    }

    return true;
}

// ── Resolve destination JID ──────────────────────────────────────────────────
async function resolveChatId(number, group_name, group_id) {
    if (group_id) {
        const jid = toGroupJid(group_id);
        console.log(`Using group ID directly: ${jid}`);
        return jid;
    }

    if (group_name) {
        for (const [jid, meta] of groupCache.entries()) {
            if (meta.subject && meta.subject.toLowerCase() === group_name.toLowerCase()) {
                console.log(`Found group '${meta.subject}' with ID: ${jid}`);
                return jid;
            }
        }
        // group_name not found in cache; fall through to treat it as a number
        // (this mirrors the original bridge's fallback behaviour and handles the
        //  broadcast case where target is passed as both number and group_name)
    }

    if (number) {
        return toPersonalJid(number);
    }

    return null;
}

// ── Media helpers ─────────────────────────────────────────────────────────────
function buildMediaContent(media, caption) {
    if (!media) return null;
    const buf = Buffer.from(media.data, 'base64');
    const mime = (media.mimetype || 'application/octet-stream').toLowerCase();
    const filename = media.filename || 'file';

    if (mime.startsWith('image/')) {
        return { image: buf, mimetype: mime, caption };
    }
    if (mime.startsWith('video/')) {
        return { video: buf, mimetype: mime, caption, fileName: filename };
    }
    if (mime.startsWith('audio/')) {
        return { audio: buf, mimetype: mime, ptt: false };
    }
    return { document: buf, mimetype: mime, fileName: filename, caption };
}

// ── Send handlers ─────────────────────────────────────────────────────────────
async function handleSendMessage(number, text, group_name, group_id, media) {
    const chatId = await resolveChatId(number, group_name, group_id);
    if (!chatId) {
        console.error('No valid destination (number or group_name) provided.');
        return;
    }
    try {
        if (media) {
            const content = buildMediaContent(media, text);
            if (content) {
                await sock.sendMessage(chatId, content);
                console.log(`Sent media message to ${chatId}: ${text || '(no caption)'}`);
            }
        } else {
            await sock.sendMessage(chatId, { text: text || '' });
            console.log(`Sent message to ${chatId}: ${text}`);
        }
    } catch (err) {
        console.error(`Failed to send message to ${chatId}:`, err);
    }
}

async function handleSendPoll(number, group_name, group_id, pollQuestion, options, allow_multiple_answers) {
    const chatId = await resolveChatId(number, group_name, group_id);
    if (!chatId) {
        console.error('No valid destination (number or group_name) provided for poll.');
        return;
    }
    try {
        const result = await sock.sendMessage(chatId, {
            poll: {
                name: pollQuestion,
                values: options,
                // selectableCount 0 = any number of options (multiple answers),
                // 1 = exactly one (single answer)
                selectableCount: allow_multiple_answers ? 0 : 1,
            },
        });
        const msgId = result?.key?.id;
        if (msgId) {
            pollStore.set(msgId, { question: pollQuestion, options });
        }
        console.log(`Sent poll to ${chatId}: ${pollQuestion}`);
    } catch (err) {
        console.error(`Failed to send poll to ${chatId}:`, err);
    }
}

async function handleGetGroups(ws) {
    if (!sock) {
        ws.send(JSON.stringify({ type: 'get_groups_response', data: [], error: 'Not connected' }));
        return;
    }
    try {
        const groups = await sock.groupFetchAllParticipating();
        // Refresh cache while we're here
        for (const [id, meta] of Object.entries(groups)) {
            groupCache.set(id, meta);
        }
        const result = Object.entries(groups).map(([id, meta]) => ({
            id,
            name: meta.subject || '',
        }));
        console.log(`Returning ${result.length} groups.`);
        ws.send(JSON.stringify({ type: 'get_groups_response', data: result }));
    } catch (err) {
        console.error('Error fetching groups:', err);
        ws.send(JSON.stringify({ type: 'get_groups_response', data: [], error: err.message }));
    }
}

async function handleSetGroupSubject(ws, group_id, subject) {
    if (!group_id || !subject) {
        ws.send(JSON.stringify({
            type: 'set_group_subject_response',
            success: false,
            error: 'group_id and subject are required',
        }));
        return;
    }
    const chatId = toGroupJid(group_id);
    try {
        await sock.groupUpdateSubject(chatId, subject);
        console.log(`Set group subject for ${chatId} to "${subject}"`);
        ws.send(JSON.stringify({ type: 'set_group_subject_response', success: true }));
    } catch (err) {
        console.error(`Failed to set group subject for ${chatId}:`, err);
        ws.send(JSON.stringify({ type: 'set_group_subject_response', success: false, error: err.message }));
    }
}

async function handleSetGroupPicture(ws, group_id, media) {
    if (!group_id || !media) {
        ws.send(JSON.stringify({
            type: 'set_group_picture_response',
            success: false,
            error: 'group_id and media are required',
        }));
        return;
    }
    const chatId = toGroupJid(group_id);
    try {
        const buf = Buffer.from(media.data, 'base64');
        await sock.updateProfilePicture(chatId, buf);
        console.log(`Set group picture for ${chatId}`);
        ws.send(JSON.stringify({ type: 'set_group_picture_response', success: true }));
    } catch (err) {
        console.error(`Failed to set group picture for ${chatId}:`, err);
        ws.send(JSON.stringify({ type: 'set_group_picture_response', success: false, error: err.message }));
    }
}

async function handleSendEvent(number, group_name, group_id, name, description, location, start_time, end_time, call_type) {
    // WhatsApp Scheduled Events are not exposed by the Baileys library yet.
    // The command is accepted so HA does not error out, but the event is not sent.
    console.warn(
        `send_event: WhatsApp Scheduled Events are not currently supported by the Baileys runtime. ` +
        `Event "${name}" was NOT sent.`
    );
}

// ── Baileys socket ────────────────────────────────────────────────────────────
const AUTH_DIR = process.env.WA_DATA_PATH
    ? path.join(process.env.WA_DATA_PATH, 'baileys_auth')
    : path.join(process.cwd(), '.baileys_auth');

async function startBaileys() {
    const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);

    let version;
    try {
        const res = await fetchLatestBaileysVersion();
        version = res.version;
        console.log(`Using WhatsApp Web version ${version.join('.')}`);
    } catch {
        console.warn('Could not fetch latest WA version; using Baileys default.');
    }

    sock = makeWASocket({
        version,
        auth: state,
        // Silence Baileys' own pino logger; the bridge uses console.log directly.
        logger: pino({ level: 'silent' }),
        printQRInTerminal: false,
        // Identify as a named desktop client (not anonymous browser)
        browser: ['Ha-Wa-Bridge-Baileys', 'Chrome', '10.0'],
        // Reduce memory: skip full chat history sync and link previews
        syncFullHistory: false,
        generateHighQualityLinkPreview: false,
        markOnlineOnConnect: false,
    });

    // Save credentials whenever they change
    sock.ev.on('creds.update', saveCreds);

    // ── Connection lifecycle ──────────────────────────────────────────────────
    sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update;

        if (qr) {
            console.log('QR Code received');
            lastQr = qr;
            qrcode.toString(qr, { type: 'terminal', small: true }, (err, str) => {
                if (!err) console.log(str);
            });
            broadcast({ type: 'qr', data: qr });
        }

        if (connection === 'connecting') {
            broadcast({ type: 'status', status: 'initializing' });
        }

        if (connection === 'open') {
            console.log('WhatsApp Client is ready!');
            isReady = true;
            lastQr = null;
            broadcast({ type: 'status', status: 'ready' });
            reportMemory();

            // Pre-warm the group cache so group-by-name lookups work immediately
            try {
                const groups = await sock.groupFetchAllParticipating();
                for (const [id, meta] of Object.entries(groups)) {
                    groupCache.set(id, meta);
                }
                console.log(`Group cache warmed: ${groupCache.size} groups.`);
            } catch (err) {
                console.warn('Could not pre-load group cache:', err.message);
            }
        }

        if (connection === 'close') {
            const code = new Boom(lastDisconnect?.error)?.output?.statusCode;
            const loggedOut = code === DisconnectReason.loggedOut;
            console.log(`Connection closed – reason code: ${code}. Logged out: ${loggedOut}`);
            isReady = false;

            if (loggedOut) {
                console.log('Logged out. Clearing auth state...');
                try { fs.rmSync(AUTH_DIR, { recursive: true, force: true }); } catch (rmErr) {
                    console.error('Failed to remove auth directory:', rmErr.message);
                }
                broadcast({ type: 'status', status: 'auth_failure' });
            }
            // Exit so Docker / HA Supervisor restarts the container
            process.exit(1);
        }
    });

    // ── Group metadata updates ────────────────────────────────────────────────
    sock.ev.on('groups.upsert', (groups) => {
        for (const g of groups) groupCache.set(g.id, g);
    });
    sock.ev.on('groups.update', (updates) => {
        for (const u of updates) {
            const existing = groupCache.get(u.id) || {};
            groupCache.set(u.id, { ...existing, ...u });
        }
    });

    // ── Incoming messages ─────────────────────────────────────────────────────
    if (incomingMode !== 'disabled') {
        sock.ev.on('messages.upsert', async ({ messages, type }) => {
            // 'notify' = new real-time messages; 'append' = historical sync
            if (type !== 'notify') return;

            for (const msg of messages) {
                const { key, message: msgContent, messageTimestamp } = msg;
                const remoteJid = key.remoteJid;

                // Skip WhatsApp status broadcasts
                if (!remoteJid || remoteJid === 'status@broadcast') continue;

                // Skip protocol / empty messages
                if (!msgContent) continue;

                const fromMe = !!key.fromMe;
                if (fromMe && !detectOwnMessages) continue;

                const group = isGroupJid(remoteJid);
                // In group chats key.participant is the sender; in DMs it's remoteJid
                const senderJid = group
                    ? (key.participant || msg.participant || '')
                    : remoteJid;

                let chatName = '';
                if (group) {
                    chatName = await getGroupName(remoteJid);
                }

                if (!shouldForwardMessage(senderJid, group, chatName)) continue;

                // Extract text from all common message subtypes
                const body = msgContent.conversation
                    || msgContent.extendedTextMessage?.text
                    || msgContent.imageMessage?.caption
                    || msgContent.videoMessage?.caption
                    || msgContent.documentMessage?.caption
                    || '';

                const hasMedia = !!(
                    msgContent.imageMessage
                    || msgContent.videoMessage
                    || msgContent.audioMessage
                    || msgContent.documentMessage
                    || msgContent.stickerMessage
                );

                const ts = typeof messageTimestamp === 'object'
                    ? Number(messageTimestamp)
                    : messageTimestamp;

                const payloadData = {
                    // Normalise @s.whatsapp.net → @c.us for HA automation compat
                    from: normalizeJidForHA(remoteJid),
                    to: normalizeJidForHA(fromMe ? remoteJid : (sock.user?.id || '')),
                    body,
                    timestamp: ts,
                    hasMedia,
                    author: normalizeJidForHA(senderJid),
                    deviceType: 'unknown',
                    isForwarded: !!(msgContent.extendedTextMessage?.contextInfo?.isForwarded),
                    fromMe,
                    chatName,
                    isGroup: group,
                    groupId: group ? remoteJid : null,
                };

                logIncomingData('MESSAGE', payloadData, msg);
                broadcast({ type: 'message', data: payloadData });
            }
        });
    } else {
        console.log('Incoming message handling is DISABLED. The bridge will not forward any received messages to Home Assistant.');
    }

    // ── Poll vote updates ─────────────────────────────────────────────────────
    // Baileys delivers poll-vote updates as messages.update events with a
    // pollUpdates field.  The selected options arrive as SHA-256 hashes of the
    // option text; we resolve them against the local pollStore (populated when
    // we sent the poll).  Votes on polls sent by others will have empty
    // selectedOptions – this is a known limitation of the Baileys API.
    sock.ev.on('messages.update', async (updates) => {
        for (const update of updates) {
            if (!update.update?.pollUpdates) continue;

            const { key, update: { pollUpdates } } = update;
            const remoteJid = key.remoteJid;
            const group = isGroupJid(remoteJid);
            const chatName = group ? await getGroupName(remoteJid) : '';

            if (incomingMode === 'disabled') continue;
            if (incomingMode === 'groups_only' && !group) continue;
            if (incomingMode === 'numbers_only' && group) continue;
            if (allowedGroupsLower.length > 0 && (!group || !allowedGroupsLower.includes(chatName.toLowerCase()))) continue;

            for (const pollUpdate of pollUpdates) {
                const voterJid = pollUpdate.pollUpdateMessageKey?.participant
                    || pollUpdate.pollUpdateMessageKey?.remoteJid
                    || '';
                const voter = extractNumber(voterJid);

                if (incomingMode === 'numbers_only' && !allowedNumbersSet.has(voter)) continue;
                if (allowedNumbersSet.size > 0 && incomingMode !== 'numbers_only' && !allowedNumbersSet.has(voter)) continue;

                const pollMsgId = key.id;
                const pollEntry = pollStore.get(pollMsgId);
                const selectedHashes = pollUpdate.vote?.selectedOptions || [];
                const selectedOptions = resolveVoteOptions(pollEntry, selectedHashes);

                const ts = pollUpdate.senderTimestampMs
                    ? Number(pollUpdate.senderTimestampMs) / 1000
                    : Math.floor(Date.now() / 1000);

                const payloadData = {
                    voter,
                    group_id: group ? extractNumber(remoteJid) : null,
                    selectedOptions,
                    pollCreationMessageId: pollMsgId,
                    timestamp: ts,
                };

                logIncomingData('VOTE_UPDATE', payloadData, update);
                broadcast({ type: 'poll_vote', data: payloadData });
            }
        }
    });
}

// ── WebSocket command handler ─────────────────────────────────────────────────
wss.on('connection', (ws) => {
    console.log('New client connected');

    if (isReady) {
        ws.send(JSON.stringify({ type: 'status', status: 'ready' }));
    } else if (lastQr) {
        ws.send(JSON.stringify({ type: 'qr', data: lastQr }));
    } else {
        ws.send(JSON.stringify({ type: 'status', status: 'initializing' }));
    }

    ws.on('message', async (raw) => {
        let data;
        try {
            data = JSON.parse(raw);
        } catch {
            console.error('Invalid JSON from HA client');
            return;
        }
        console.log('Received command:', data);

        try {
            if (data.type === 'send_message') {
                const { number, message: text, group_name, group_id, media } = data;
                await handleSendMessage(number, text, group_name, group_id, media);

            } else if (data.type === 'send_poll') {
                const { number, group_name, group_id, message: pollQuestion, options, allow_multiple_answers } = data;
                await handleSendPoll(number, group_name, group_id, pollQuestion, options, allow_multiple_answers);

            } else if (data.type === 'broadcast') {
                const { targets, message: text, media } = data;
                if (Array.isArray(targets) && targets.length > 0) {
                    console.log(`Broadcasting message to ${targets.length} targets.`);
                    for (const target of targets) {
                        // Pass target as both number and group_name: resolveChatId will
                        // first try to match a group by name, then fall back to treating
                        // it as a phone number – intentional, mirrors the original bridge.
                        await handleSendMessage(target, text, target, null, media);
                    }
                } else {
                    console.error('No targets provided for broadcast.');
                }

            } else if (data.type === 'get_groups') {
                await handleGetGroups(ws);

            } else if (data.type === 'set_group_subject') {
                const { group_id, subject } = data;
                await handleSetGroupSubject(ws, group_id, subject);

            } else if (data.type === 'set_group_picture') {
                const { group_id, media } = data;
                await handleSetGroupPicture(ws, group_id, media);

            } else if (data.type === 'send_event') {
                const { number, group_name, group_id, name, description, location, start_time, end_time, call_type } = data;
                await handleSendEvent(number, group_name, group_id, name, description, location, start_time, end_time, call_type);
            }
        } catch (err) {
            console.error('Error processing command:', err);
        }
    });
});

// ── Bootstrap ─────────────────────────────────────────────────────────────────
console.log('Initializing WhatsApp Baileys bridge (no browser required)…');
reportMemory(); // baseline before Baileys loads

startBaileys().catch(err => {
    console.error('Fatal error starting Baileys:', err);
    process.exit(1);
});
