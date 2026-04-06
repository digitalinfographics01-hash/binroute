#!/usr/bin/env node
/**
 * Fetch cascade CSV attachments from Gmail via Gmail API (OAuth2).
 * Downloads ALL *_original_gateway_decline* attachments from analytics@sticky.io
 * for all clients (kytsanmanagementllc, optimus, primecommerce, purhorizon, etc.)
 *
 * First run: opens browser for OAuth authorization.
 * Subsequent runs: uses saved token automatically.
 *
 * Usage:
 *   node scripts/fetch-cascade-emails.js              — fetch new attachments
 *   node scripts/fetch-cascade-emails.js --days 60    — search last N days (default 30)
 *   node scripts/fetch-cascade-emails.js --all        — search all time
 */

const fs = require('fs');
const path = require('path');
const http = require('http');
const { URL } = require('url');
const { google } = require('googleapis');

const CREDENTIALS_PATH = path.join(__dirname, '..', 'gmail-credentials.json');
const TOKEN_PATH = path.join(__dirname, '..', 'gmail-token.json');
const SAVE_DIR = path.join(__dirname, '..', 'Cascaded orders data', 'StickyIO_Attachments');
const SENDER = 'analytics@sticky.io';
const ATTACHMENT_PATTERN = 'original_gateway_decline';
const SCOPES = ['https://www.googleapis.com/auth/gmail.readonly'];

if (!fs.existsSync(SAVE_DIR)) {
  fs.mkdirSync(SAVE_DIR, { recursive: true });
}

const existingFiles = new Set(fs.readdirSync(SAVE_DIR));

async function authorize() {
  const creds = JSON.parse(fs.readFileSync(CREDENTIALS_PATH, 'utf8'));
  const { client_id, client_secret } = creds.installed || creds.web;

  const oAuth2Client = new google.auth.OAuth2(
    client_id, client_secret, 'http://localhost:3847/oauth2callback'
  );

  // Check for saved token
  if (fs.existsSync(TOKEN_PATH)) {
    const token = JSON.parse(fs.readFileSync(TOKEN_PATH, 'utf8'));
    oAuth2Client.setCredentials(token);

    // Check if token needs refresh
    if (token.expiry_date && token.expiry_date < Date.now()) {
      try {
        const { credentials } = await oAuth2Client.refreshAccessToken();
        oAuth2Client.setCredentials(credentials);
        fs.writeFileSync(TOKEN_PATH, JSON.stringify(credentials));
        console.log('Token refreshed.');
      } catch (e) {
        console.log('Token expired, re-authorizing...');
        return getNewToken(oAuth2Client);
      }
    }
    return oAuth2Client;
  }

  return getNewToken(oAuth2Client);
}

function getNewToken(oAuth2Client) {
  return new Promise((resolve, reject) => {
    const authUrl = oAuth2Client.generateAuthUrl({
      access_type: 'offline',
      scope: SCOPES,
      prompt: 'consent',
    });

    console.log('\nOpening browser for authorization...');
    console.log('If browser does not open, visit this URL:\n');
    console.log(authUrl);
    console.log();

    // Open browser
    const opener = process.platform === 'win32' ? 'start' : process.platform === 'darwin' ? 'open' : 'xdg-open';
    require('child_process').exec(`${opener} "${authUrl}"`);

    // Start local server to receive callback
    const server = http.createServer(async (req, res) => {
      const url = new URL(req.url, 'http://localhost:3847');
      const code = url.searchParams.get('code');
      if (!code) {
        res.writeHead(400);
        res.end('No code received');
        return;
      }

      try {
        const { tokens } = await oAuth2Client.getToken(code);
        oAuth2Client.setCredentials(tokens);
        fs.writeFileSync(TOKEN_PATH, JSON.stringify(tokens));
        console.log('Authorization successful! Token saved.');
        res.writeHead(200, { 'Content-Type': 'text/html' });
        res.end('<h2>Authorization successful!</h2><p>You can close this window and return to the terminal.</p>');
        server.close();
        resolve(oAuth2Client);
      } catch (err) {
        res.writeHead(500);
        res.end('Token exchange failed: ' + err.message);
        server.close();
        reject(err);
      }
    });

    server.listen(3847, () => {
      console.log('Waiting for authorization callback on http://localhost:3847...');
    });
  });
}

async function fetchEmails(auth) {
  const gmail = google.gmail({ version: 'v1', auth });

  const allMode = process.argv.includes('--all');
  const daysIdx = process.argv.indexOf('--days');
  const daysBack = daysIdx !== -1 ? parseInt(process.argv[daysIdx + 1], 10) : 30;

  let query = `from:${SENDER} has:attachment`;
  if (!allMode) {
    const since = new Date();
    since.setDate(since.getDate() - daysBack);
    query += ` after:${since.toISOString().split('T')[0]}`;
  }

  console.log(`Searching: ${query}`);

  let allMessages = [];
  let pageToken = null;

  do {
    const res = await gmail.users.messages.list({
      userId: 'me',
      q: query,
      maxResults: 100,
      pageToken,
    });
    const messages = res.data.messages || [];
    allMessages = allMessages.concat(messages);
    pageToken = res.data.nextPageToken;
    if (pageToken) console.log(`  Found ${allMessages.length} emails so far, fetching more...`);
  } while (pageToken);

  console.log(`Found ${allMessages.length} emails total.`);

  let downloaded = 0, skipped = 0;

  for (let i = 0; i < allMessages.length; i++) {
    const msgId = allMessages[i].id;

    const msg = await gmail.users.messages.get({
      userId: 'me',
      id: msgId,
      format: 'full',
    });

    const parts = msg.data.payload.parts || [];
    for (const part of parts) {
      if (!part.filename || !part.filename.endsWith('.csv')) continue;
      if (!part.filename.includes(ATTACHMENT_PATTERN)) continue;

      if (existingFiles.has(part.filename)) {
        skipped++;
        continue;
      }

      // Download attachment
      const attId = part.body.attachmentId;
      if (!attId) continue;

      const att = await gmail.users.messages.attachments.get({
        userId: 'me',
        messageId: msgId,
        id: attId,
      });

      const data = Buffer.from(att.data.data, 'base64');
      const savePath = path.join(SAVE_DIR, part.filename);
      fs.writeFileSync(savePath, data);
      existingFiles.add(part.filename);
      downloaded++;
      console.log(`  Downloaded: ${part.filename} (${(data.length / 1024).toFixed(1)}KB)`);
    }

    // Progress
    if ((i + 1) % 20 === 0) console.log(`  Processed ${i + 1}/${allMessages.length} emails...`);
  }

  console.log(`\n=== Summary ===`);
  console.log(`Emails scanned: ${allMessages.length}`);
  console.log(`Attachments downloaded: ${downloaded}`);
  console.log(`Skipped (already exists): ${skipped}`);
}

async function main() {
  const auth = await authorize();
  await fetchEmails(auth);
}

main().catch(err => {
  console.error('Failed:', err.message);
  process.exit(1);
});
