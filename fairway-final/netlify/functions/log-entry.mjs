import { SignJWT, importPKCS8 } from 'jose';

const SHEET_ID = '1VLp9gzDW1GhAHRGKic7aX3rRgUsmSQ2QUr2rZ5Wvu2Y';
const SHEET_TAB = 'Master Tab';
const RANGE = `'${SHEET_TAB}'!A3:P`;

async function getAccessToken() {
  const email = Netlify.env.get('GOOGLE_SERVICE_ACCOUNT_EMAIL');
  const rawKey = Netlify.env.get('GOOGLE_PRIVATE_KEY').replace(/\\n/g, '\n');
  const privateKey = await importPKCS8(rawKey, 'RS256');
  const now = Math.floor(Date.now() / 1000);
  const jwt = await new SignJWT({
    iss: email,
    scope: 'https://www.googleapis.com/auth/spreadsheets',
    aud: 'https://oauth2.googleapis.com/token',
    iat: now,
    exp: now + 3600,
  })
    .setProtectedHeader({ alg: 'RS256', typ: 'JWT' })
    .sign(privateKey);

  const resp = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: `grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=${jwt}`,
  });
  const data = await resp.json();
  if (!data.access_token) throw new Error('Failed to get access token: ' + JSON.stringify(data));
  return data.access_token;
}

async function appendRow(accessToken, values) {
  const url = `https://sheets.googleapis.com/v4/spreadsheets/${SHEET_ID}/values/${encodeURIComponent(RANGE)}:append?valueInputOption=USER_ENTERED&insertDataOption=INSERT_ROWS`;
  const resp = await fetch(url, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      values: [values],
    }),
  });
  const data = await resp.json();
  if (data.error) throw new Error(JSON.stringify(data.error));
  return data;
}

export default async (req, context) => {
  if (req.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type' } });
  }

  if (req.method !== 'POST') {
    return new Response(JSON.stringify({ error: 'Method not allowed' }), { status: 405, headers: { 'Content-Type': 'application/json' } });
  }

  try {
    const body = await req.json();
    const {
      bldgNumber = '',
      address = '',
      specificLocation = '',
      reviewPerson = '',
      priority = '',
      category = '',
      operatingOrReserve = '',
      description = '',
    } = body;

    if (!description) {
      return new Response(JSON.stringify({ error: 'description is required' }), { status: 400, headers: { 'Content-Type': 'application/json' } });
    }

    // Row maps to columns A through P
    // A=Bldg, B=Address, C=Location, D=Reviewer, E=Priority, F=Category, G=Op/Res, H=Description, I-P=blank
    const row = [
      bldgNumber,
      address,
      specificLocation,
      reviewPerson,
      priority,
      category,
      operatingOrReserve,
      description,
      '', // I - Photos (filled later)
      '', // J - Quote 1
      '', // K - Quote 2
      '', // L - Quote 3
      '', // M - Contractor
      '', // N - Flw-up
      '', // O - Compl?
      '', // P - Auth Pymt
    ];

    const token = await getAccessToken();
    const result = await appendRow(token, row);

    return new Response(JSON.stringify({
      success: true,
      message: 'Row appended',
      updatedRange: result.updates?.updatedRange || 'unknown',
    }), {
      headers: { 'Content-Type': 'application/json' },
    });
  } catch (err) {
    return new Response(JSON.stringify({ error: err.message || 'Failed to append row' }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' },
    });
  }
};

export const config = { path: '/api/log-entry' };
