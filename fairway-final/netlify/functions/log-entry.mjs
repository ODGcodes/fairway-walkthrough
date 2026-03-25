import { SignJWT, importPKCS8 } from 'jose';

const SHEET_ID = '1VLp9gzDW1GhAHRGKic7aX3rRgUsmSQ2QUr2rZ5Wvu2Y';
const SHEET_TAB = 'Master Tab';
const RANGE = `'${SHEET_TAB}'!A3:P`;

async function getAccessToken() {
  const email = Netlify.env.get('GOOGLE_SERVICE_ACCOUNT_EMAIL');
  let rawKey = Netlify.env.get('GOOGLE_PRIVATE_KEY');
  // Handle double-escaped or literal \n in the key
  rawKey = rawKey.replace(/\\\\n/g, '\n').replace(/\\n/g, '\n');
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
  // First, find the next empty row by checking column A
  const findUrl = `https://sheets.googleapis.com/v4/spreadsheets/${SHEET_ID}/values/${encodeURIComponent("'" + SHEET_TAB + "'!A:A")}?majorDimension=COLUMNS`;
  const findResp = await fetch(findUrl, {
    headers: { 'Authorization': `Bearer ${accessToken}` },
  });
  const findData = await findResp.json();
  // Next empty row = length of column A values + 1 (but at least row 3)
  const lastRow = findData.values && findData.values[0] ? findData.values[0].length : 2;
  const nextRow = Math.max(lastRow + 1, 3);

  // Write directly to the exact row using update (not append)
  const writeRange = `'${SHEET_TAB}'!A${nextRow}:P${nextRow}`;
  const url = `https://sheets.googleapis.com/v4/spreadsheets/${SHEET_ID}/values/${encodeURIComponent(writeRange)}?valueInputOption=USER_ENTERED`;
  const resp = await fetch(url, {
    method: 'PUT',
    headers: {
      'Authorization': `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      range: writeRange,
      majorDimension: 'ROWS',
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

    // Vapi sends tool calls in various formats — check all possible paths
    let args = body;
    let toolCallId = null;
    if (body.message && body.message.toolCallList) {
      const call = body.message.toolCallList[0];
      toolCallId = call.id;
      // Arguments can be in call.arguments OR call.function.arguments
      args = call.arguments || call.function?.arguments || {};
      if (typeof args === 'string') args = JSON.parse(args);
    } else if (body.message && body.message.toolWithToolCallList) {
      // Another Vapi format — toolWithToolCallList[0].toolCall.function.parameters
      const item = body.message.toolWithToolCallList[0];
      toolCallId = item.toolCall?.id;
      args = item.toolCall?.function?.parameters || item.toolCall?.function?.arguments || {};
      if (typeof args === 'string') args = JSON.parse(args);
    } else if (body.message && body.message.toolCalls) {
      const call = body.message.toolCalls[0];
      toolCallId = call.id;
      args = call.function?.arguments || call.arguments || {};
      if (typeof args === 'string') args = JSON.parse(args);
    } else if (body.message && body.message.functionCall) {
      toolCallId = body.message.functionCall.id;
      args = body.message.functionCall.parameters || {};
      if (typeof args === 'string') args = JSON.parse(args);
    }

    const {
      bldgNumber = '',
      address = '',
      specificLocation = '',
      reviewPerson = '',
      priority = '',
      category = '',
      operatingOrReserve = '',
      description = '',
    } = args;

    if (!description) {
      const errResp = { results: [{ toolCallId: toolCallId || 'unknown', result: 'Error: description is required' }] };
      return new Response(JSON.stringify(errResp), { status: 200, headers: { 'Content-Type': 'application/json' } });
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

    // Return in Vapi's expected format
    const response = toolCallId
      ? { results: [{ toolCallId, result: 'Row logged successfully' }] }
      : { success: true, message: 'Row appended', updatedRange: result.updates?.updatedRange || 'unknown' };

    return new Response(JSON.stringify(response), {
      headers: { 'Content-Type': 'application/json' },
    });
  } catch (err) {
    return new Response(JSON.stringify({ results: [{ toolCallId: 'unknown', result: 'Error: ' + (err.message || 'Failed to append row') }] }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    });
  }
};

export const config = { path: '/api/log-entry' };
