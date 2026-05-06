import { getStore } from "@netlify/blobs";
import { SignJWT, importPKCS8 } from 'jose';

const SHEET_ID = '1VLp9gzDW1GhAHRGKic7aX3rRgUsmSQ2QUr2rZ5Wvu2Y';
const SHEET_TAB = 'Master Tab';
const SITE_URL = 'https://fairway-walkthrough.netlify.app';

async function getAccessToken() {
  const email = Netlify.env.get('GOOGLE_SERVICE_ACCOUNT_EMAIL');
  let rawKey = Netlify.env.get('GOOGLE_PRIVATE_KEY');
  rawKey = rawKey.replace(/\\\\n/g, '\n').replace(/\\n/g, '\n');
  const privateKey = await importPKCS8(rawKey, 'RS256');
  const now = Math.floor(Date.now() / 1000);
  const jwt = await new SignJWT({
    iss: email,
    scope: 'https://www.googleapis.com/auth/spreadsheets',
    aud: 'https://oauth2.googleapis.com/token',
    iat: now,
    exp: now + 3600,
  }).setProtectedHeader({ alg: 'RS256', typ: 'JWT' }).sign(privateKey);
  const resp = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: `grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=${jwt}`,
  });
  const data = await resp.json();
  if (!data.access_token) throw new Error('Auth failed');
  return data.access_token;
}

function findBestRow(sheetRows, photoAddr, photoBldg, photoCat) {
  // Priority 1: address + category (exact)
  for (let i = 2; i < sheetRows.length; i++) {
    const rowAddr = (sheetRows[i][2] || '').toString().trim();
    const rowCat = (sheetRows[i][6] || '').toString().trim().toLowerCase();
    if (photoAddr && rowAddr === photoAddr && photoCat && rowCat === photoCat) return i;
  }
  // Priority 2: address only
  for (let i = 2; i < sheetRows.length; i++) {
    const rowAddr = (sheetRows[i][2] || '').toString().trim();
    if (photoAddr && rowAddr === photoAddr) return i;
  }
  // Priority 3: building + category (only when no address)
  if (!photoAddr) {
    for (let i = 2; i < sheetRows.length; i++) {
      const rowBldg = (sheetRows[i][1] || '').toString().trim();
      const rowCat = (sheetRows[i][6] || '').toString().trim().toLowerCase();
      if (photoBldg && rowBldg === photoBldg && photoCat && rowCat === photoCat) return i;
    }
  }
  // Priority 4: building only (only when no address)
  if (!photoAddr) {
    for (let i = 2; i < sheetRows.length; i++) {
      const rowBldg = (sheetRows[i][1] || '').toString().trim();
      if (photoBldg && rowBldg === photoBldg) return i;
    }
  }
  return -1;
}

export default async (req, context) => {
  try {
    const token = await getAccessToken();

    // Step 1: Read all photo metadata
    const metaStore = getStore("photo-meta");
    const { blobs } = await metaStore.list();
    const photos = [];
    for (const b of blobs) {
      try {
        const m = await metaStore.get(b.key, { type: "json" });
        if (m) photos.push(m);
      } catch (e) {}
    }

    // Step 2: Read the sheet
    const range = encodeURIComponent(`'${SHEET_TAB}'!A:J`);
    const readUrl = `https://sheets.googleapis.com/v4/spreadsheets/${SHEET_ID}/values/${range}?majorDimension=ROWS`;
    const readResp = await fetch(readUrl, { headers: { 'Authorization': `Bearer ${token}` } });
    const readData = await readResp.json();
    if (!readData.values) return new Response(JSON.stringify({ error: 'No data in sheet' }), { status: 500, headers: { 'Content-Type': 'application/json' } });

    const sheetRows = readData.values;
    const totalRows = sheetRows.length;

    // Step 3: Clear column J (rows 3 onwards)
    const clearRange = `'${SHEET_TAB}'!J3:J${totalRows}`;
    await fetch(`https://sheets.googleapis.com/v4/spreadsheets/${SHEET_ID}/values/${encodeURIComponent(clearRange)}:clear`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    });

    // Step 4: Match each photo to a row and track which rows get photos
    const rowPhotos = {}; // rowIndex -> { addr, cat }
    let matched = 0, unmatched = 0;
    const unmatchedList = [];

    for (const photo of photos) {
      const photoAddr = (photo.address || '').toString().trim();
      const photoBldg = (photo.bldgNumber || '').toString().trim();
      const photoCat = (photo.category || '').toString().trim().toLowerCase();

      // If photo has rowNumber in metadata, use it directly
      if (photo.rowNumber && parseInt(photo.rowNumber) > 0) {
        const rowIdx = parseInt(photo.rowNumber) - 1;
        if (!rowPhotos[rowIdx]) rowPhotos[rowIdx] = { addr: photoAddr, cat: photo.category || '' };
        matched++;
        continue;
      }

      const bestRow = findBestRow(sheetRows, photoAddr, photoBldg, photoCat);
      if (bestRow >= 0) {
        if (!rowPhotos[bestRow]) rowPhotos[bestRow] = { addr: photoAddr, cat: photo.category || '' };
        matched++;
      } else {
        unmatched++;
        unmatchedList.push({ photoName: photo.photoName, address: photoAddr, bldg: photoBldg, category: photoCat });
      }
    }

    // Step 5: Write HYPERLINK for each row that has at least one photo
    const writes = [];
    for (const [rowIdx, info] of Object.entries(rowPhotos)) {
      const rowNum = parseInt(rowIdx) + 1;
      const addr = (info.addr || '').trim();
      const cat = (info.cat || '').trim();
      const params = new URLSearchParams();
      if (addr) params.set('address', addr);
      if (cat) params.set('category', cat);
      const galleryUrl = SITE_URL + '/photos.html?' + params.toString();
      const displayLabel = 'Photos' + (addr ? ' ' + addr : '') + (cat ? ' ' + cat : '');
      const cellValue = '=HYPERLINK("' + galleryUrl.replace(/"/g, '""') + '","' + displayLabel.replace(/"/g, '""') + '")';
      writes.push({ range: `'${SHEET_TAB}'!J${rowNum}`, values: [[cellValue]] });
    }

    if (writes.length > 0) {
      const batchResp = await fetch(`https://sheets.googleapis.com/v4/spreadsheets/${SHEET_ID}/values:batchUpdate`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ valueInputOption: 'USER_ENTERED', data: writes }),
      });
      const batchData = await batchResp.json();
      if (batchData.error) return new Response(JSON.stringify({ error: batchData.error }), { status: 500, headers: { 'Content-Type': 'application/json' } });
    }

    return new Response(JSON.stringify({
      success: true,
      totalPhotos: photos.length,
      matched,
      unmatched,
      rowsLinked: writes.length,
      unmatchedPhotos: unmatchedList,
    }), { headers: { 'Content-Type': 'application/json' } });
  } catch (err) {
    return new Response(JSON.stringify({ error: err.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
  }
};

export const config = { path: '/api/fix-photos' };
