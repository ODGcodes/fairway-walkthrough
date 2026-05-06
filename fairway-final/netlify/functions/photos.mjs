
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
  let addrCatMatch = -1;
  let addrOnlyMatch = -1;
  let bldgCatMatch = -1;
  let bldgOnlyMatch = -1;

  for (let i = 2; i < sheetRows.length; i++) {
    const row = sheetRows[i];
    const rowBldg = (row[1] || '').toString().trim();
    const rowAddr = (row[2] || '').toString().trim();
    const rowCat = (row[6] || '').toString().trim().toLowerCase();

    const addrMatch = photoAddr && rowAddr && rowAddr === photoAddr;
    const bldgMatch = photoBldg && rowBldg && rowBldg === photoBldg;
    const catMatch = photoCat && rowCat && rowCat === photoCat;

    if (addrMatch && catMatch) { addrCatMatch = i; break; }
    if (addrMatch && addrOnlyMatch < 0) addrOnlyMatch = i;
    if (!photoAddr && bldgMatch && catMatch && bldgCatMatch < 0) bldgCatMatch = i;
    if (!photoAddr && bldgMatch && bldgOnlyMatch < 0) bldgOnlyMatch = i;
  }

  if (addrCatMatch >= 0) return addrCatMatch;
  if (addrOnlyMatch >= 0) return addrOnlyMatch;
  if (bldgCatMatch >= 0) return bldgCatMatch;
  if (bldgOnlyMatch >= 0) return bldgOnlyMatch;
  return -1;
}

export default async (req, context) => {
  try {
    const token = await getAccessToken();

    // Step 1: Read all photo metadata from blob store
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

    // Step 3: Clear ALL of column J (rows 3 onwards, since row 1=title, row 2=headers)
    const clearRange = `'${SHEET_TAB}'!J3:J${totalRows}`;
    const clearUrl = `https://sheets.googleapis.com/v4/spreadsheets/${SHEET_ID}/values/${encodeURIComponent(clearRange)}:clear`;
    await fetch(clearUrl, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    });

    // Step 4: For each sheet row, find all matching photos and build a gallery link
    // Group photos by their best matching row
    const rowPhotos = {}; // rowIndex -> { addresses: Set, categories: Set, count: number }

    let matched = 0;
    let unmatched = 0;
    const unmatchedList = [];

    for (const photo of photos) {
      const photoAddr = (photo.address || '').toString().trim();
      const photoBldg = (photo.bldgNumber || '').toString().trim();
      const photoCat = (photo.category || '').toString().trim().toLowerCase();

      const bestRow = findBestRow(sheetRows, photoAddr, photoBldg, photoCat);

      if (bestRow >= 0) {
        matched++;
        if (!rowPhotos[bestRow]) rowPhotos[bestRow] = { addresses: new Set(), categories: new Set(), count: 0 };
        if (photoAddr) rowPhotos[bestRow].addresses.add(photoAddr);
        if (photo.category) rowPhotos[bestRow].categories.add(photo.category);
        rowPhotos[bestRow].count++;
      } else {
        unmatched++;
        unmatchedList.push({ photoName: photo.photoName, address: photoAddr, bldg: photoBldg, category: photoCat });
      }
    }

    // Step 5: Write HYPERLINK formulas for each matched row
    const writes = [];
    for (const [rowIdx, info] of Object.entries(rowPhotos)) {
      const rowNum = parseInt(rowIdx) + 1;
      const addr = Array.from(info.addresses).join(', ');
      const cat = Array.from(info.categories).join(', ');

      // Build gallery link with filter params
      const params = new URLSearchParams();
      if (info.addresses.size === 1) params.set('address', Array.from(info.addresses)[0]);
      if (info.categories.size === 1) params.set('category', Array.from(info.categories)[0]);
      const galleryUrl = SITE_URL + '/photos.html?' + params.toString();

      const displayLabel = 'Photos' + (addr ? ' ' + addr : '') + (cat ? ' ' + cat : '');
      const cellValue = '=HYPERLINK("' + galleryUrl.replace(/"/g, '""') + '","' + displayLabel.replace(/"/g, '""') + '")';

      writes.push({
        range: `'${SHEET_TAB}'!J${rowNum}`,
        values: [[cellValue]],
      });
    }

    // Batch write all HYPERLINK formulas
    if (writes.length > 0) {
      const batchUrl = `https://sheets.googleapis.com/v4/spreadsheets/${SHEET_ID}/values:batchUpdate`;
      const batchResp = await fetch(batchUrl, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          valueInputOption: 'USER_ENTERED',
          data: writes,
        }),
      });
      const batchData = await batchResp.json();
      if (batchData.error) {
        return new Response(JSON.stringify({ error: batchData.error, matched, unmatched }), { status: 500, headers: { 'Content-Type': 'application/json' } });
      }
    }

    return new Response(JSON.stringify({
      success: true,
      totalPhotos: photos.length,
      matched,
      unmatched,
      rowsWithPhotos: writes.length,
      unmatchedPhotos: unmatchedList,
    }), { headers: { 'Content-Type': 'application/json' } });

  } catch (err) {
    return new Response(JSON.stringify({ error: err.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
  }
};

export const config = { path: '/api/fix-photos' };
