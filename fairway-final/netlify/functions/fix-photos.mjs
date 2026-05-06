
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

    // Step 4: Build a set of all photo address+category combinations
    // Also build a set of all photo bldg+category combinations (for photos with no address)
    const photoAddrCats = new Map(); // "addr|cat" -> { addr, cat, count }
    const photoBldgCats = new Map(); // "bldg|cat" -> { bldg, cat, count } (only when no addr)

    for (const photo of photos) {
      const addr = (photo.address || '').toString().trim();
      const bldg = (photo.bldgNumber || '').toString().trim();
      const cat = (photo.category || '').toString().trim();

      if (addr && cat) {
        const key = addr + '|' + cat.toLowerCase();
        if (!photoAddrCats.has(key)) photoAddrCats.set(key, { addr, cat, count: 0 });
        photoAddrCats.get(key).count++;
      } else if (addr) {
        const key = addr + '|';
        if (!photoAddrCats.has(key)) photoAddrCats.set(key, { addr, cat: '', count: 0 });
        photoAddrCats.get(key).count++;
      } else if (bldg && cat) {
        const key = bldg + '|' + cat.toLowerCase();
        if (!photoBldgCats.has(key)) photoBldgCats.set(key, { bldg, cat, count: 0 });
        photoBldgCats.get(key).count++;
      }
    }

    // Step 5: For each sheet row, check if there are photos for its address+category
    // If yes, write a gallery HYPERLINK to column J
    const writes = [];
    const linkedRows = [];
    const unlinkedRows = [];

    for (let i = 2; i < sheetRows.length; i++) {
      const row = sheetRows[i];
      const rowAddr = (row[2] || '').toString().trim();
      const rowBldg = (row[1] || '').toString().trim();
      const rowCat = (row[6] || '').toString().trim();
      const rowDesc = (row[8] || '').toString().trim();
      const rowNum = i + 1;

      let matchAddr = '';
      let matchCat = '';
      let found = false;

      // Priority 1: exact address + category match
      if (rowAddr && rowCat) {
        const key = rowAddr + '|' + rowCat.toLowerCase();
        if (photoAddrCats.has(key)) {
          const info = photoAddrCats.get(key);
          matchAddr = info.addr;
          matchCat = info.cat;
          found = true;
        }
      }

      // Priority 2: address only match (any category)
      if (!found && rowAddr) {
        for (const [key, info] of photoAddrCats) {
          if (info.addr === rowAddr) {
            matchAddr = info.addr;
            matchCat = info.cat;
            found = true;
            break;
          }
        }
      }

      // Priority 3: building + category match (only when row has no address)
      if (!found && !rowAddr && rowBldg && rowCat) {
        const key = rowBldg + '|' + rowCat.toLowerCase();
        if (photoBldgCats.has(key)) {
          const info = photoBldgCats.get(key);
          matchCat = info.cat;
          found = true;
        }
      }

      if (found) {
        const params = new URLSearchParams();
        if (matchAddr) params.set('address', matchAddr);
        if (matchCat) params.set('category', matchCat);
        const galleryUrl = SITE_URL + '/photos.html?' + params.toString();
        const displayLabel = 'Photos' + (matchAddr ? ' ' + matchAddr : '') + (matchCat ? ' ' + matchCat : '');
        const cellValue = '=HYPERLINK("' + galleryUrl.replace(/"/g, '""') + '","' + displayLabel.replace(/"/g, '""') + '")';

        writes.push({ range: `'${SHEET_TAB}'!J${rowNum}`, values: [[cellValue]] });
        linkedRows.push({ row: rowNum, addr: rowAddr, cat: rowCat, desc: rowDesc.substring(0, 40) });
      } else {
        unlinkedRows.push({ row: rowNum, addr: rowAddr, bldg: rowBldg, cat: rowCat, desc: rowDesc.substring(0, 40) });
      }
    }

    // Step 6: Batch write all HYPERLINKs
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
      totalSheetRows: sheetRows.length - 2,
      rowsLinked: linkedRows.length,
      rowsWithNoPhotos: unlinkedRows.length,
      linkedRows,
      unlinkedRows,
    }), { headers: { 'Content-Type': 'application/json' } });
  } catch (err) {
    return new Response(JSON.stringify({ error: err.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
  }
};

export const config = { path: '/api/fix-photos' };
