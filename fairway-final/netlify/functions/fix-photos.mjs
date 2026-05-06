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

    // Step 4: Sort ALL photos by timestamp
    photos.sort((a, b) => {
      const ta = a.timestamp || '';
      const tb = b.timestamp || '';
      return ta.localeCompare(tb);
    });

    // Step 5: Group consecutive photos with same address+category into "sets"
    // Each set = one entry's photos
    const photoSets = [];
    let currentSet = null;

    for (const photo of photos) {
      const addr = (photo.address || '').toString().trim();
      const cat = (photo.category || '').toString().trim().toLowerCase();
      const bldg = (photo.bldgNumber || '').toString().trim();
      const key = (addr || 'bldg:' + bldg) + '|' + cat;

      if (currentSet && currentSet.key === key) {
        // Same entry, add to current set
        currentSet.photos.push(photo);
      } else {
        // New entry
        if (currentSet) photoSets.push(currentSet);
        currentSet = {
          key,
          addr,
          bldg,
          cat,
          catOriginal: (photo.category || '').toString().trim(),
          photos: [photo],
        };
      }
    }
    if (currentSet) photoSets.push(currentSet);

    // Step 6: Build index of sheet rows by address+category
    // For each address+category combo, maintain an ordered list of row indices
    // and a cursor tracking which row to assign next
    const rowIndex = {}; // "addr|cat" -> { rows: [rowIdx, ...], cursor: 0 }

    for (let i = 2; i < sheetRows.length; i++) {
      const row = sheetRows[i];
      const rowAddr = (row[2] || '').toString().trim();
      const rowBldg = (row[1] || '').toString().trim();
      const rowCat = (row[6] || '').toString().trim().toLowerCase();

      // Index by address+category
      if (rowAddr && rowCat) {
        const key = rowAddr + '|' + rowCat;
        if (!rowIndex[key]) rowIndex[key] = { rows: [], cursor: 0 };
        rowIndex[key].rows.push(i);
      }
      // Also index by bldg+category for rows without address
      if (!rowAddr && rowBldg && rowCat) {
        const key = 'bldg:' + rowBldg + '|' + rowCat;
        if (!rowIndex[key]) rowIndex[key] = { rows: [], cursor: 0 };
        rowIndex[key].rows.push(i);
      }
    }

    // Step 7: Walk through photo sets in timestamp order
    // Assign each set to the next available row with matching address+category
    const rowAssignments = {}; // rowIdx -> { addr, cat, photoCount }
    const unmatchedSets = [];

    for (const set of photoSets) {
      const addrKey = set.addr + '|' + set.cat;
      const bldgKey = 'bldg:' + set.bldg + '|' + set.cat;

      let assigned = false;

      // Try address+category match first
      if (set.addr && rowIndex[addrKey] && rowIndex[addrKey].cursor < rowIndex[addrKey].rows.length) {
        const rowIdx = rowIndex[addrKey].rows[rowIndex[addrKey].cursor];
        rowIndex[addrKey].cursor++;
        rowAssignments[rowIdx] = {
          addr: set.addr,
          cat: set.catOriginal,
          photoCount: set.photos.length,
          photoNames: set.photos.map(p => p.photoName),
        };
        assigned = true;
      }
      // Try bldg+category match (only when no address)
      else if (!set.addr && set.bldg && rowIndex[bldgKey] && rowIndex[bldgKey].cursor < rowIndex[bldgKey].rows.length) {
        const rowIdx = rowIndex[bldgKey].rows[rowIndex[bldgKey].cursor];
        rowIndex[bldgKey].cursor++;
        rowAssignments[rowIdx] = {
          addr: set.addr,
          cat: set.catOriginal,
          bldg: set.bldg,
          photoCount: set.photos.length,
          photoNames: set.photos.map(p => p.photoName),
        };
        assigned = true;
      }

      if (!assigned) {
        unmatchedSets.push({
          addr: set.addr,
          bldg: set.bldg,
          cat: set.catOriginal,
          photoCount: set.photos.length,
          photoNames: set.photos.map(p => p.photoName),
        });
      }
    }

    // Step 8: Write HYPERLINK formulas for each assigned row
    const writes = [];
    const linkedRows = [];

    for (const [rowIdx, info] of Object.entries(rowAssignments)) {
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
      linkedRows.push({
        row: rowNum,
        addr: info.addr,
        cat: info.cat,
        photoCount: info.photoCount,
        desc: (sheetRows[parseInt(rowIdx)][8] || '').substring(0, 50),
      });
    }

    // Find unlinked rows (rows with no photos assigned)
    const unlinkedRows = [];
    for (let i = 2; i < sheetRows.length; i++) {
      if (!rowAssignments[i]) {
        const row = sheetRows[i];
        unlinkedRows.push({
          row: i + 1,
          addr: (row[2] || '').toString().trim(),
          bldg: (row[1] || '').toString().trim(),
          cat: (row[6] || '').toString().trim(),
          desc: (row[8] || '').toString().trim().substring(0, 50),
        });
      }
    }

    // Batch write
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
      photoSets: photoSets.length,
      totalSheetRows: sheetRows.length - 2,
      rowsLinked: linkedRows.length,
      rowsUnlinked: unlinkedRows.length,
      unmatchedPhotoSets: unmatchedSets.length,
      linkedRows,
      unlinkedRows,
      unmatchedSets,
    }), { headers: { 'Content-Type': 'application/json' } });
  } catch (err) {
    return new Response(JSON.stringify({ error: err.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
  }
};

export const config = { path: '/api/fix-photos' };
