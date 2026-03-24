import { getStore } from "@netlify/blobs";

export default async (req, context) => {
  if (req.method === "GET") {
    const url = new URL(req.url);
    const key = url.searchParams.get("key");
    const store = getStore("photos");
    const metaStore = getStore("photo-meta");

    if (key) {
      const blob = await store.get(key, { type: "arrayBuffer" });
      if (!blob) return new Response("Not found", { status: 404 });
      let mime = "image/jpeg";
      try {
        const m = await metaStore.get(key, { type: "json" });
        if (m && m.mimeType) mime = m.mimeType;
      } catch (e) {}
      return new Response(blob, { headers: { "Content-Type": mime, "Cache-Control": "public, max-age=86400" } });
    }

    const { blobs } = await metaStore.list();
    const photos = [];
    for (const b of blobs) {
      try {
        const m = await metaStore.get(b.key, { type: "json" });
        photos.push({ key: b.key, ...m });
      } catch (e) {}
    }
    return new Response(JSON.stringify({ photos }), { headers: { "Content-Type": "application/json" } });
  }

  if (req.method === "POST") {
    try {
      const body = await req.json();
      const { photoName, fileBase64, fileMimeType, address, bldgNumber, category, description, reviewer, timestamp } = body;
      if (!fileBase64 || !photoName) {
        return new Response(JSON.stringify({ error: "Missing fileBase64 or photoName" }), { status: 400, headers: { "Content-Type": "application/json" } });
      }
      const binary = Uint8Array.from(atob(fileBase64), c => c.charCodeAt(0));
      const store = getStore("photos");
      await store.set(photoName, binary);
      const photoUrl = "https://fairway-walkthrough.netlify.app/api/photos?key=" + encodeURIComponent(photoName);
      const metaStore = getStore("photo-meta");
      await metaStore.setJSON(photoName, {
        photoName, mimeType: fileMimeType || "image/jpeg",
        address: address || "", bldgNumber: bldgNumber || "",
        category: category || "", description: description || "",
        reviewer: reviewer || "", timestamp: timestamp || new Date().toISOString(),
        url: photoUrl
      });

      // Send metadata + photo URL to Make webhook for Google Sheets logging
      const makeWebhookUrl = Netlify.env.get("MAKE_PHOTO_WEBHOOK_URL");
      if (makeWebhookUrl) {
        try {
          await fetch(makeWebhookUrl, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              photoName, photoUrl,
              address: address || "", bldgNumber: bldgNumber || "",
              category: category || "", description: description || "",
              reviewer: reviewer || "", timestamp: timestamp || new Date().toISOString()
            })
          });
        } catch (e) { /* Don't fail the upload if Make webhook fails */ }
      }

      return new Response(JSON.stringify({ success: true, photoName, url: photoUrl }), { headers: { "Content-Type": "application/json" } });
    } catch (err) {
      return new Response(JSON.stringify({ error: err.message || "Upload failed" }), { status: 500, headers: { "Content-Type": "application/json" } });
    }
  }

  if (req.method === "DELETE") {
    const url = new URL(req.url);
    const key = url.searchParams.get("key");
    if (!key) {
      return new Response(JSON.stringify({ error: "Missing ?key= parameter" }), { status: 400, headers: { "Content-Type": "application/json" } });
    }
    const store = getStore("photos");
    const metaStore = getStore("photo-meta");
    await store.delete(key);
    await metaStore.delete(key);
    return new Response(JSON.stringify({ success: true, deleted: key }), { headers: { "Content-Type": "application/json" } });
  }

  return new Response("Method not allowed", { status: 405 });
};

export const config = { path: "/api/photos" };
