// Minimal Web Push bootstrap: register SW, fetch VAPID public key, subscribe, upsert on the server.
// Exposes window.enableGamePush(addressOrNull)

const api = (p, opt) => fetch(`/api${p}`, { credentials: "include", ...opt });

function        urlBase64ToUint8Array(base64String) {
  const padding = "=".repeat((4 - base64String.length % 4) % 4);
  const base64 = (base64String + padding).replace(/-/g, "+").replace(/_/g, "/");
  const rawData = atob(base64);
  const outputArray = new Uint8Array(rawData.length);
  for (let i = 0; i < rawData.length; ++i) outputArray[i] = rawData.charCodeAt(i);
  return outputArray;
}

async function  ensureSw() {
  if (!("serviceWorker" in navigator)) throw new Error("No SW");
  const reg = await navigator.serviceWorker.register("/js/sw.js");
  return reg;
}

async function  getVapid() {
  const r   = await api("/push/vapid-public-key");
  let  txt  = (await r.text()).trim();
  // If backend ever returns JSON string, parse once
  if (txt.startsWith('"') && txt.endsWith('"')) {
    try { txt = JSON.parse(txt); } catch {}
  }
  // remove any stray whitespace
  return txt.replace(/\s+/g, "");
}

async function  subscribe(reg) {
  const vapid = await getVapid();
  const sub = await reg.pushManager.subscribe({
    userVisibleOnly: true,
    applicationServerKey: urlBase64ToUint8Array(vapid)
  });
  return sub;
}

async function  upsert(address, sub) {
    // Preserve the exact base64 the browser produces; no extra URL-safe transform here.
    const bytesToB64 = (ab) => {
        const arr = new Uint8Array(ab);
        let s = "";
        for (let i = 0; i < arr.length; i++) s += String.fromCharCode(arr[i]);
        return btoa(s); // plain base64
        };

        const p256 = bytesToB64(sub.getKey("p256dh"));
        const auth = bytesToB64(sub.getKey("auth"));

        const body = {
        Address:   (address || "").toLowerCase(),
        Endpoint:  sub.endpoint,
        P256dh:    p256,   // send as-is
        Auth:      auth,   // send as-is
        UserAgent: navigator.userAgent,
        Locale:    navigator.language,
        Timezone:  Intl.DateTimeFormat().resolvedOptions().timeZone
    };

    await api("/push/subscribe", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body)
    });
}

async function  enableGamePush(address) {
  try {
    if (!address) return;
    if (!("Notification" in window)) return;
    if (Notification.permission === "denied") return;
    if (Notification.permission === "default") {
      const perm = await Notification.requestPermission();
      if (perm !== "granted") return;
    }
    const reg = await ensureSw();
    const existing = await reg.pushManager.getSubscription();
    console.log('[push] endpoint', sub?.endpoint);  
    let sub = existing;

    // Force a fresh subscription once after you deploy this fix.
    // (You can remove this block after confirming notifications work.)
    if (!sub) {
        sub = await subscribe(reg);
    }

    await upsert(address, sub);
  } catch (e) {
    console.warn("[push]", e?.message || e);
  }
}

window.enableGamePush = enableGamePush;
export { enableGamePush };
