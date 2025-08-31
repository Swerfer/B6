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
  const body = {
    Address: (address || "").toLowerCase(),
    Endpoint: sub.endpoint,
    P256dh: btoa(String.fromCharCode(...new Uint8Array(sub.getKey("p256dh")))),
    Auth:    btoa(String.fromCharCode(...new Uint8Array(sub.getKey("auth")))),
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
    const sub = existing || await subscribe(reg);
    await upsert(address, sub);
  } catch (e) {
    console.warn("[push]", e?.message || e);
  }
}

window.enableGamePush = enableGamePush;
export { enableGamePush };
