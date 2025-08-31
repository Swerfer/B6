self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (e) => e.waitUntil(self.clients.claim()));

self.addEventListener("push", (event) => {
  if (!event.data) return;
  let payload = {};
  try { payload = event.data.json(); } catch { payload = { title: "B6 Missions", body: event.data.text() }; }
  const title = payload.title || "B6 Missions";
  const body  = payload.body  || "";
  const data  = payload.data  || {};
  const opts  = {
    body,
    tag: payload.tag || data.tag,
    renotify: true,
    data,
    badge: "/assets/icons/badge.png",
    icon:  "/assets/icons/icon-192.png"
  };
  event.waitUntil(self.registration.showNotification(title, opts));
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const url = (event.notification.data && event.notification.data.url) || "/game.html";
  event.waitUntil(
    clients.matchAll({ type: "window", includeUncontrolled: true }).then((list) => {
      for (const c of list) {
        if (c.url.includes(url) || c.url.includes("/game")) { c.focus(); return; }
      }
      clients.openWindow(url);
    })
  );
});
