self.addEventListener("install", () => { console.log("[sw] install v1291"); self.skipWaiting(); });
self.addEventListener("activate", (e) => { console.log("[sw] activate v1291"); e.waitUntil(self.clients.claim()); });

self.addEventListener("push", (event) => {
  let payload = {};
  if (event.data) {
      try {
      payload = event.data.json();
      } catch {
      // Some push services deliver text instead of JSON
      payload = { title: "B6 Missions", body: event.data.text() };
      }
  } else {
      // Fallback for “no payload” pushes (seen on some Windows/Edge deliveries)
      payload = {
      title: "B6 Missions",
      body:  "You have a new update. Tap to open.",
      data:  { url: "/game.html" },
      tag:   "b6-generic"
      };
  }

  const title = payload.title || "B6 Missions";
  const body  = payload.body  || "";
  const data  = payload.data  || {};
  const opts  = {
      body,
      tag: payload.tag || data.tag || "b6-generic",
      renotify: true,
      data,
      badge: "/assets/icons/badge.png",
      icon:  "/assets/icons/icon-192.png"
  };

  console.log('[sw] push event', event && event.data ? 'with data' : 'no data');
    event.waitUntil(self.registration.showNotification(title, opts).catch(err => {
    console.error('[sw] showNotification failed:', err);
  }));

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
