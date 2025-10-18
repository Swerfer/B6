/**********************************************************************
 hub.js — thin SignalR client for GameHub
  - One global connection (auto-reconnect).
  - Join/leave mission groups by lowercased address (and checksum form).
  - Register callbacks for MissionUpdated, StatusChanged, RoundResult.
**********************************************************************/

let connection = null;
let startPromise = null;
let currentGroups = new Set();

// External callbacks (no-ops until set from the app)
let onMissionUpdated = () => {};
let onStatusChanged  = () => {};
let onRoundResult    = () => {};

// Utils
const toLc = (s) => (s ? String(s).toLowerCase() : "");
const toCk = (lc) => {
  try { return ethers.utils.getAddress(lc); } catch { return null; }
};

function                ensureClient()              {
  if (!window.signalR) {
    throw new Error("SignalR client script not found (window.signalR missing).");
  }
}

export function         getConnection()             { return connection; }

export async function   startHub()                  {
  ensureClient();
  if (connection && (connection.state === signalR.HubConnectionState.Connected)) {
    return connection;
  }
  if (startPromise) return startPromise;

  connection = new signalR.HubConnectionBuilder()
    .withUrl("/api/hub/game")
    .withAutomaticReconnect()
    .build();

  // Wire server → client events
  connection.on("MissionUpdated", (addr) => {
    try { onMissionUpdated(addr); } catch (e) { console.error("MissionUpdated handler error:", e); }
  });

  connection.on("StatusChanged", (addr, newStatus) => {
    try { onStatusChanged(addr, newStatus); } catch (e) { console.error("StatusChanged handler error:", e); }
  });

  connection.on("RoundResult", (addr, round, winner, amountWei) => {
    try { onRoundResult(addr, round, winner, amountWei); } catch (e) { console.error("RoundResult handler error:", e); }
  });

  // Optional ping if your hub sends it; safe if not present
  connection.on("ServerPing", (msg) => {
    // no-op; left for debug parity with existing code
    // console.log("[hub] ping:", msg);
  });

  // Re-subscribe groups after reconnect
  connection.onreconnected(async () => {
    const groups = Array.from(currentGroups);
    for (const g of groups) {
      try { await connection.invoke("SubscribeMission", g); }
      catch (e) { console.error("[hub] resubscribe failed:", g, e); }
    }
  });

  startPromise = connection.start()
    .then(() => connection)
    .finally(() => { startPromise = null; });

  return startPromise;
}

export async function   stopHub()                   {
  if (!connection) return;
  try { await connection.stop(); } finally {
    connection = null;
    currentGroups.clear();
  }
}

export async function   joinMissionGroup(address)   {
  const lc = toLc(address);
  if (!lc) return;
  await startHub();

  const ck = toCk(lc);
  for (const g of [lc, ck].filter(Boolean)) {
    if (currentGroups.has(g)) continue;
    try {
      await connection.invoke("SubscribeMission", g);
      currentGroups.add(g);
    } catch (e) {
      console.error("[hub] SubscribeMission failed:", g, e);
    }
  }
}

export async function   leaveMissionGroup(address)  {
  const lc = toLc(address);
  if (!lc || !connection) return;

  const ck = toCk(lc);
  for (const g of [lc, ck].filter(Boolean)) {
    if (!currentGroups.has(g)) continue;
    try {
      await connection.invoke("UnsubscribeMission", g);
    } catch (e) {
      // non-fatal
    } finally {
      currentGroups.delete(g);
    }
  }
}

// Optional: leave all groups (e.g., on page unload)
export async function   leaveAllGroups()            {
  if (!connection) { currentGroups.clear(); return; }
  for (const g of Array.from(currentGroups)) {
    try { await connection.invoke("UnsubscribeMission", g); } catch {}
  }
  currentGroups.clear();
}

// App wires its own handlers here
export function         setHandlers({ onMissionUpdated: MU, onStatusChanged: SC, onRoundResult: RR } = {}) {
  if (typeof MU === "function") onMissionUpdated = MU;
  if (typeof SC === "function") onStatusChanged  = SC;
  if (typeof RR === "function") onRoundResult    = RR;
}

// For quick debugging from console
window.__hub = {
  startHub, stopHub, joinMissionGroup, leaveMissionGroup, leaveAllGroups,
  setHandlers, getConnection: () => connection,
};
