/* js/home.js */
import {
  FACTORY_ABI, FACTORY_ADDRESS, getReadProvider,
  formatCountdown, addrLinkIcon, shorten
} from "./core.js";

const els = {
  nextStart:       document.getElementById("nextStart"),
  statJoinable:    document.getElementById("statJoinable"),
  statActive:      document.getElementById("statActive"),
  statTotal:       document.getElementById("statTotal"),
  factoryAddress:  document.getElementById("factoryAddress"),
  implAddress:     document.getElementById("implAddress"),
  refreshBtn:      document.getElementById("refreshBtn"),
  btnTutorial:         document.getElementById("btnTutorial"),
  tutorialOverlay:     document.getElementById("tutorialOverlay"),
  tutorialClose:       document.getElementById("tutorialClose"),
  tutorialCloseBottom: document.getElementById("tutorialCloseBottom"),
};

let ticker = null;
function setCountdown(ts){
  if (ticker) { clearInterval(ticker); ticker = null; }
  if (!ts) { els.nextStart.textContent = "—"; return; }
  const tick = () => els.nextStart.textContent = formatCountdown(ts);
  tick();
  ticker = setInterval(tick, 1000);
}

async function api(path){
  const r = await fetch(`/api${path}`, { cache:"no-store" });
  if (!r.ok) throw new Error(`${path}: ${r.status}`);
  return r.json();
}

async function loadBackendStats(){
    // indexer summaries
    const [joinable, notEnded] = await Promise.all([
    api("/missions/joinable"),
    api("/missions/not-ended")
    ]);

    const joinableList = Array.isArray(joinable)
    ? joinable
    : (joinable?.missions || joinable?.Missions || []);

    const notEndedList = Array.isArray(notEnded)
    ? notEnded
    : (notEnded?.missions || notEnded?.Missions || []);

    els.statJoinable.textContent = String(joinableList.length);

    // status 3 = Active
    const activeCount = notEndedList.filter(m => Number(m.status ?? m.mission_status) === 3).length;
    els.statActive.textContent = String(activeCount);

    // soonest future start
    const now = Math.floor(Date.now()/1000);
    let soonest = 0;
    let soonestAddr = null;

    // prefer actual upcoming mission starts from not-ended
    for (const m of notEndedList){
      const start = Number(m.mission_start ?? m.MissionStart ?? 0);
      if (start > now && (!soonest || start < soonest)) {
        soonest = start;
        // try common field names for the address
        soonestAddr = m.mission_address ?? m.address ?? null;
      }
    }

    // fallback: use joinable missions' mission_start when not-ended gave none
    if (!soonest) {
      for (const m of joinableList){
        const start = Number(m.mission_start ?? m.MissionStart ?? 0);
        if (start > now && (!soonest || start < soonest)) {
          soonest = start;
          soonestAddr = m.mission_address ?? m.address ?? null;
        }
      }
    }

    setCountdown(soonest || 0);

    // Make countdown pill clickable if we found a mission address
    const pill = document.querySelector(".countdown-pill");
    if (pill) {
      if (soonestAddr) {
        pill.style.cursor = "pointer";
        pill.onclick = () => { window.location.href = `game.html?mission=${soonestAddr}`; };
      } else {
        pill.style.cursor = "";
        pill.onclick = null;
      }
    }

}

async function loadOnchainStats(){
  // core.js loads /api/config and sets FACTORY_ADDRESS; allow a tiny delay
  if (!FACTORY_ADDRESS) await new Promise(r=>setTimeout(r,200));
  if (!FACTORY_ADDRESS) return;

  const provider = getReadProvider();
  const factory  = new ethers.Contract(FACTORY_ADDRESS, FACTORY_ABI, provider);

  let total = 0, impl = null;
  try { total = Number(await factory.getTotalMissions()); } catch {}
  try { impl  = await factory.missionImplementation();    } catch {}

  els.statTotal.textContent = String(total || 0);

  // render short, copyable addr (icon removed)
  els.factoryAddress.innerHTML =
    `<span class="copy-wrap" data-copy="${FACTORY_ADDRESS}">${shorten(FACTORY_ADDRESS)}</span>`;
  // whole row clickable
  els.factoryAddress.closest(".stat")?.addEventListener("click", () => {
    window.open(`https://explorer.cronos.org/address/${FACTORY_ADDRESS}`, "_blank");
  });

  if (impl){
    els.implAddress.innerHTML =
      `<span class="copy-wrap" data-copy="${impl}">${shorten(impl)}</span>`;
    els.implAddress.closest(".stat")?.addEventListener("click", () => {
      window.open(`https://explorer.cronos.org/address/${impl}`, "_blank");
    });
  }

}

async function refreshAll(){
  try {
    await Promise.all([loadBackendStats(), loadOnchainStats()]);
  } catch (err){
    console.error(err);
  }
}

els.refreshBtn?.addEventListener("click", refreshAll);
refreshAll();
setInterval(refreshAll, 15000);

// Make homepage stats clickable with hand cursor
const statTotal = document.getElementById("statTotal")?.closest(".stat");
if (statTotal) {
  statTotal.style.cursor = "pointer";
  statTotal.addEventListener("click", () => { window.location.href = "game.html?view=all"; });
}

const statJoinable = document.getElementById("statJoinable")?.closest(".stat");
if (statJoinable) {
  statJoinable.style.cursor = "pointer";
  statJoinable.addEventListener("click", () => { window.location.href = "game.html?view=joinable"; });
}

const statActive = document.getElementById("statActive")?.closest(".stat");
if (statActive) {
  statActive.style.cursor = "pointer";
  statActive.addEventListener("click", () => { window.location.href = "game.html?view=active"; });
}

// ===== Overlays (Tutorial & FAQ) — unified wiring =====

// helper: lock page scroll while an overlay is open
function lockScroll(){
  if (!document.body.dataset.prevOverflow) {
    document.body.dataset.prevOverflow = document.body.style.overflow || "";
  }
  document.body.style.overflow = "hidden";
}

// helper: open an overlay by id (closes others first)
function openOverlayById(overlayId){
  const ov = document.getElementById(overlayId);
  if (!ov) return;

  // close all other overlays first
  document.querySelectorAll(".tutorial-overlay,.faq-overlay,.pt-overlay,.modal-overlay").forEach(el=>{
    el.classList.remove("open","hidden");
    el.style.display = "none";
  });

  // show and mark as open (CSS controls display for .open)
  ov.style.display = "";
  ov.classList.add("open");
  lockScroll();
}

// map buttons to overlays and close buttons
const overlayDefs = [
  { btnId: "btnTutorial",              overlayId: "tutorialOverlay",         closeIds: ["tutorialClose"] },
  { btnId: "btnFaq",                   overlayId: "faqOverlay",              closeIds: ["faqClose"] },
  { btnId: "btnPrivacyPolicyAndTerms", overlayId: "ptOverlay",               closeIds: ["ptClose"] }
];

// wire all overlays in one pass
overlayDefs.forEach(({btnId, overlayId, closeIds})=>{
  const btn = document.getElementById(btnId);
  const ov  = document.getElementById(overlayId);
  if (!ov) return;

  // open
  btn?.addEventListener("click", () => openOverlayById(overlayId));

  // close via X buttons
  (closeIds || []).forEach(id => document.getElementById(id)?.addEventListener("click", goHome));

  // close by clicking backdrop
  ov.addEventListener("click", (e) => { if (e.target === ov) goHome(); });
});

// close all overlays via Esc
document.addEventListener("keydown", (e) => { if (e.key === "Escape") goHome(); });

// Home button: close any overlays and reset the homepage
btnHome?.addEventListener("click", goHome);

// --- FAQ collapsibles ---
(function wireFaqToggles(){
  const container = document.getElementById("faqOverlay");
  if (!container) return;
  container.querySelectorAll(".faq-q").forEach(btn=>{
    btn.addEventListener("click", (e)=>{
      e.preventDefault();
      e.stopPropagation();
      const item = btn.closest(".faq-item");
      item?.classList.toggle("open");
    });
  });
})();


function goHome(){
  // Close ALL overlays (now includes Privacy & Terms)
  document.querySelectorAll(".tutorial-overlay,.faq-overlay,.pt-overlay,.modal-overlay").forEach(el=>{
    el.style.display = "none";
    el.classList.remove("open","hidden");
  });

  // Restore page scroll if it was locked
  if (Object.prototype.hasOwnProperty.call(document.body.dataset, "prevOverflow")) {
    document.body.style.overflow = document.body.dataset.prevOverflow || "";
    delete document.body.dataset.prevOverflow;
  } else {
    document.body.style.overflow = "";
  }

  window.scrollTo(0, 0);
}

btnHome?.addEventListener("click", goHome);

