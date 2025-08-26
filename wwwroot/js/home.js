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

    // prefer actual upcoming mission starts from not-ended
    for (const m of notEndedList){
    const start = Number(m.mission_start ?? m.MissionStart ?? 0);
    if (start > now && (!soonest || start < soonest)) soonest = start;
    }

    // fallback: use joinable missions' mission_start when not-ended gave none
    if (!soonest) {
    for (const m of joinableList){
        const start = Number(m.mission_start ?? m.MissionStart ?? 0);
        if (start > now && (!soonest || start < soonest)) soonest = start;
    }
    }

    setCountdown(soonest || 0);
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

  // clickable + copyable addresses (core.js handles [data-copy] click)
  els.factoryAddress.innerHTML =
    `<span class="copy-wrap" data-copy="${FACTORY_ADDRESS}">${shorten(FACTORY_ADDRESS)}</span>${addrLinkIcon(FACTORY_ADDRESS)}`;
  if (impl){
    els.implAddress.innerHTML =
      `<span class="copy-wrap" data-copy="${impl}">${shorten(impl)}</span>${addrLinkIcon(impl)}`;
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
const btnHome = document.getElementById("btnHome");
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

