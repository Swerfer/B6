/**********************************************************************
 admin.js – admin page logic (createMission + section gating)
**********************************************************************/
import { 
    connectWallet, 
    disconnectWallet, 
    walletAddress 
} from "./walletConnect.js";

import { 
    FACTORY_ADDRESS, 
    READ_ONLY_RPC, 
    FACTORY_ABI,
    MISSION_ABI,
    showAlert, 
    showConfirm,
    setBtnLoading,
    clearSelection,
    shorten,          // new
    statusText, 
} from "./core.js";

/* ---------- DOM ---------- */
const adminSections     = document.querySelectorAll (".section-box");
const connectBtn        = document.getElementById   ("connectWalletBtn");
const unauth            = document.getElementById   ("unauthNotice");
const form              = document.getElementById   ("createMissionForm");
const createBtn         = document.getElementById   ("createMissionBtn");
const invalidNotice     = document.getElementById   ("formInvalidNotice");
const enrollmentStartIn = document.getElementById   ("enrollmentStart");
const enrollmentEndIn   = document.getElementById   ("enrollmentEnd");
const missionStartIn    = document.getElementById   ("missionStart");
const missionEndIn      = document.getElementById   ("missionEnd");
const missionTypeSel    = document.getElementById   ("missionType");
const roundsIn          = document.getElementById   ("rounds");
const missionsList      = document.getElementById   ("missionsList");
const missionModal      = document.getElementById   ("missionModal");
const modalTitle        = document.getElementById   ("missionModalTitle");
const modalBody         = document.getElementById   ("missionModalBody");
const modalCloseX       = document.getElementById   ("missionModalClose");
const modalCloseBtn     = document.getElementById   ("missionModalCloseBtn");

/* ---------- live-validation ---------- */
const highlight = els => {
  [...form.elements].forEach(el => el.classList.remove("field-invalid"));
  els.forEach(el => el?.classList.add("field-invalid"));
};

const validate = () => {
  if(!form) return {ok:false, bad:[]};

  const f      = form.elements;
  const bad    = [];

  /* numeric helpers */
  const valNum = el => el.value.trim() === "" ? NaN : +el.value;

  /* grab all the things up-front */
  const rounds = valNum(f.rounds);                  // ≥ 5
  const minP   = valNum(f.minPlayers);              // ≥ rounds
  const maxP   = valNum(f.maxPlayers);              // ≥ minP
  const fee    = parseFloat(f.enrollmentAmount.value);
  const sEraw  = f.enrollmentStart.value;
  const eEraw  = f.enrollmentEnd.value;
  const mSraw  = f.missionStart.value;
  const mEraw  = f.missionEnd.value;
  const sE     = sEraw ? new Date(sEraw) : NaN;
  const eE     = eEraw ? new Date(eEraw) : NaN;
  const mS     = mSraw ? new Date(mSraw) : NaN;
  const mE     = mEraw ? new Date(mEraw) : NaN;

  /* ---- mandatory & range checks ---- */
  if(isNaN(rounds)         || rounds < 5)            bad.push(f.rounds);
  if(isNaN(minP)           || minP   < rounds)       bad.push(f.minPlayers);
  if(isNaN(maxP)           || maxP   < minP)         bad.push(f.maxPlayers);
  if(!sEraw)                                         bad.push(f.enrollmentStart);
  if(!eEraw  || !(sE < eE))                          bad.push(f.enrollmentEnd);
  if(!mSraw  || !(mS >= eE))                         bad.push(f.missionStart);
  if(!mEraw  || !(mE >  mS))                         bad.push(f.missionEnd);
  if(isNaN(fee)            || fee   <= 0)            bad.push(f.enrollmentAmount);

  return { ok: bad.length === 0, bad };
};

const updateBtn = () => {
  const {ok, bad} = validate();
  createBtn.disabled = !ok;
  invalidNotice.classList.toggle("d-none", ok);
  highlight(bad);
};

form?.addEventListener("input",  updateBtn);
form?.addEventListener("change", updateBtn);
document.addEventListener("DOMContentLoaded", updateBtn,    {once:true});

function missionTypeName(type) {
  return {
    0: "Custom",
    1: "Hourly",
    2: "Quarter-Daily",
    3: "Bi-Daily",
    4: "Daily",
    5: "Weekly",
    6: "Monthly"
  }[type] || `Unknown (${type})`;
}

/* ---------- helpers ---------- */
async function openMissionModal(item, btnRef = null){
  try{
    const p  = new ethers.providers.JsonRpcProvider(READ_ONLY_RPC);
    const mc = new ethers.Contract(item.addr, MISSION_ABI, p);
    if (btnRef) setBtnLoading(btnRef, true, "Reloading");
  const [
    players,              // address[]
    missionType,          // uint8
    enrollmentStart,      // uint256
    enrollmentEnd,        // uint256
    enrollmentAmount,     // uint256
    enrollmentMinPlayers, // uint8
    enrollmentMaxPlayers, // uint8
    missionStart,         // uint256
    missionEnd,           // uint256
    missionRounds,        // uint8
    roundCount,           // uint8
    ethStart,             // uint256
    ethCurrent,           // uint256
    playersWon,           // array with { player, amountWon }
    pauseTimestamp,       // uint256
    refundedPlayers       // address[]
  ] = await mc.getMissionData();

    const status = await mc.getRealtimeStatus();

    const ts = s => new Date(Number(s) * 1000).toLocaleString(navigator.language, {
      dateStyle: 'short',
      timeStyle: 'short'
    });

    /* build rows */
    const rows = `
      <tr><th>Status</th>             <td>${statusText(status)}</td></tr>
      <tr><th>Players</th>            <td>${players.length}</td></tr>
      <tr><th>Mission Type</th>       <td>${missionTypeName(missionType)}</td></tr>
      <tr><th>Enrollment Start</th>   <td>${ts(enrollmentStart)}</td></tr>
      <tr><th>Enrollment End</th>     <td>${ts(enrollmentEnd)}</td></tr>
      <tr><th>Mission Start</th>      <td>${ts(missionStart)}</td></tr>
      <tr><th>Mission End</th>        <td>${ts(missionEnd)}</td></tr>
      <tr><th>Min Players</th>        <td>${enrollmentMinPlayers}</td></tr>
      <tr><th>Max Players</th>        <td>${enrollmentMaxPlayers}</td></tr>
      <tr><th>Round Count</th>        <td>${roundCount}</td></tr>
      <tr><th>Enrollment Amount</th>  <td>${ethers.utils.formatEther(enrollmentAmount)} CRO</td></tr>
      <tr><th>CRO Start</th>          <td>${ethers.utils.formatEther(ethStart)} CRO</td></tr>
      <tr><th>CRO Current</th>        <td>${ethers.utils.formatEther(ethCurrent)} CRO</td></tr>
      <tr><th>Rounds</th>             <td>${missionRounds}</td></tr>
      <tr><th>Players Won</th>        <td>${playersWon.length}</td></tr>
      <tr><th>Refunded Players</th>   <td>${refundedPlayers.length}</td></tr>
    `;
    
    const shouldRefund = (
      status === 7 && // Status.Failed
      players.length > 0 &&
      refundedPlayers.length === 0
    );

    if (shouldRefund) {
      triggerRefundModal(item.addr);
    }

    modalTitle.textContent = `Mission ${shorten(item.addr)}`;

    let buttons = `
      <button class="btn btn-sm btn-outline-info me-2 reload-btn" data-addr="${item.addr}">
        <i class="fa-solid fa-rotate-right me-1"></i> Reload
      </button>
    `;

    const needsRefund = (
      status === 7 &&
      players.length > 0 &&
      refundedPlayers.length === 0
    );

    if (needsRefund) {
      buttons = `
        <button class="btn btn-sm btn-outline-warning me-2 refund-btn" data-addr="${item.addr}">
          <i class="fa-solid fa-coins me-1"></i> Refund
        </button>` + buttons;
    }

    buttons += `
      <button id="missionModalCloseBtn" class="btn btn-sm btn-outline-info me-2">
        <i class="fa-solid fa-xmark me-1"></i> Close
      </button>
    `;

    modalBody.innerHTML = `
      <table class="mission-table w-100">${rows}</table>
      <div class="text-center mt-4">${buttons}</div>
    `;

    const reloadBtn = modalBody.querySelector('.reload-btn');
    if (reloadBtn) {
      reloadBtn.addEventListener('click', () => openMissionModal(item, reloadBtn));
    }

    const refundBtn = modalBody.querySelector('.refund-btn');
    if (refundBtn) {
      refundBtn.addEventListener('click', () => triggerRefundModal(item.addr, refundBtn));
    }

    missionModal.classList.remove("hidden");
    document.body.classList.add("modal-open");

  }catch(e){
    showAlert(`getMissionData failed:<br>${e.message}`,"error");
  } finally {
    if (btnRef) setBtnLoading(btnRef, false);
  }
}

async function triggerRefundModal(address, btnRef){
  try {

    const provider = new ethers.providers.Web3Provider(window.ethereum);
    const signer   = provider.getSigner();
    const mc       = new ethers.Contract(address, MISSION_ABI, signer); 

    showConfirm("This mission has <b>Failed</b>, but no players were refunded yet.<br>Do you want to call <code>refundPlayers()</code> now?", async () => {
      try {
        const refundBtn = document.querySelector('button[onclick*="triggerRefundModal"]');
        setBtnLoading(btnRef, true, "Refunding");

        const tx = await mc.refundPlayers();
        await tx.wait();

        setBtnLoading(btnRef, false);
        showAlert("Refund completed.", "success");
        openMissionModal({ addr: address });
      } catch (e) {
        showAlert(`Refund failed: ${e.message}`, "error");
      } finally {
          setBtnLoading(btnRef, false);
      }
    });
  } catch (e) {
    showAlert("Unable to refund: " + e.message, "error");
  }
}

function updateConnectButton(state = "idle", address = ""){
  const text = document.getElementById("connectBtnText");
  if(!text) return;

  switch(state){
    case "connecting":
      text.innerHTML = "Connecting…";
      break;
    case "connected":
      text.innerHTML = shorten(address);
      break;
    default:
      text.innerHTML = "Connect Wallet";
  }
}

/* close: add/remove modal-open class */
function closeMissionModal(){
  missionModal.classList.add("hidden");
  document.body.classList.remove("modal-open");
}

modalCloseX?.addEventListener("click", closeMissionModal);
modalCloseBtn?.addEventListener("click", closeMissionModal);

const toUnix = iso => Math.floor(new Date(iso).getTime() / 1000);
const eth    = ethers.utils;                       // alias

function toggleSections(show){
  /* show/hide the three admin panels */
  adminSections.forEach(sec => sec.classList.toggle("hidden", !show));
  /* invert visibility for the friendly notice */
  if (unauth) unauth.classList.toggle("hidden", show);
}

const MS = 1000, H = 3600*MS, D = 24*H, W = 7*D;
const missionDefaults = {
  1: { enroll:D, arm:H,  round:H        }, // Hourly
  2: { enroll:D, arm:H,  round:6*H      }, // Quarter-Daily
  3: { enroll:D, arm:H,  round:12*H     }, // Bi-Daily
  4: { enroll:D, arm:H,  round:24*H     }, // Daily
  5: { enroll:W, arm:H,  round:7*D      }, // Weekly
  6: { enroll:W, arm:H,  round:30*D     }  // Monthly
};
const toLocalIso = dt =>
  new Date(dt.getTime()-dt.getTimezoneOffset()*60*1000).toISOString().slice(0,16);

/* ---------- auto-populate logic ---------- */
function applyDateDefaults(){
  const type = Number(missionTypeSel.value);
  if(!missionDefaults[type]) return;               // ignore “Custom”

  const base   = new Date(enrollmentStartIn.value);
  if(isNaN(base)) return;                          // malformed date

  const def    = missionDefaults[type];
  const rounds = Math.max(Number(roundsIn.value)||1, 1);

  const enrollEnd = new Date(+base + def.enroll);
  const mStart    = new Date(+enrollEnd + def.arm);
  const mEnd      = new Date(+mStart   + def.round * rounds);

  enrollmentEndIn.value = toLocalIso(enrollEnd);
  missionStartIn.value  = toLocalIso(mStart);
  missionEndIn.value    = toLocalIso(mEnd);
  updateBtn?.();
}

/* ---------- ask to apply defaults ---------- */
const askDefaults = () => {
  // only offer when a base date exists and type isn't "Custom"
  if (!enrollmentStartIn.value || Number(missionTypeSel.value) === 0) return;
  clearSelection();
  const typeName = missionTypeSel.options[missionTypeSel.selectedIndex].textContent;
  showConfirm(
    `Apply default dates for <b>${typeName}</b>?<br><small>(you can still edit them afterwards)</small>`,
    applyDateDefaults
  );
};

/* show the dialog only when the user has *committed* a change */
enrollmentStartIn?.addEventListener("change", askDefaults);  // fires when date-picker closes
missionTypeSel?.addEventListener("change", askDefaults);

/* ---------- role check ---------- */
async function isOwnerOrAuthorized(addr){
  /* 1 — try via the wallet’s provider (requires the account to be “connected”) */
  try{
    const provider = new ethers.providers.Web3Provider(window.ethereum);
    const factory  = new ethers.Contract(FACTORY_ADDRESS, FACTORY_ABI, provider);
    const [ownerAddr, isAuth] = await Promise.all([
      factory.owner(),
      factory.authorized(addr)
    ]);
    return ownerAddr.toLowerCase() === addr || isAuth;
  }catch(err){
    /* MetaMask throws “dapp not connected” when the new account hasn’t granted access.
       We silently fall back to a public RPC for a read-only check. */
    if (String(err?.message).includes("dapp not connected")){
      try{
        const fallback  = new ethers.providers.JsonRpcProvider(READ_ONLY_RPC);
        const factory   = new ethers.Contract(FACTORY_ADDRESS, FACTORY_ABI, fallback);
        const [ownerAddr, isAuth] = await Promise.all([
          factory.owner(),
          factory.authorized(addr)
        ]);
        return ownerAddr.toLowerCase() === addr || isAuth;
      }catch(e2){
        console.warn("Read-only RPC failed:", e2.message);
      }
    } else {
      console.warn(err.message);
    }
    return false;   // default: not owner / not authorized
  }
}

/* ---------- wallet button ---------- */
connectBtn.addEventListener("click", () => {
  if(walletAddress){
    showConfirm("Disconnect current wallet?", () => {
      disconnectWallet();
      toggleSections(false);
      updateConnectButton(); // reset
    });
  } else {
    updateConnectButton("connecting");
    connectWallet().then(handlePostConnect);
  }
});

/* ---------- after connect / account change ---------- */
async function handlePostConnect(addrOverride){
  /* use the override (from the MetaMask event) or fall back to the
     live-exported walletAddress coming from walletConnect.js */
  const addr = addrOverride || walletAddress;
  if(!addr){
    toggleSections(false);
    return;
  }

  const allowed = await isOwnerOrAuthorized(addr);
  toggleSections(allowed);

  if (allowed) {
    updateConnectButton("connected", addr);
    await loadMissions();   // ✅ load only after admin is confirmed
  } else {
    updateConnectButton(); // reset
    showAlert(
      `This wallet is neither from an <b>owner</b> nor from an <b>authorized</b>.`, 'warning',
      () => disconnectWallet()
    );
  }

}

/* ---------- MetaMask account change ---------- */
if(window.ethereum){
  window.ethereum.on("accountsChanged", accounts => {
    /* walletConnect.js updates its export asynchronously, so we
       pass the new address directly and wait one tick to be safe */
    const newAddr = accounts[0] ? accounts[0].toLowerCase() : null;
    setTimeout(() => handlePostConnect(newAddr), 0);
  });
}

/* ---------- auto-connect on page load ---------- */
(document.readyState === "loading"
  ? document.addEventListener("DOMContentLoaded", () => connectWallet().then(handlePostConnect), { once:true })
  : connectWallet().then(handlePostConnect));

//* ---------- load missions list ---------- */
async function loadMissions(){
  try{
    const provider = new ethers.providers.JsonRpcProvider(READ_ONLY_RPC);
    const factory  = new ethers.Contract(FACTORY_ADDRESS, FACTORY_ABI, provider);

    /* fetch two parallel arrays */
    const [addrs, stats] = await factory.getAllMissions();

    /* combine, giving each an implicit index (newest = last element) */
    const items = addrs.map((addr, i) => ({
      addr,
      status: Number(stats[i]),
      idx: addrs.length - i   // for newest-first sort
    }));

    /* sort: all PartlySuccess first, then newest-to-oldest */
    items.sort((a,b) => {
      if(a.status === 5 && b.status !== 5) return -1;
      if(b.status === 5 && a.status !== 5) return  1;
      return b.idx - a.idx;
    });

    /* render */
    missionsList.innerHTML = "";
    items.forEach(m => {
      const li = document.createElement("li");
      li.className = "mission-item" + (m.status === 3 ? " partly-success" : "");
      li.innerHTML = `
        <span>${shorten(m.addr)}</span>
        <span>${statusText(m.status)}</span>`;
      li.addEventListener("click", () => openMissionModal(m));
      missionsList.appendChild(li);
    });

    /* show / hide the whole section if empty */
    missionsSection.classList.toggle("hidden", items.length === 0);

  }catch(err){
    console.warn("loadMissions()", err);
  }
}

/* ---------- form submit ---------- */
form?.addEventListener("submit", async e => {
  e.preventDefault();
  if(createBtn) setBtnLoading(createBtn, true, "Creating&nbsp;Mission");         // start spinner

  if(!walletAddress){ 
    setBtnLoading(createBtn, false);  
    return showAlert("Please connect a wallet first.","error"); 
  }
  if(!(await isOwnerOrAuthorized(walletAddress))){
    setBtnLoading(createBtn, false); 
    return showAlert("This wallet is not authorized.","error");
  }
  if(!walletAddress)   return showAlert("Please connect a wallet first.","error");
  if(!(await isOwnerOrAuthorized(walletAddress)))
    return showAlert("This wallet is not authorized.","error");

  try{
    const p       = new ethers.providers.Web3Provider(window.ethereum);
    const signer  = p.getSigner();
    const factory = new ethers.Contract(FACTORY_ADDRESS, FACTORY_ABI, signer);

    /* ─ gather & convert ─ */
    const f = form.elements;
    const args = [
      parseInt(f.missionType.value),           // MissionType
      toUnix(f.enrollmentStart.value),         // enrollmentStart
      toUnix(f.enrollmentEnd.value),           // enrollmentEnd
      eth.parseEther(f.enrollmentAmount.value),// enrollmentAmount
      parseInt(f.minPlayers.value),            // min players
      parseInt(f.maxPlayers.value),            // max players
      toUnix(f.missionStart.value),            // missionStart
      toUnix(f.missionEnd.value),              // missionEnd
      parseInt(f.rounds.value)                 // missionRounds
    ];
    const tx = await factory.createMission(
      ...args,
      { value: eth.parseEther(f.initialPot.value || "0") }
    );
    showAlert("Transaction sent – waiting for confirmation…","info");
    await tx.wait();
    showAlert("Mission created successfully!","success");
    await loadMissions();
    form.reset();
    }catch (err){

        /* ---------- extract a meaningful revert reason ---------- */
        let msg =
                /* 1 ▸ common MetaMask shape                                 */
                err?.data?.message
            || err?.error?.data?.message
            || err?.reason
            /* 2 ▸ ethers.js ProviderError                                 */
            || err?.error?.message
            || null;

        /* 3 ▸ last-resort: decode raw `Error(string)` ABI data (0x08c379a0…) */
        if (!msg){
            const hexData = err?.data?.originalError?.data
                        || err?.data
                        || err?.error?.data;
            if (hexData && hexData.startsWith("0x08c379a0")){
            try{
                const iface = new ethers.utils.Interface(["function Error(string)"]);
                msg = iface.decodeFunctionData("Error", hexData)[0];
            }catch{/* ignore – fall through */}
            }
        }

        if (!msg) msg = err.message || "Transaction failed";
        showAlert(msg, "error");
    } finally {
      setBtnLoading(createBtn,false);                  // always stop
      updateBtn();                                     // re-validate after reset
    }
});

/* ---------- export public functions for admin.html ---------- */
window.triggerRefundModal = triggerRefundModal;
window.openMissionModal   = openMissionModal;

