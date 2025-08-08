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
    statusText,
    fadeSpinner,
    missionTypeName,
    copyableAddr,
    shorten,
    decodeError,
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
const missionNameIn     = document.getElementById   ("missionName");
const missionTypeSel    = document.getElementById   ("missionType");
const roundsIn          = document.getElementById   ("rounds");
const missionsList      = document.getElementById   ("missionsList");
const missionModal      = document.getElementById   ("missionModal");
const modalTitle        = document.getElementById   ("missionModalTitle");
const modalBody         = document.getElementById   ("missionModalBody");
const modalCloseX       = document.getElementById   ("missionModalClose");
const modalCloseBtn     = document.getElementById   ("missionModalCloseBtn"); // Do not remove this line, it is used in the modalBody

/* ---------- live-validation ---------- */
const highlight = els => {
  [...form.elements].forEach(el => el.classList.remove("field-invalid"));
  els.forEach(el => el?.classList.add("field-invalid"));
};

const validate = () => {
  if(!form) return {ok:false, bad:[]};

  const f       = form.elements;
  const name    = f.missionName.value.trim();
  const type    = parseInt(f.missionType.value);
  const bad    = [];

  if (type === 0 && !name) {
    bad.push(f.missionName);  // Custom missions require a name
  }
  if (name.length > 24) {
    bad.push(f.missionName);  // Should never happen with maxlength, but safe
  }

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

function formatSecondsToDHMS(seconds){
  seconds = Number(seconds);
  if (isNaN(seconds) || seconds <= 0) return "—";

  const d = Math.floor(seconds / 86400);
  const h = Math.floor((seconds % 86400) / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = seconds % 60;

  const pad = n => n.toString().padStart(2, "0");
  const dhms = `${d}d ${pad(h)}:${pad(m)}:${pad(s)}`;
  return dhms;
}

const updateBtn = () => {
  const {ok, bad} = validate();
  createBtn.disabled = !ok;
  invalidNotice.classList.toggle("d-none", ok);
  highlight(bad);
};

form?.addEventListener("input",  updateBtn);
form?.addEventListener("change", updateBtn);
document.addEventListener("DOMContentLoaded", updateBtn,    {once:true});

document.getElementById("latestMissionsLink")?.addEventListener("click", async e=>{
  e.preventDefault();
  let n = parseInt(prompt("How many missions?","10"),10);
  if(isNaN(n)||n<=0) return;
  await loadLatestMissions(n);
});

/* ---------- helpers ---------- */

function flashCard(el, delay = 0) {
  setTimeout(() => {
    el.classList.add("flash");
    setTimeout(() => el.classList.remove("flash"), 300);
  }, delay);
}

async function openMissionModal(item, btnRef = null, factoryStatus = null){
  try{
    const p  = new ethers.providers.JsonRpcProvider(READ_ONLY_RPC);
    const mc = new ethers.Contract(item.addr, MISSION_ABI, p);
    if (btnRef instanceof HTMLButtonElement) { // If a button reference is provided, show loading state
      setBtnLoading(btnRef, true, "Reloading");
    }
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
    pauseTimestamp,       // uint256 - Do not remove this, despite not being used here
    refundedPlayers       // address[]
  ] = await mc.getMissionData();

    const status = await mc.getRealtimeStatus();

    if (btnRef && btnRef.dataset.loading){
     await new Promise(res => {
       setBtnLoading(btnRef, false);
       btnRef.addEventListener("transitionend", res, { once:true });
     });
    }

    const ts = s => new Date(Number(s) * 1000).toLocaleString(navigator.language, {
      dateStyle: 'short',
      timeStyle: 'short'
    });

    /* build rows */
    const rowTemplates = [
      `<tr><th>Factory Status</th>     <td>${statusText(factoryStatus)}</td></tr>`,
      `<tr><th>Realtime Status</th>    <td>${statusText(status)}</td></tr>`,
      `<tr><th>Players</th>            <td${players.length < enrollmentMinPlayers ? ' class="text-warning fw-bold"' : ''}>${players.length}</td></tr>`,
      `<tr><th>Mission Type</th>       <td>${missionTypeName[missionType]}</td></tr>`,
      `<tr><th>Enrollment Start</th>   <td>${ts(enrollmentStart)}</td></tr>`,
      `<tr><th>Enrollment End</th>     <td>${ts(enrollmentEnd)}</td></tr>`,
      `<tr><th>Mission Start</th>      <td>${ts(missionStart)}</td></tr>`,
      `<tr><th>Mission End</th>        <td>${ts(missionEnd)}</td></tr>`,
      `<tr><th>Min Players</th>        <td>${enrollmentMinPlayers}</td></tr>`,
      `<tr><th>Max Players</th>        <td>${enrollmentMaxPlayers}</td></tr>`,
      `<tr><th>Round Count</th>        <td>${roundCount}</td></tr>`,
      `<tr><th>Enrollment Amount</th>  <td>${ethers.utils.formatEther(enrollmentAmount)} CRO</td></tr>`,
      `<tr><th>CRO Start</th>          <td>${ethers.utils.formatEther(ethStart)} CRO</td></tr>`,
      `<tr><th>CRO Current</th>        <td>${ethers.utils.formatEther(ethCurrent)} CRO</td></tr>`,
      `<tr><th>Rounds</th>             <td>${missionRounds}</td></tr>`,
      `<tr><th>Players Won</th>        <td>${playersWon.length}</td></tr>`,
      `<tr><th>Refunded Players</th>   <td>${refundedPlayers.length}</td></tr>`
    ];

    const shouldRefund = (
      status === 7 && // Status.Failed
      players.length > 0 &&
      refundedPlayers.length === 0
    );

    if (shouldRefund) {
      triggerRefundModal(item.addr);
    }

    modalTitle.innerHTML = `Mission ${copyableAddr(item.addr)}</br><span class="missionTitle">Mission name: ${item.name}</span>`;

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
      <table class="mission-table w-100"><tbody id="missionDataBody"></tbody></table>
      <div class="text-center mt-4">${buttons}</div>
    `;

    const tbody = document.getElementById("missionDataBody");
      tbody.innerHTML = ""; // clear old rows if any

    rowTemplates.forEach((rowHTML, i) => {
      const row = document.createElement("tr");
      row.innerHTML = rowHTML.replace(/^<tr>|<\/tr>$/g, ""); // strip <tr> wrapper
      row.style.opacity = 0;
      row.style.transition = "opacity 0.3s ease";
      setTimeout(() => {
        tbody.appendChild(row);
        requestAnimationFrame(() => row.style.opacity = 1);
      }, i * 30); // 0.03s per row
    });

    document.getElementById("missionModalCloseBtn")?.addEventListener("click", closeMissionModal);

    const reloadBtn = modalBody.querySelector('.reload-btn');
    if (reloadBtn) {
      reloadBtn.addEventListener('click', () => openMissionModal(item, reloadBtn, factoryStatus));
    }

    const refundBtn = modalBody.querySelector('.refund-btn');
    if (refundBtn) {
      refundBtn.addEventListener('click', () => triggerRefundModal(item.addr, refundBtn, factoryStatus));
    }

    adminSections.forEach(sec => sec.classList.add("hidden"));
    missionModal.classList.remove("hidden");

  }catch(e){
    showAlert(`getMissionData failed:<br>${e.message}`,"error");
  } 
}

async function triggerRefundModal(address, btnRef, factoryStatus = null) {
  try {
    const provider = new ethers.providers.Web3Provider(window.ethereum);
    const signer   = provider.getSigner();
    const mc       = new ethers.Contract(address, MISSION_ABI, signer); 

    showConfirm(
      "This mission has <b>Failed</b>, but no players were refunded yet.<br>Do you want to call <code>refundPlayers()</code> now?",
      async () => {
        let success = false;

        setBtnLoading(btnRef, true, "Refunding");

        try {
          const tx = await mc.refundPlayers();
          await tx.wait();
          success = true;
        } catch (e) {
          showAlert(`Refund failed: ${e.message}`, "error");
        }

        await new Promise(res => {
          setBtnLoading(btnRef, false);
          btnRef?.addEventListener("transitionend", res, { once: true });
          setTimeout(res, 600);
        });

        if (success) {
          showAlert("Refund completed.", "success");
        }

        openMissionModal({ addr: address }, null, factoryStatus);  // ✅ always reload modal
      }
    );
  } catch (e) {
    // This only triggers if signer setup fails, etc.
    setBtnLoading(btnRef, false);
    showAlert("Unable to refund: " + e.message, "error");
    openMissionModal({ addr: address }, null, factoryStatus);  // ✅ show modal even on setup fail
  }
}

function updateConnectButton(state = "idle", address = ""){
  const btn  = document.getElementById("connectWalletBtn");
  const text = document.getElementById("connectBtnText");
  if (!text) return;
  switch(state){
    case "connecting":
      btn ? setBtnLoading(btn, true, "Connecting")
          : text.textContent = "Connecting…";
      break;

    case "connected":
      if (btn) setBtnLoading(btn, false, "", false);
      text.textContent = shorten(address);
      break;

    default:                    // idle / disconnected
      if (btn) setBtnLoading(btn, false);
      text.textContent = "Connect Wallet";
  }
}

/* close: add/remove modal-open class */
function closeMissionModal(showList = false){
  missionModal.classList.add("hidden");
  if (showList) {
    document.getElementById("missionsSection")?.classList.remove("hidden");
  }
}

modalCloseX?.addEventListener("click", () => closeMissionModal(true));

const toUnix = iso => Math.floor(new Date(iso).getTime() / 1000);
const eth    = ethers.utils;                       // alias

function toggleSections(show){
  adminSections.forEach(sec => sec.classList.toggle("hidden", !show));
  document.querySelectorAll(".admin-only-btn").forEach(btn => {
    btn.style.display = show ? "inline-flex" : "none";
  });
  if (unauth) unauth.classList.toggle("hidden", show);
  if (!show) closeMissionModal(false);
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

/* ---------- Read-only view helpers ---------- */
async function refreshGlobalView(){
  const p       = new ethers.providers.JsonRpcProvider(READ_ONLY_RPC);
  const factory = new ethers.Contract(FACTORY_ADDRESS, FACTORY_ABI, p);

  const summary = await factory.getFactorySummary();
  const [owner, _factory, impl, totM, wk, mo, funds, ownerFunds, succ, fail, fundsByType] = summary;

  const fmt = v => ethers.utils.formatEther(v);
  const g = document.getElementById("globalView");
  g.innerHTML = "";
  let rowIndex = 0;
  const add = (label, val) => {
    g.insertAdjacentHTML("beforeend",
      `<div class="view-card"><h4>${label}</h4><p>${val}</p></div>`);
    flashCard(g.lastElementChild, rowIndex++ * 100);
  };

  add("Factory&nbsp;Owner",   copyableAddr(owner));
  add("Factory&nbsp;Address", copyableAddr(_factory));
  add("Mission&nbsp;Impl.",   copyableAddr(impl));
  add("Total&nbsp;Missions",  totM);
  add("Successes",            succ);
  add("Failures",             fail);
  add("Weekly&nbsp;Limit",    wk);
  add("Monthly&nbsp;Limit",   mo);
  add("Owner&nbsp;Earnings",  fmt(ownerFunds)+" CRO");
  add("Total&nbsp;Funds",     fmt(funds)+" CRO");
  fundsByType.forEach((f,i)=>
    add(`${missionTypeName[i + 1]}&nbsp;Funds`, fmt(f)+" CRO"));
}

async function loadFactoryWriteData() {
  const provider = new ethers.providers.JsonRpcProvider(READ_ONLY_RPC);
  const factory  = new ethers.Contract(FACTORY_ADDRESS, FACTORY_ABI, provider);

  try {
    const [owner, weekly, monthly, funds, realbalance] = await Promise.all([
      factory.owner(),
      factory.weeklyLimit(),
      factory.monthlyLimit(),
      factory.totalMissionFunds(),
      provider.getBalance(FACTORY_ADDRESS)
    ]);

    const isOwner = walletAddress && walletAddress.toLowerCase() === owner.toLowerCase();

    document.getElementById("weeklyLimit").placeholder = `Weekly limit (current: ${weekly})`;
    document.getElementById("monthlyLimit").placeholder = `Monthly limit (current: ${monthly})`;
    document.getElementById("fundsAvailable").textContent = `${ethers.utils.formatEther(realbalance)} CRO available`;

    // Optional: If you also want to show the mission-locked funds elsewhere:
    // document.getElementById("lockedFunds").textContent =
    //   `${ethers.utils.formatEther(missionFunds)} CRO locked`;

    document.getElementById("withdrawBtn").disabled = !isOwner;
  } catch (err) {
    console.error("loadFactoryWriteData() failed:", err);
    showAlert("Could not load factory write info", "error");
  }
}

async function setEnrollmentLimits(btn) {
  const w = document.getElementById("weeklyLimit")?.value.trim();
  const m = document.getElementById("monthlyLimit")?.value.trim();
  if (!w || !m) return showAlert("Please fill both limits", "warning");

  showConfirm("Update enrollment limits?", async () => {
    setBtnLoading(btn, true, "Submitting");

    try {
      const provider = new ethers.providers.Web3Provider(window.ethereum);
      const signer   = provider.getSigner();
      const factory  = new ethers.Contract(FACTORY_ADDRESS, FACTORY_ABI, signer);
      const tx = await factory.setEnrollmentLimits(w, m);
      await tx.wait();
      showAlert("Enrollment limits updated", "success");
      await loadFactoryWriteData();
    } catch (err) {
      showAlert(decodeError(err), "error");
    }

    setBtnLoading(btn, false, "Submit", false);
  });
}

async function addAuthorizedAddress(btn) {
  const addr = document.getElementById("authAdd")?.value.trim();
  if (!ethers.utils.isAddress(addr)) return showAlert("Invalid address", "warning");

  showConfirm(`Authorize address ${addr}?`, async () => {
    setBtnLoading(btn, true, "Adding");

    try {
      const provider = new ethers.providers.Web3Provider(window.ethereum);
      const signer   = provider.getSigner();
      const factory  = new ethers.Contract(FACTORY_ADDRESS, FACTORY_ABI, signer);
      const tx = await factory.addAuthorizedAddress(addr);
      await tx.wait();
      showAlert("Address authorized", "success");
    } catch (err) {
      showAlert(decodeError(err), "error");
    }

    setBtnLoading(btn, false, "Add", false);
  });
}

async function removeAuthorizedAddress(btn) {
  const addr = document.getElementById("authRemove")?.value.trim();
  if (!ethers.utils.isAddress(addr)) return showAlert("Invalid address", "warning");

  showConfirm(`Remove address ${addr}?`, async () => {
    setBtnLoading(btn, true, "Removing");

    try {
      const provider = new ethers.providers.Web3Provider(window.ethereum);
      const signer   = provider.getSigner();
      const factory  = new ethers.Contract(FACTORY_ADDRESS, FACTORY_ABI, signer);
      const tx = await factory.removeAuthorizedAddress(addr);
      await tx.wait();
      showAlert("Address removed", "success");
    } catch (err) {
      showAlert(decodeError(err), "error");
    }

    setBtnLoading(btn, false, "Remove", false);
  });
}

async function proposeOwnershipTransfer(btn) {
  const newOwner = document.getElementById("proposeOwner")?.value.trim();
  if (!ethers.utils.isAddress(newOwner)) return showAlert("Invalid address", "warning");

  showConfirm(`Propose ownership transfer to ${newOwner}?`, async () => {
    setBtnLoading(btn, true, "Proposing");

    try {
      const provider = new ethers.providers.Web3Provider(window.ethereum);
      const signer   = provider.getSigner();
      const factory  = new ethers.Contract(FACTORY_ADDRESS, FACTORY_ABI, signer);
      const tx = await factory.proposeOwnershipTransfer(newOwner);
      await tx.wait();
      showAlert("Proposal submitted", "success");
    } catch (err) {
      showAlert(decodeError(err), "error");
    }

    setBtnLoading(btn, false, "Propose", false);
  });
}

async function confirmOwnershipTransfer() {
  try {
    const provider = new ethers.providers.Web3Provider(window.ethereum);
    const signer   = provider.getSigner();
    const factory  = new ethers.Contract(FACTORY_ADDRESS, FACTORY_ABI, signer);

    const tx = await factory.confirmOwnershipTransfer(); 
    showAlert("Confirming transfer…", "info");
    await tx.wait();
    showAlert("Ownership transferred", "success");
  } catch (err) {
    showAlert(decodeError(err), "error");
  }
}

let proposalCountdownInterval = null;

async function updateConfirmCard() {
  const card = document.getElementById("confirmCard");
  if (!card) return;

  clearInterval(proposalCountdownInterval); // clear old timer
  card.style.display = "none";

  try {
    const provider = new ethers.providers.JsonRpcProvider(READ_ONLY_RPC);
    const factory  = new ethers.Contract(FACTORY_ADDRESS, FACTORY_ABI, provider);
    const [newOwner, proposer, timestamp, timeLeft] = await factory.getOwnershipProposal();

    if (!newOwner || newOwner === ethers.constants.AddressZero || timeLeft === 0) return;

    document.getElementById("proposedOwnerShort").textContent = shorten(newOwner);
    document.getElementById("proposerShort").textContent = shorten(proposer);
    const countdownEl = document.getElementById("proposalCountdown");

    const targetTs = parseInt(timestamp) + 24 * 3600;
    const updateTimer = () => {
      const now = Math.floor(Date.now() / 1000);
      const left = targetTs - now;
      if (left <= 0) {
        clearInterval(proposalCountdownInterval);
        card.style.display = "none";
        return;
      }
      countdownEl.textContent = formatSecondsToDHMS(left).replace(/^\d+d\s*/, '');
    };

    updateTimer();
    proposalCountdownInterval = setInterval(updateTimer, 1000);
    card.style.display = "block";

  } catch (err) {
    console.warn("Proposal check failed:", err.message);
  }
}

async function withdrawFunds(btn) {
  const val = document.getElementById("withdrawAmount")?.value.trim();
  if (!val || isNaN(val) || Number(val) <= 0) {
    return showAlert("Invalid amount", "warning");
  }

  showConfirm(`Withdraw <strong>${val} CRO</strong> from the factory?`, async () => {
    setBtnLoading(btn, true, "Withdrawing");

    try {
      const provider = new ethers.providers.Web3Provider(window.ethereum);
      const signer   = provider.getSigner();
      const factory  = new ethers.Contract(FACTORY_ADDRESS, FACTORY_ABI, signer);
      const amount   = ethers.utils.parseEther(val);
      const tx       = await factory.withdrawFunds(amount);
      await tx.wait();
      showAlert("Funds withdrawn", "success");
      await loadFactoryWriteData();
      document.getElementById("withdrawAmount").value = "";
    } catch (err) {
      showAlert(decodeError(err), "error");
    }

    setBtnLoading(btn, false, "Withdraw", false);
  });
}

function setMaxWithdraw() {
  const p = new ethers.providers.JsonRpcProvider(READ_ONLY_RPC);
  p.getBalance(FACTORY_ADDRESS).then(f => {
    document.getElementById("withdrawAmount").value = ethers.utils.formatEther(f);
  });
}

async function lookupPlayer(addr){
  const wrap = document.getElementById("playerView");
  const p       = new ethers.providers.JsonRpcProvider(READ_ONLY_RPC);
  const factory = new ethers.Contract(FACTORY_ADDRESS, FACTORY_ABI, p);

  const [limits, ok, wSec, mSec, [joined]] = await Promise.all([
    factory.getPlayerLimits(addr),
    factory.canEnroll(addr),
    factory.secondsTillWeeklySlot(addr),
    factory.secondsTillMonthlySlot(addr),
    factory.getPlayerParticipation(addr),
  ]);

  wrap.innerHTML = "";
  const active = joined.filter(s => [1, 2, 3, 4].includes(s)).length;
  const labels = [
    ["Weekly&nbsp;Limit&nbsp;Used", limits[0]],
    ["Monthly&nbsp;Limit&nbsp;Used", limits[2]],
    ["Can&nbsp;Enroll?", ok ? "Yes" : "No"],
    ["Next Weekly Slot", formatSecondsToDHMS(wSec)],
    ["Next Monthly Slot", formatSecondsToDHMS(mSec)],
    ["Total Missions Joined", joined.length],
    ["Active Missions", active],
    ["Ended Missions", joined.length - active],
  ];

  labels.forEach(([label, value], i) => {
    if (ok && (label.includes("Weekly Slot") || label.includes("Monthly Slot"))) return;
    wrap.insertAdjacentHTML("beforeend", 
      `<div class="view-card"><h4>${label}</h4><p>${value}</p></div>`);
    flashCard(wrap.lastElementChild, i * 100);  // 0.03s between flashes
  });
}

async function lookupAddr(addr){
  const wrap = document.getElementById("addrView");
  const p       = new ethers.providers.JsonRpcProvider(READ_ONLY_RPC);
  const factory = new ethers.Contract(FACTORY_ADDRESS, FACTORY_ABI, p);

  const [ownerAddr, auth, isM] = await Promise.all([
    factory.owner(),            // ✨ added
    factory.authorized(addr),
    factory.isMission(addr),
  ]);

  /* decide label: "Yes - Owner" supersedes regular "Yes" */
  let authLabel = "No";
  if (addr.toLowerCase() === ownerAddr.toLowerCase()) {
    authLabel = "Yes&nbsp;-&nbsp;Owner";
  } else if (auth) {
    authLabel = "Yes";
  }

  wrap.innerHTML = "";
  wrap.insertAdjacentHTML("beforeend",
    `<div class="view-card"><h4>Authorized&nbsp;or&nbsp;Mission?</h4><p>${authLabel}</p></div>`);
  flashCard(wrap.lastElementChild, 0);

  wrap.insertAdjacentHTML("beforeend",
    `<div class="view-card"><h4>Is&nbsp;Mission?</h4><p>${isM ? "Yes" : "No"}</p></div>`);
  flashCard(wrap.lastElementChild, 100);
}

/* ---------- bind forms once DOM ready ---------- */
document.getElementById("playerForm")?.addEventListener("submit",e=>{
  e.preventDefault();
  const a=e.target.playerAddr.value.trim().toLowerCase();
  if(ethers.utils.isAddress(a)) {
    lookupPlayer(a);
  } else {
    showAlert("Invalid address","error");
  }

});

document.getElementById("addrForm")?.addEventListener("submit",e=>{
  e.preventDefault();
  const a=e.target.addrCheck.value.trim().toLowerCase();
  if(ethers.utils.isAddress(a)) {
    lookupAddr(a);
  } else {
    showAlert("Invalid address","error");
  }
});

/* refresh Factory Read Functions section whenever the section is shown */
document.querySelector('[data-target="factory-read"]')
  ?.addEventListener("click", ()=>refreshGlobalView());

/* refresh Factory Write Functions section whenever the section is shown */
document.querySelector('[data-target="factory-write"]')
  ?.addEventListener("click", () => {
    loadFactoryWriteData();
    updateConfirmCard(); // ← added to dynamically show/hide confirm card
  });

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
  if (missionNameIn) {
    if (Number(missionTypeSel.value) === 0) {
      missionNameIn.placeholder = "";
    } else {
      missionNameIn.placeholder = "Optional";
    }
  }

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
    adminSections.forEach(sec => sec.classList.add("hidden"));
    document.getElementById("missionsSection")?.classList.remove("hidden");
    await loadMissions();   // ✅ load only after admin is confirmed
  } else {
    updateConnectButton();
    toggleSections(false);
    missionsList.innerHTML = "";              
    document.getElementById("missionsSection")?.classList.add("hidden");
    form?.reset();
    createBtn?.setAttribute("disabled", "true");
    showAlert(
      `This wallet is neither from an <b>owner</b> nor from an <b>authorized</b>.`, 
      'warning',
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
function initializeAdminUI() {
  toggleSections(false); // Hide all admin stuff immediately
  connectWallet().then(handlePostConnect);
}

(document.readyState === "loading"
  ? document.addEventListener("DOMContentLoaded", initializeAdminUI, { once: true })
  : initializeAdminUI());

//* ---------- load missions list ---------- */
async function loadMissions(filter = "all") {
  const spinner = document.getElementById("missionsLoadingSpinner");
  spinner.classList.remove("hidden");
  try {
    fadeSpinner(spinner, true);
    missionsList.innerHTML = "";
    missionsList.classList.remove("empty");

    const provider = new ethers.providers.JsonRpcProvider(READ_ONLY_RPC);
    const factory  = new ethers.Contract(FACTORY_ADDRESS, FACTORY_ABI, provider);

    let addrs = [], stats = [], names = [];

    switch (filter) {
      case "active":
        [addrs, stats, names] = await factory.getMissionsNotEnded();
        setTitle("Active Missions");
        break;
      case "partial":
        [addrs, stats, names] = await factory.getMissionsByStatus(5);
        setTitle("Partly Ended Missions");
        break;
      case "failed":
        [addrs, stats, names] = await factory.getMissionsByStatus(7);
        setTitle("Failed Missions");
        break;
      case "ended":
        [addrs, stats, names] = await factory.getMissionsEnded();
        setTitle("Ended Missions");
        break;
      default:
        [addrs, stats, names] = await factory.getAllMissions();
        setTitle("All Missions");
    }
    const items = addrs.map((addr, i) => ({
      addr,
      name: names[i],
      status: Number(stats[i]),
      idx: addrs.length - i
    }));

    items.sort((a, b) => {
      if (a.status === 5 && b.status !== 5) return -1;
      if (b.status === 5 && a.status !== 5) return 1;
      return b.idx - a.idx;
    });

    missionsList.innerHTML = "";
    missionsList.classList.remove("empty");
    if (items.length === 0) {
      missionsList.classList.add("empty");
      const li = document.createElement("li");
      li.className = "mission-empty text-muted";
      li.textContent = "No missions found for this filter.";
      missionsList.appendChild(li);
    } else {
        await buildMissionGrid(addrs, stats, names);
    }

    missionsSection.classList.remove("hidden");
  } catch (err) {
    console.warn("loadMissions()", err);
  } finally {
    spinner.classList.remove("show");
    setTimeout(() => spinner.classList.add("hidden"), 500);
  }
}

async function loadLatestMissions(n = 10){
  const spinner = document.getElementById("missionsLoadingSpinner");
  fadeSpinner(spinner, true);
  try{
    const p       = new ethers.providers.JsonRpcProvider(READ_ONLY_RPC);
    const factory = new ethers.Contract(FACTORY_ADDRESS, FACTORY_ABI, p);

    const [addrs, stats, names] = await factory.getLatestMissions(n);
    addrs.length == 1 
      ? setTitle(`Latest ${addrs.length} Mission`)
      : setTitle(`Latest ${addrs.length} Missions`);

    /* reuse existing grid builder */
    await buildMissionGrid(addrs, stats, names);
  }catch(err){
    console.warn("loadLatestMissions()", err);
  }finally{
    fadeSpinner(spinner, false);
  }
}

async function buildMissionGrid(addrs, stats, names){
  const items = addrs.map((addr, i) => ({
    addr,
    name: names[i],
    status: Number(stats[i]),
    idx: addrs.length - i
  }));
  items.sort((a, b) => (a.status === 5 && b.status !== 5 ? -1 : b.status === 5 && a.status !== 5 ? 1 : b.idx - a.idx));

  missionsList.innerHTML = "";
  missionsList.classList.toggle("empty", items.length === 0);

  if (items.length === 0) {
    const li = document.createElement("li");
    li.className = "mission-empty text-muted";
    li.textContent = "No missions found for this filter.";
    missionsList.appendChild(li);
    return;
  }

  items.forEach(m => {
    const li = document.createElement("li");
    li.className = "mission-item" + (m.status === 3 ? " partly-success" : "");
    li.innerHTML = `
      <span>${m.name}</span>
      <span>${statusText(m.status)}</span>
      <span class="mission-spinner fade-spinner hidden"></span>
    `;
    li.addEventListener("click", () => {
      const sp = li.querySelector(".mission-spinner");
      fadeSpinner(sp, true);
      openMissionModal(m, sp, m.status).finally(() => fadeSpinner(sp, false));
    });
    missionsList.appendChild(li);
  });
  missionsSection.classList.remove("hidden");
}

function setTitle(label) {
  const titleEl = document.getElementById("missionsTitle");
  if (titleEl) {
    titleEl.innerHTML = `<i class="fa-solid fa-list me-2"></i>${label}`;
  }
}

/* ---------- form submit ---------- */
form?.addEventListener("submit", async e => {
  e.preventDefault();
  if(createBtn) setBtnLoading(createBtn, true, "Creating&nbsp;Mission");      

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
      parseInt(f.rounds.value),                // missionRounds
      f.missionName.value.trim()
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
        let msg = decodeError(err);

        if (!msg) msg = err.message || "Transaction failed";
        showAlert(msg, "error");
    } finally {
      setBtnLoading(createBtn,false,"Create Mission",false);                  // always stop
      updateBtn();                                     // re-validate after reset
    }
});

// Navigation icon handlers
document.querySelectorAll(".icon-nav").forEach(btn => {
  btn.addEventListener("click", () => {
    closeMissionModal(false);
    const targetId = btn.getAttribute("data-target");
    adminSections.forEach(sec => {
      if (sec.id === targetId) {
        sec.classList.remove("hidden");
      } else {
        sec.classList.add("hidden");
      }
    });
  });
});

document.addEventListener("DOMContentLoaded", () => {
  document.querySelectorAll('[data-filter]').forEach(item => {
    item.addEventListener('click', e => {
      e.preventDefault();
      const filter = item.getAttribute('data-filter');
      loadMissions(filter); // use unified function
    });
  });
});

document.getElementById("reloadRead")?.addEventListener("click", async ()=>{
  const spinner = document.getElementById("readSpinner");
  if (!spinner) return;

  spinner.classList.add("show");

  // Ensure at least 2s spinner display
  const delay = ms => new Promise(res => setTimeout(res, ms));
  await Promise.all([
    refreshGlobalView(),
    delay(2000)
  ]);

  spinner.classList.remove("show");
});

document.getElementById("reloadWrite")?.addEventListener("click", async ()=>{
  const spinner = document.querySelector("#reloadWrite .fade-spinner");
  if (!spinner) return;

  spinner.classList.add("show");

  // Ensure at least 2s spinner display
  const delay = ms => new Promise(res => setTimeout(res, ms));
  await Promise.all([
    loadFactoryWriteData(),
    delay(2000)
  ]);

  spinner.classList.remove("show");
});

/* ---------- export public functions for admin.html ---------- */
window.triggerRefundModal = triggerRefundModal;
window.openMissionModal   = openMissionModal;
window.setEnrollmentLimits        = setEnrollmentLimits;
window.addAuthorizedAddress       = addAuthorizedAddress;
window.removeAuthorizedAddress    = removeAuthorizedAddress;
window.proposeOwnershipTransfer   = proposeOwnershipTransfer;
window.confirmOwnershipTransfer   = confirmOwnershipTransfer;
window.withdrawFunds              = withdrawFunds;
window.setMaxWithdraw             = setMaxWithdraw;

