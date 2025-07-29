/**********************************************************************
 admin.js – admin page logic (createMission + section gating)
**********************************************************************/
import { connectWallet, disconnectWallet, walletAddress } from "./walletConnect.js";
import { showAlert, showConfirm } from "./core.js";

/* ---------- constants ---------- */
const FACTORY_ADDRESS = "0x10cCD267621bdB51A03FCcc22653884Bd92956AC";    
const FACTORY_ABI = [
  "function owner() view returns(address)",
  "function authorized(address) view returns(bool)",
  "function createMission(uint8,uint256,uint256,uint256,uint8,uint8,uint256,uint256,uint8) payable returns(address)"
];

/* ---------- DOM ---------- */
const connectBtn     = document.getElementById("connectWalletBtn");
const adminSections  = document.querySelectorAll(".section-box");
const form           = document.getElementById("createMissionForm");

/* ---------- helpers ---------- */
const toUnix = iso => Math.floor(new Date(iso).getTime() / 1000);
const eth    = ethers.utils;                       // alias

function toggleSections(show){
  adminSections.forEach(sec => sec.classList.toggle("hidden", !show));
}

/* ---------- role check ---------- */
async function isOwnerOrAuthorized(addr){
  try{
    const provider = new ethers.providers.Web3Provider(window.ethereum);
    const factory  = new ethers.Contract(FACTORY_ADDRESS, FACTORY_ABI, provider);
    const [ownerAddr, isAuth] = await Promise.all([
      factory.owner(),
      factory.authorized(addr)
    ]);
    return ownerAddr.toLowerCase() === addr || isAuth;
  }catch(e){
    console.error(e);
    return false;
  }
}

/* ---------- wallet button ---------- */
connectBtn.addEventListener("click", () => {
  if(walletAddress){
    showConfirm("Disconnect current wallet?", () => {
      disconnectWallet();
      toggleSections(false);
    });
  }else{
    connectWallet().then(handlePostConnect);
  }
});

/* ---------- after connect / account change ---------- */
async function handlePostConnect(){
  if(!walletAddress) return;
  const allowed = await isOwnerOrAuthorized(walletAddress);
  toggleSections(allowed);
  if(!allowed){
    showAlert("Connected wallet is neither <b>owner</b> nor <b>authorized</b>.","warning");
  }
}

if(window.ethereum){
  window.ethereum.on("accountsChanged", handlePostConnect);
}

/* ---------- auto-connect on page load ---------- */
(document.readyState === "loading"
  ? document.addEventListener("DOMContentLoaded", () => connectWallet().then(handlePostConnect), { once:true })
  : connectWallet().then(handlePostConnect));

/* ---------- form submit ---------- */
form?.addEventListener("submit", async e => {
  e.preventDefault();
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
    form.reset();
  }catch(err){
    console.error(err);
    showAlert(err?.data?.message || err.message,"error");
  }
});