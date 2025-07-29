/**********************************************************************
 admin.js – admin page logic (createMission + section gating)
**********************************************************************/
import { connectWallet, disconnectWallet, walletAddress } from "./walletConnect.js";
import { showAlert, showConfirm } from "./core.js";

/* ---------- constants ---------- */
const FACTORY_ADDRESS = "0x10cCD267621bdB51A03FCcc22653884Bd92956AC";
const READ_ONLY_RPC   = "https://evm.cronos.org";   
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
    });
  }else{
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
  if(!allowed){
    showAlert(
      "Connected wallet is neither <b>owner</b> nor <b>authorized</b>.",
      "warning"
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
    }catch (err){
        console.error(err);

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
    }
});