// scripts/deployAll.js
const hre  = require("hardhat");
const fs   = require("fs");
const path = require("path");

// Make undici less eager to timeout for RPC calls
try {
  const { setGlobalDispatcher, Agent } = require("undici");
  setGlobalDispatcher(new Agent({
    headersTimeout: 60_000,
    bodyTimeout: 0,
    keepAliveTimeout: 60_000,
  }));
} catch {}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ✅ Wait using tx.wait(confirmations); fallback to manual polling if only a hash is given
async function waitForConfirmations(txOrHash, confirmations = 5, timeoutMs = 300_000) {
  const provider = hre.ethers.provider;
  let tx = null, hash = null;

  if (typeof txOrHash === "string") {
    hash = txOrHash;
  } else if (txOrHash?.hash) {
    tx = txOrHash; hash = tx.hash;
  } else if (txOrHash?.deploymentTransaction) {
    tx = txOrHash.deploymentTransaction(); hash = tx.hash;
  } else {
    throw new Error("waitForConfirmations: cannot resolve transaction/hash");
  }

  const start = Date.now();
  let attempt = 0;

  if (tx?.wait) {
    while (true) {
      try {
        return await tx.wait(confirmations);
      } catch (e) {
        const msg = String(e?.message || e);
        if (Date.now() - start > timeoutMs) throw e;
        if (/UND_ERR_HEADERS_TIMEOUT|Headers Timeout|timeout|ETIMEDOUT|ECONNRESET/i.test(msg)) {
          attempt++; const backoff = Math.min(10_000, 2_000 * attempt);
          console.log(`⏳ tx.wait transient error; retrying in ${backoff/1000}s…`);
          await sleep(backoff);
          continue;
        }
        throw e;
      }
    }
  }

  // Manual polling path if only a hash was provided
  while (true) {
    try {
      const receipt = await provider.getTransactionReceipt(hash);
      if (receipt?.blockNumber != null) {
        const cur = await provider.getBlockNumber();
        const confs = Math.max(0, cur - receipt.blockNumber + 1);
        if (confs >= confirmations) return receipt;
      }
    } catch (e) {
      const msg = String(e?.message || e);
      if (/UND_ERR_HEADERS_TIMEOUT|Headers Timeout|timeout|ETIMEDOUT|ECONNRESET/i.test(msg)) {
        attempt++; const backoff = Math.min(10_000, 2_000 * attempt);
        console.log(`⏳ RPC timeout while polling; retrying in ${backoff/1000}s…`);
        await sleep(backoff);
        continue;
      }
      throw e;
    }
    if (Date.now() - start > timeoutMs) throw new Error(`waitForConfirmations: timed out after ${timeoutMs/1000}s for ${hash}`);
    await sleep(1_500);
  }
}

async function main() {
  console.log("🚀 Starting full project deployment…");

  const [deployer] = await hre.ethers.getSigners();
  console.log("👤 Deployer:", deployer.address);
  const balance = await hre.ethers.provider.getBalance(deployer.address);
  console.log("💰 Balance:", hre.ethers.formatEther(balance), "CRO");

  // 1) Deploy Mission implementation
  const Mission = await hre.ethers.getContractFactory("Mission");
  const missionImpl = await Mission.deploy({ gasLimit: 12_000_000 });
  await missionImpl.waitForDeployment();
  const missionImplAddress = await missionImpl.getAddress();
  console.log("🧩 Mission implementation deployed at:", missionImplAddress);

  // ✅ Use tx.wait instead of provider.waitForTransaction
  await waitForConfirmations(missionImpl.deploymentTransaction(), 5);

  // 2) Deploy MissionFactory (pass impl)
  const Factory = await hre.ethers.getContractFactory("MissionFactory");
  const missionFactory = await Factory.deploy(missionImplAddress, { gasLimit: 4_000_000 });
  await missionFactory.waitForDeployment();
  const missionFactoryAddress = await missionFactory.getAddress();
  console.log("🏗️  MissionFactory deployed at:", missionFactoryAddress);

  await waitForConfirmations(missionFactory.deploymentTransaction(), 5);

  // 3) Sanity-check
  const getterValue = await missionFactory.missionImplementation();
  console.log("🔍 missionImplementation() returns:", getterValue);

  // 4) Save deployments
  const deploymentInfo = {
    network:        hre.network.name,
    deployer:       deployer.address,
    MissionImpl:    missionImplAddress,
    MissionFactory: missionFactoryAddress,
    funded:         "0 CRO",
    timestamp:      new Date().toISOString()
  };
  const filePath = path.join(__dirname, `../deployments/${hre.network.name}.json`);
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(deploymentInfo, null, 2));
  console.log("📂 Deployment info saved to", filePath);

  // 5) Verify (with retries)
  await sleep(10_000);

  console.log("🎉 All done!");
}

main().catch(err => {
  console.error(err);
  process.exitCode = 1;
});
