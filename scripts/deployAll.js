const hre   = require("hardhat");
const fs    = require("fs");
const path  = require("path");

async function main() {
  console.log("🚀 Starting full project deployment...");

  const [deployer] = await hre.ethers.getSigners();
  console.log("👤 Deployer:", deployer.address);
  const balance = await hre.ethers.provider.getBalance(deployer.address);
  console.log("💰 Balance:", hre.ethers.formatEther(balance), "ETH");

  let missionFactory;

  // Deploy MissionFactory
  console.log("\n⏳ Deploying MissionFactory...");
  try {
    const MissionFactory = await hre.ethers.getContractFactory("MissionFactory");
    missionFactory = await MissionFactory.deploy({ gasLimit: 8000000 }); // 24-07-2025 Gas was 4,731,905
    await missionFactory.waitForDeployment();
    await new Promise(resolve => setTimeout(resolve, 5000)); // Wait for 5 seconds to ensure deployment is complete
  } catch (error) {
    console.error("Deployment failed:", error);
  }

  const missionFactoryAddress = await missionFactory.getAddress();
  console.log(`✅ MissionFactory deployed at: ${missionFactoryAddress}`);

  // Get Mission implementation address from the factory
  const missionImplAddress = await missionFactory.missionImplementation();
  console.log(`🧩 Mission implementation deployed at: ${missionImplAddress}`);

  // Fund MissionFactory with 10 ETH
  console.log(`\n💸 Funding MissionFactory with 10 ETH...`);
  const tx = await deployer.sendTransaction({
    to: missionFactoryAddress,
    value: hre.ethers.parseEther("10"), // 10 ETH
  });
  await tx.wait();
  console.log("✅ Funding complete!");

  // Save deployment info
  const deploymentInfo = {
    network: hre.network.name,
    deployer: deployer.address,
    MissionFactory: missionFactoryAddress,
    MissionImplementation: missionImplAddress,
    funded: "10 ETH",
    timestamp: new Date().toISOString(),
  };

  const filePath = path.join(__dirname, `../deployments/${hre.network.name}.json`);
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(deploymentInfo, null, 2));

  console.log(`\n📂 Deployment info saved to ${filePath}`);
  console.log("🎉 All done!");
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
