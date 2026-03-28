// Test real wallet data fetching with actual API key
const walletAddress = "0x1677B97859620CcbF4eEcF33f6feB1b7bEA8D97E";

async function testRealWalletData() {
  console.log("Testing real wallet data for:", walletAddress);
  
  const apiKey = process.env.ETHERSCAN_API_KEY;
  console.log("API Key available:", !!apiKey);
  
  if (!apiKey) {
    console.error("No BASESCAN_API_KEY found in environment");
    return;
  }
  
  // Test BASE ETH balance
  try {
    const ethResponse = await fetch(`https://api.basescan.org/api?module=account&action=balance&address=${walletAddress}&tag=latest&apikey=${apiKey}`);
    const ethData = await ethResponse.json();
    console.log("ETH Balance Response:", ethData);
    
    if (ethData.result && ethData.result !== '0') {
      const ethBalance = parseFloat(ethData.result) / Math.pow(10, 18);
      console.log(`Actual ETH Balance: ${ethBalance} ETH`);
    } else {
      console.log("No ETH balance found");
    }
  } catch (error) {
    console.error("ETH balance error:", error);
  }
  
  // Test token list
  try {
    const tokenResponse = await fetch(`https://api.basescan.org/api?module=account&action=tokenlist&address=${walletAddress}&apikey=${apiKey}`);
    const tokenData = await tokenResponse.json();
    console.log("Token Response Status:", tokenData.status);
    console.log("Token Result Length:", tokenData.result?.length || 0);
    
    if (tokenData.result && Array.isArray(tokenData.result)) {
      console.log("Found tokens:");
      tokenData.result.forEach((token, i) => {
        if (i < 10) { // Show first 10 tokens
          const balance = parseFloat(token.balance) / Math.pow(10, parseInt(token.decimals) || 18);
          console.log(`- ${token.symbol}: ${balance} (${token.name})`);
        }
      });
      
      if (tokenData.result.length > 10) {
        console.log(`... and ${tokenData.result.length - 10} more tokens`);
      }
    } else {
      console.log("No tokens found or API error:", tokenData);
    }
  } catch (error) {
    console.error("Token data error:", error);
  }
}

testRealWalletData();