# Solana Sandwich Bot

## Overview

A **Solana Sandwich Bot** is a type of **MEV (Maximal Extractable Value)** bot designed to exploit price discrepancies in Solana's decentralized exchange (**DEX**) transactions by inserting ("sandwiching") its own trades around a victim's transaction.

## Let's Connect!,

<a href="mailto:fenrow325@gmail.com" target="_blank">
  <img src="https://img.shields.io/badge/Gmail-D14836?style=for-the-badge&logo=gmail&logoColor=white" alt="Gmail">
</a>
<a href="https://t.me/fenrow" target="_blank">
  <img src="https://img.shields.io/badge/Telegram-2CA5E0?style=for-the-badge&logo=telegram&logoColor=white" alt="Telegram">
</a>
<a href="https://discord.com/users/fenrow_325" target="_blank">
  <img src="https://img.shields.io/badge/Discord-5865F2?style=for-the-badge&logo=discord&logoColor=white" alt="Discord">
</a>

### Global Metrics
|Metric|Value|
|---|---|
|Proportion of sandwich-inclusive block|2.858%|
|Average sandwiches per block|0.04020|
|Standard Deviation of sandwiches per block|0.31385|


### Stake pool dsitribution (Epoch 777):
|Pool|Stake (SOL)|Pool Share|
|---|---|---|
|Marinade (overall)|4,769,581|53.10%|
| - Marinade Liquid|2,394,101|49.62%|
| - Marinade Native|2,375,480|57.14%|
|Jito|4,802,602|27.59%|
|xSHIN|271,700|27.22%|
|SFDP|4,931,260|12.70%|
|JPool|83,242|7.71%|
|BlazeStake|50,520|4.59%|
|The Vault|17,952|1.14%|
|Aero|1,831|0.36%|

### Honourable Mention
These are hand-picked, visible to the naked eye colluders. If you're staking to them, you should unstake because you placed your trust on validators actively breaking trust.

If your validator is on this list, check the docs of your favourite Solana validator flavour, compile the binaries yourself and make sure to apply any command line arguments as indicated.

|Validator|Stake|Observed Leader Blocks|Weighted Sandwich-inclusive blocks|Weighted Sandwiches|
|---|---|---|---|---|
|Haus – Guaranteed Best APY & No Fees|2,005,970|31,492|1,528.50|1,907.67|
|AG 0% fee + ALL MEV profit share|1,463,103|23,640|2,043.83|2,461.58|
|HM5H...dMRA|1,037,426|14,852|1,128.42|1,635.42|
|BT8L...gziD|807,033|12,284|4,916.08|11,013.92|
|[Marinade Customer] 9fgw...zsXs|362,150|2,704|1,251.92|3,258.25|
|[Marinade/Jito Customer] AltaBlock|276,158|1,932|725.00|1,373.83|
|Blocksmith 🗝️|265,604|5,276|437.50|533.42|

## Preface
Sandwiching refers to the action of forcing the earlier inclusion of a transaction (frontrun) before a transaction published earlier (victim), with another transaction after the victim transaction to realise a profit (backrun), while abusing the victim's slippage settings. We define a sandwich as "a set of transactions that include exactly one frontrun and exactly one backrun transaction, as well as at least one victim transaction", a sandwicher as "a party that sandwiches", and a colluder as "a validator that forwards transactions they receive to a sandwicher".

Some have mentioned that users should issue transactions with lower slippage instead but it's not entirely possible when trading token pairs with extremely high volatility. Being forced to issue transactions with low slippage may lead to higher transaction failure rates and missed opportunities, which is also suboptimal.

The reasons why sandwiching is harmful to the ecosystem had been detailed by [another researcher](https://github.com/a-guard/malicious-validators/blob/main/README.md#why-are-sandwich-attacks-harmful) and shall not be repeated in detail here, but it mainly boils down to breaking trust, transparency and fairness.

We believe that colluder identification should be a continuous effort since [generating new keys](https://docs.anza.xyz/cli/wallets/file-system) to run a new validator is essentially free, and with a certain stake pool willing to sell stake to any validator regardless of operating history, one-off removals will prove ineffective. This repository aims to serve as a tool to continuously identify sandwiches and colluders such that relevant parties can remove stake from sandwichers as soon as possible.

## Key Components

### MEV (Maximal Extractable Value)

- The profit extracted by reordering, inserting, or censoring transactions in a block.
- On Solana, MEV strategies include frontrunning, backrunning, and sandwich attacks.

### Sandwich Attack Mechanics

- Frontrun: The bot places a buy order before the victim’s large buy (increasing price).
- Victim’s Transaction: The victim executes their trade at a worse price due to the bot’s initial trade.
- Backrun: The bot sells the asset immediately after, profiting from the inflated price.

### Solana-Specific Challenges

- High Throughput: Solana’s fast block times (~400ms) require low-latency bots.
- Transaction Parallelization: Solana processes transactions in parallel, making MEV extraction different from Ethereum.
- Priority Fees: Bots must set higher fees to ensure their transactions are prioritized.

### Required Tech Stack

- RPC Nodes (QuickNode, Helius, private nodes): For low-latency transaction data.
- Jito Labs (Jito-Solana client): Optimized for MEV with features like bundled transactions.
- Sealevel Runtime: Understanding Solana’s parallel execution model for efficient MEV.
- Web3.js / @solana/web3.js: For interacting with the Solana blockchain.
- Arbitrage Detection Algorithms: Identifying profitable sandwich opportunities.

## Report Interpretation
The reports consist of 14 columns and their meanings are as follows:
|Column(s)|Meaning|
|---|---|
|leader/vote|The validator's identity and vote account pubkeys|
|name|The validator's name according to onchain data|
|Sc|"Score", normalised weighted number of sandwiches|
|Sc_p|"Presence score", normalised number of blocks with sandwiches, which roughly means proportion of sandwich inclusive blocks|
|R-Sc/R-Sc_p|Unnormalised Sc and Sc_p|
|slots|Number of leader slots observed for the validator|
|Sc_p_{lb\|ub}|Bounds of the confidence interval of the validator's true proportion of sandwich inclusive blocks. Flagged if the lower bound is above the cluster mean|
|Sc_{lb\|ub}|Bounds of the confidence interval of which the validator is considered to have an "average" number of sandwiches per block. Flagged if Sc_p is above the upper bound|
{Sc_p\|Sc}_flag|True if the validator is being flagged due to the respective metric, false otherwise|

---

## 📞 Contact Information
For questions, feedback, or collaboration opportunities, feel free to reach out:

<div align="left">

📧 **Email**: [fenrow325@gmail.com](mailto:fenrow325@gmail.com)  
📱 **Telegram**: [@fenroW](https://t.me/fenrow)  
🎮 **Discord**: [@fenroW](https://discord.com/users/fenrow_325)  

</div>

---