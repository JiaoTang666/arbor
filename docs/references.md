# Methods and references

Arbor, Monoxide and dynamic CLPA run on a shared BlockEmulator-based PBFT engine with matched input and runtime settings.

| Component | Implementation in Arbor | Original work / upstream source |
|---|---|---|
| Monoxide baseline | Address-based account placement with relay transactions on the shared engine | Jiaping Wang and Hao Wang, *Monoxide: Scale out Blockchains with Asynchronous Consensus Zones*, NSDI 2019, pp. 95–112. [Paper](https://www.usenix.org/conference/nsdi19/presentation/wang-jiaping) |
| Dynamic CLPA | Constrained label propagation, periodic repartitioning and account-state transfer | *Achieving Scalability and Load Balance across Blockchain Shards for State Sharding*, SRDS 2022, pp. 284–294. [Paper](https://ieeexplore.ieee.org/document/9996899) · [Upstream implementation](https://github.com/HuangLab-SYSU/block-emulator/blob/main/partition/partition_CLPA.go) |
| BlockEmulator | Native execution engine, PBFT and inherited protocol implementations | Huawei Huang et al., *BlockEmulator: An Emulator Enabling to Test Blockchain Sharding Protocols*, IEEE TSC 18(2), 2025, pp. 690–703. [Paper](https://doi.org/10.1109/TSC.2025.3547222) · [Repository](https://github.com/HuangLab-SYSU/block-emulator) |

The Monoxide comparison evaluates the address-placement and relay mechanism under the common engine. The original Monoxide system also includes proof-of-work consensus zones and Chu-ko-nu mining. Dynamic CLPA uses the partitioning algorithm and state-transfer path implemented in the native engine. These scopes apply to the reference tables and reproduction recipes.

The [upstream project](https://github.com/HuangLab-SYSU/block-emulator#3-published-papers-by-huanglab-using-blockemulator) identifies the SRDS 2022 work as the source of CLPA. The inherited MIT notice is in `native/engine/LICENSE`; dependency attribution is in [THIRD_PARTY_NOTICES.md](../THIRD_PARTY_NOTICES.md).

## Related open-source implementation

**SPRING (third-party reproduction):** [LXINY-111/blockchain-spring](https://github.com/LXINY-111/blockchain-spring).

## Data source

[XBlock-ETH](https://github.com/tczpl/XBlock-ETH) provides the Block Transaction collection. [Official download directory](https://xblock.pro/xblock-eth.html) · [Paper-period archive](https://zhengpeilin.com/download.php?file=18750000to18999999_BlockTransaction.zip).
