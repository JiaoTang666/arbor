# Arbor

**面向分片区块链的时间与来源感知账户分配。**

Arbor结合时间账户图、入口森林和增量接入，在关联账户共置的同时分散关联需求。仓库提供Arbor、Monoxide和动态CLPA，包含原生PBFT执行引擎、冻结模型，以及贯穿数据准备、训练、运行和分析的统一CLI。

[English](README.md) · [实验配方](docs/experiments.md) · [数据与模型](docs/data-models.md) · [模块结构](docs/architecture.md) · [运行管理](docs/running.md)

在**12个匹配的分片数与负载配置**下，Arbor平均完整延迟相对Monoxide降低 **64.61%–94.83%**，相对CLPA降低 **74.21%–98.09%**，完整TPS在所有配置中均更高。

![分片拓展与负载响应](docs/assets/scale.png)

*论文参考结果：4／8／12／16片，50%／75%／100%注入负载，每个配置测量一次；延迟采用对数轴。[数据](reference/scale_load.csv) · [绘图脚本](scripts/render_readme.py)。*

## 跑通示例

支持Linux／WSL2，推荐Ubuntu 24.04；需要Python 3.12和C++17编译器。安装器使用已有Go 1.22+，缺少时在本地下载Go 1.22.0。

```bash
sudo apt-get update
sudo apt-get install -y python3.12 python3.12-venv g++ git
git clone https://github.com/JiaoTang666/arbor.git
cd arbor
./install.sh
./arbor run demo
```

示例自动准备合成历史、账户图、入口森林和布局，在本机原生节点上完成**每种方法120笔交易**。安装后运行约需一分钟，实际耗时随主机变化。

![Arbor命令行](docs/assets/cli.svg)

终端展示当前组、运行阶段、交易进度和最终性能表。Ctrl+C退出显示后，队列继续在后台运行。

## 准备数据与运行实验

从[XBlock官方目录](https://xblock.pro/xblock-eth.html)下载Block Transaction数据。论文使用的时期由[`18750000to18999999_BlockTransaction.zip`](https://zhengpeilin.com/download.php?file=18750000to18999999_BlockTransaction.zip)覆盖。将源包保存到`data/raw/`。

### 先跑真实数据小窗口

准备 **10,000笔有效历史交易和1,000笔测试交易**，使用`demo`小规模预设运行三种方法：

```bash
./arbor data xblock \
  --source data/raw/18750000to18999999_BlockTransaction.zip \
  --history-count 10000 --test-count 1000 --output data/xblock-small
./arbor run demo --dataset data/xblock-small \
  --output runs/xblock-small --cleanup-db
./arbor results runs/xblock-small --plot
```

该示例使用真实交易，运行4片、每片4节点。`demo`提供小规模运行设置，`--dataset`指定已准备的XBlock输入，结果与图表保存在`runs/xblock-small`。

### 运行论文规模实验

```bash
./arbor data paper
./arbor data xblock \
  --source data/raw/18750000to18999999_BlockTransaction.zip \
  --history-count 8840117 --test-count 979572 --output data/paper
./arbor run scale --cleanup-db
./arbor results runs/<run-id> --plot
```

| 命令 | 实验 |
|---|---|
| `./arbor run full` | Arbor单次运行：16片、2,000笔／秒 |
| `./arbor run ablation` | Arbor与四项组件消融 |
| `./arbor run halflife` | 27组历史半衰期组合 |
| `./arbor run group-size` | 分组上限16／32／64 |
| `./arbor run tolerance` | 负担容忍度1.05／1.10／1.20 |
| `./arbor run scale` | 三种方法×四种分片数×三档负载 |

`data paper`展示官方链接和准备命令；`data xblock`串联清洗、切分与编码，数量按有效交易计数，`--source`也可传入多个顺序源包。上述命令在本地生成窗口上复现方法流程；参考表使用的历史论文输入由[窗口身份清单](configs/paper-source.json)记录。

论文数据包含 **8,840,117笔历史交易和979,572笔测试交易**。Arbor历史半衰期为 **6h／48h／3.5d**，分组上限 **64**，负担容忍度 **1.10**；在线关系核为 **6h／24h／7d**。每片4节点、区块容量1,000、区块间隔5秒；100%名义注入负载为`200×分片数`笔原始交易／秒。

[`reference/`](reference/)提供论文参考表，新运行结果保存在`runs/`。完整TPS包含注入与排空时间；平均／P95／P99完整延迟从计划释放开始，统计至全部必要阶段完成。

## 配置与运行管理

```bash
./arbor run full --method Monoxide --shards 8 --rate 1200 --dry-run
./arbor run full --config configs/small.yaml --dataset data/demo
./arbor run scale --config my-run.yaml --detach
./arbor status runs/<run-id>
./arbor pause runs/<run-id>
./arbor resume runs/<run-id>
./arbor stop runs/<run-id>
```

配置优先级为CLI、YAML、配方默认值；嵌套资源设置逐项合并。`--dry-run`展示实际任务清单。`pause`让当前组完成后暂停；`stop`停止本次运行的进程并保留文件；`resume`跳过完成组，补做中断的结果保存和清理。交易执行中断后，可检查现场并在新运行目录中重试。

**默认保留数据库。** 添加`--cleanup-db`后，成功组在结果保存完成后清理数据库与原生日志；失败组保留。`--json`便于脚本调用，`--no-color`关闭颜色。

| 配置 | 使用范围 | 启动前可用内存／磁盘余量 |
|---|---|---|
| `demo`、`configs/small.yaml` | 合成交易与真实数据小窗口示例 | 2 GiB／2 GiB |
| 论文配方 | 完整论文数据与原生矩阵 | 300 GiB／数据盘70 GiB、系统盘100 GiB |

论文预设对应原实验主机，可通过YAML的`resources`按数据规模和机器配置调整；小样例配置可直接运行。[`doctor`](docs/running.md)显示主机资源，每组结果记录采样进程RSS。运行器中的历史图与布局准备默认使用8 GiB RSS限额，可通过`resources.offline_rss_gib`调整。

## 训练与使用模型

实验默认使用冻结模型，训练和模型选择均通过显式命令完成：

```bash
./arbor train fusion --small --output runs/fusion
./arbor train admission --small --output runs/admission
./arbor run full --config configs/small.yaml --dataset data/demo \
  --fusion-model runs/fusion/model.json --admission-model runs/admission/model.json
```

完整训练将`--small`换为`--dataset data/paper`。每次运行保存所选模型的快照。[数据与训练文档](docs/data-models.md)介绍自定义输入、数据窗口与时序训练流程。

## 开发

```bash
.venv/bin/pytest -q
.venv/bin/python scripts/check_parity.py
```

Python模块位于`src/arbor`，Go／C++内核位于`native`，实验差异由`configs`配方表达。CI验证构建、单元测试、三方法运行、五项消融、跨语言模型一致性、绘图及训练模型的原生执行。

代码采用MIT许可，保留[BlockEmulator与依赖声明](THIRD_PARTY_NOTICES.md)。[基线与参考文献](docs/references.md)说明复现范围；数据通过XBlock获取并遵循来源条款。

## 作者与引用

**朱嘉诚／Jiacheng Zhu**（第一作者）、**黄杰／Jie Huang**（通讯作者）。

浙江科技大学／Zhejiang University of Science and Technology。

引用信息见[`CITATION.cff`](CITATION.cff)。代码问题与缺陷反馈请通过[GitHub Issues](https://github.com/JiaoTang666/arbor/issues)提交。
