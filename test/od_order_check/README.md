# od_order_check —— 用"前向单向语义模拟器"抓 `parseMarketData` 的访问顺序错误

## 为什么需要它

`db` 的 `BaseUnit` 用的是 `simdjson::ondemand::parser`。ondemand 是**前向单向**迭代器：

- `value[key]` 等价于 `find_field(key)`，**只能找到"当前位置之后"的键**；
- 访问一个已经越过的键 → 返回 `NO_SUCH_FIELD`；
- **不抛异常、不崩、不打日志**。仓库惯例又不检查 `get()` 的返回值 ⇒ 字段静默为 0。

后果是"整条通道零数据"，而订阅日志一切正常 —— 这是排查成本最高的一类故障。
2026-10-17 接六家（HTX/BITGET/MEXC/KUCOIN/BINGX/XT）时，**六家第一版全部**踩了这个坑，
因为真实键顺序既不是字母序、也不是"价格在前"的直觉序，例如：

| 通道 | 实测顺序 | 直觉写法为什么会错 |
|---|---|---|
| KuCoin `tickerV2` | `bestBidSize` 在 `bestBidPrice` **之前** | 先读价再读量 |
| Bitget `books1` | `asks` 在 `bids` 之前；顶层 `ts` 在 `data` **之后** | 先读 bids、先读顶层 ts |
| XT `depth` | `a`(asks) 在 `b`(bids) 之前 | 先读 b |
| BingX `@trade` | 顶层**没有** `ts` | 先探 ts ⇒ 扫到文档末尾 ⇒ 后面的 `data` 也取不到 |
| MEXC `depth.full` | `cts, asks, bids` | 先读 bids |

## 它怎么工作

`od_check.py` 用 ~60 行实现了一个 `simdjson::ondemand` 的最小语义模型：

- 每个对象维护 `cursor`，`node[key]` 用 `keys.index(key, cursor)` **前向**查找；
  找不到抛 `NO_SUCH_FIELD`（对应 C++ 里静默得 0）；
- `get(double|int64|string|bool)` 做**类型**检查，不符抛 `INCORRECT_TYPE`
  （对应 C++ 里 `get(int64_t&)` 读字符串得 0）；
- 数组走 `begin()` / `__iter__`，逐元素前进。

然后把六个 `parseMarketData` **逐行照抄**成 Python（同样的分支、同样的访问顺序），
喂 `frames/*.jsonl` 里的**真实抓包报文**。

## 跑

```bash
cd db/test/od_order_check
python3 od_check.py
```

期望输出（12 行 `parsed_ok == frames`，最后 `problems = 0`）：

```
HTX     DEPTH1 frames=  7 parsed_ok=  7
HTX     TRADES frames=  1 parsed_ok=  1
BITGET  DEPTH1 frames= 37 parsed_ok= 37
...
problems = 0
```

## 失败长什么样

```
✗ [BITGET/DEPTH1] 顺序错误 → $.data[0][bids] NO_SUCH_FIELD (cursor=2, keys=['asks','bids','ts','seq','pseq'])
```

`cursor=2` 表示已经消费到第 2 个键（`ts`），却回头去找第 1 个键 `bids` ——
在 C++ 里就是"深度恒为空，日志干净"。

## 加一家新交易所

1. 在 `frames/` 放该所的抓包（一行一帧，**原始报文**，gzip 的先解压）；
2. 在 `od_check.py` 里加一个 `def <venue>(doc, mt)`，**照抄** `parseMarketData` 的访问顺序
   （包括 `if (!gotBid || !gotAsk) return` 这类提前返回）；
3. 在 `FILES` 里登记 `(路径, 函数, is_depth, is_trade)`；
4. 跑。`problems == 0` 才算过。

## 局限（必须知道）

- 它验证的是**访问顺序 + 字段类型**，**不验证 C++ 能不能编译**（本机没有
  simdjson/fmt/boost，`db` 只能在部署机 `make`）。
- 它不验证数值口径（`info.value` / `magnifyNumber` / `reduceNumber` 的换算），
  那部分靠 `docs/db-新增六家交易所.md` §10 的口径表人工核对。
- `frames/` 是 2026-10-17 的快照。交易所改字段顺序时它抓不到 ——
  所以要定期重新抓一次替换 fixture（抓包脚本见文档 §7）。
