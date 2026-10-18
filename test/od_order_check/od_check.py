#!/usr/bin/env python3
"""
ondemand 前向单向语义模拟器 + 六家解析器逐行复刻。

目的：在**不能编译 db** 的本机上，验证 db/src/{htx,bitget,mexc,kucoin,bingx,xt}/*.cpp 的
parseMarketData 是否满足 simdjson::ondemand 的硬约束：
    operator[] 只能找到"当前位置之后"的键；访问已越过的键 → NO_SUCH_FIELD（不抛异常、不崩，
    字段静默为 0）。原实现有 6 处违反，全部是"整条通道零数据"级别的静默失败。

跑法：python3 od_check.py
"""

import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
FRAMES = os.path.join(HERE, "frames")

# ---------- 模拟 simdjson::ondemand ----------

class NoSuchField(Exception):
    pass

class BadType(Exception):
    pass

def _is_int(v):
    return isinstance(v, int) and not isinstance(v, bool)

def _is_num(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)

class Node:
    """一个 ondemand::value。cursor 模拟"已消费到第几个键"。"""
    __slots__ = ("raw", "cursor", "path")

    def __init__(self, raw, path="$"):
        self.raw = raw
        self.cursor = 0
        self.path = path

    # operator[]
    def __getitem__(self, key):
        if not isinstance(self.raw, dict):
            raise BadType(f"{self.path} is not object, want [{key}]")
        keys = list(self.raw.keys())
        try:
            idx = keys.index(key, self.cursor)
        except ValueError:
            raise NoSuchField(
                f"{self.path}[{key!r}] NO_SUCH_FIELD (cursor={self.cursor}, keys={keys})"
            )
        self.cursor = idx + 1
        return Node(self.raw[key], f"{self.path}.{key}")

    def get(self, kind):
        v = self.raw
        if kind == "d":
            if _is_num(v):
                return float(v)
            raise BadType(f"{self.path} want double, got {type(v).__name__}")
        if kind == "i":
            if _is_int(v):
                return v
            raise BadType(f"{self.path} want int64, got {type(v).__name__}={v!r}")
        if kind == "s":
            if isinstance(v, str):
                return v
            raise BadType(f"{self.path} want string, got {type(v).__name__}")
        if kind == "b":
            if isinstance(v, bool):
                return v
            raise BadType(f"{self.path} want bool, got {type(v).__name__}")
        raise AssertionError(kind)

    def begin(self):
        if not isinstance(self.raw, list):
            raise BadType(f"{self.path} is not array")
        return ArrIter(self)

    # range-for：C++ 里 for (auto x : node) 走 begin()/end()
    def __iter__(self):
        return self.begin()

class ArrIter:
    __slots__ = ("node", "i")
    def __init__(self, node):
        self.node = node
        self.i = 0
    def __iter__(self):
        return self
    def __next__(self):
        if self.i >= len(self.node.raw):
            raise StopIteration
        v = self.node.raw[self.i]
        self.i += 1
        return Node(v, f"{self.node.path}[{self.i-1}]")

def root(obj):
    return Node(obj)

# ---------- 各所解析器（严格照 C++ 的访问顺序复刻） ----------

out = []          # (venue, channel, bp, ap, bv, av / px, sz, ts)
typewarn = []
def warn(venue, e):
    typewarn.append(f"{venue}: {e}")

# ---- HTX ----
def htx(doc, mt):
    ch = doc["ch"].get("s")
    p1 = ch.find(".")
    p2 = ch.find(".", p1 + 1)
    if p1 < 0 or p2 < 0:
        return
    inst = ch[p1 + 1:p2]
    tick = doc["tick"]
    if mt == "DEPTH1":
        bp = bv = ap = av = 0.0
        gotBid = gotAsk = False
        bidArr = tick["bid"]
        it = bidArr.begin()
        bp = next(it).get("d")
        bv = next(it).get("d")
        gotBid = True
        askArr = tick["ask"]
        it = askArr.begin()
        ap = next(it).get("d")
        av = next(it).get("d")
        gotAsk = True
        if not (gotBid and gotAsk):
            return
        ts = tick["ts"].get("i")
        out.append(("HTX", "DEPTH1", inst, bp, ap, bv, av, ts))
    else:
        tickTs = tick["ts"].get("i")
        for d in tick["data"]:
            amount = d["amount"].get("d")
            tradeTs = d["ts"].get("i")
            if tradeTs <= 0:
                tradeTs = tickTs
            tid = d["id"].get("i")
            px = d["price"].get("d")
            try:
                d["direction"].get("s")
            except (NoSuchField, BadType):
                pass
            out.append(("HTX", "TRADES", inst, px, amount, tradeTs, tid))

# ---- BITGET ----
def bitget(doc, mt):
    arg = doc["arg"]
    inst = arg["instId"].get("s")
    data = doc["data"]
    if mt == "DEPTH1":
        bp = bv = ap = av = 0.0
        tsMs = 0
        for d in data:
            for lv in d["asks"]:
                it = lv.begin()
                ap = float(next(it).get("s"))
                av = float(next(it).get("s"))
                break
            for lv in d["bids"]:
                it = lv.begin()
                bp = float(next(it).get("s"))
                bv = float(next(it).get("s"))
                break
            tsMs = int(d["ts"].get("s"))
            break
        out.append(("BITGET", "DEPTH1", inst, bp, ap, bv, av, tsMs))
        return
    else:
        for d in data:
            ts = d["ts"].get("s")
            px = d["price"].get("s")
            sz = d["size"].get("s")
            try:
                side = d["side"].get("s")
            except (NoSuchField, BadType):
                side = ""
            try:
                d["tradeId"].get("s")
            except (NoSuchField, BadType):
                pass
            out.append(("BITGET", "TRADES", inst, float(px), float(sz), int(ts), side))

# ---- MEXC ----
def mexc(doc, mt):
    sym = doc["symbol"].get("s")
    data = doc["data"]
    if mt == "DEPTH1":
        cts = data["cts"].get("i")
        ap = av = bp = bv = 0.0
        for lv in data["asks"]:
            it = lv.begin()
            ap = next(it).get("d")
            av = next(it).get("d")
            break
        for lv in data["bids"]:
            it = lv.begin()
            bp = next(it).get("d")
            bv = next(it).get("d")
            break
        out.append(("MEXC", "DEPTH1", sym, bp, ap, bv, av, cts))
    else:
        for d in data:
            px = d["p"].get("d")
            vol = d["v"].get("d")
            d["T"].get("i")
            ts = d["t"].get("i")
            try:
                d["i"].get("s")
            except (NoSuchField, BadType):
                pass
            out.append(("MEXC", "TRADES", sym, px, vol, ts))

# ---- KUCOIN ----
def kucoin(doc, mt):
    data = doc["data"]
    sym = data["symbol"].get("s")
    if mt == "DEPTH1":
        bs = data["bestBidSize"].get("d")
        bp = data["bestBidPrice"].get("s")
        ap = data["bestAskPrice"].get("s")
        asz = data["bestAskSize"].get("d")
        ts = data["ts"].get("i")
        out.append(("KUCOIN", "DEPTH1", sym, float(bp), float(ap), bs, asz, ts))
    else:
        side = data["side"].get("s")
        size = data["size"].get("d")
        price = data["price"].get("s")
        try:
            data["tradeId"].get("s")
        except (NoSuchField, BadType):
            pass
        ts = data["ts"].get("i")
        out.append(("KUCOIN", "TRADES", sym, float(price), size, ts, side))

# ---- BINGX ----
def bingx(doc, mt):
    dt = doc["dataType"].get("s")
    inst = dt.split("@")[0]
    if mt == "DEPTH1":
        ts = doc["ts"].get("i")
        data = doc["data"]
        bp = bv = ap = av = 0.0
        for lv in data["bids"]:
            it = lv.begin()
            bp = float(next(it).get("s"))
            bv = float(next(it).get("s"))
            break
        for lv in data["asks"]:
            it = lv.begin()
            ap = float(next(it).get("s"))
            av = float(next(it).get("s"))
            break
        out.append(("BINGX", "DEPTH1", inst, bp, ap, bv, av, ts))
    else:
        data = doc["data"]
        for d in data:
            q = d["q"].get("s")
            p = d["p"].get("s")
            ts = d["T"].get("i")
            d["m"].get("b")
            out.append(("BINGX", "TRADES", inst, float(p), float(q), ts))

# ---- XT ----
def xt(doc, mt):
    topic = doc["topic"].get("s")
    data = doc["data"]
    sym = data["s"].get("s")
    if mt == "DEPTH1":
        bp = bv = ap = av = 0.0
        for lv in data["a"]:
            it = lv.begin()
            ap = float(next(it).get("s"))
            av = float(next(it).get("s"))
            break
        for lv in data["b"]:
            it = lv.begin()
            bp = float(next(it).get("s"))
            bv = float(next(it).get("s"))
            break
        ts = data["t"].get("i")
        out.append(("XT", "DEPTH1", sym, bp, ap, bv, av, ts))
    else:
        p = data["p"].get("s")
        a = data["a"].get("s")
        data["m"].get("s")
        ts = data["t"].get("i")
        out.append(("XT", "TRADES", sym, float(p), float(a), ts))

# ---------- 驱动器：把抓包帧按 channel 分给两条通道各跑一遍 ----------

FILES = {
    "HTX":    (os.path.join(FRAMES, "fr_htx.jsonl"),    htx,    lambda o: o.get("ch", "").endswith(".bbo"),
                                               lambda o: "trade" in o.get("ch", "")),
    "BITGET": (os.path.join(FRAMES, "fr_bitget.jsonl"), bitget, lambda o: o.get("arg", {}).get("channel") == "books1" and "data" in o,
                                               lambda o: o.get("arg", {}).get("channel") == "trade" and "data" in o),
    "MEXC":   (os.path.join(FRAMES, "fr_mexc.jsonl"),   mexc,   lambda o: o.get("channel") == "push.depth.full",
                                               lambda o: o.get("channel") == "push.deal"),
    "KUCOIN": (os.path.join(FRAMES, "fr_kucoin.jsonl"), kucoin, lambda o: "tickerV2" in o.get("topic", ""),
                                               lambda o: "execution" in o.get("topic", "")),
    "BINGX":  (os.path.join(FRAMES, "fr_bingx.jsonl"),  bingx,  lambda o: o.get("dataType", "").endswith("@depth5"),
                                               lambda o: o.get("dataType", "").endswith("@trade")),
    "XT":     (os.path.join(FRAMES, "fr_xt.jsonl"),     xt,     lambda o: o.get("topic") == "depth",
                                               lambda o: o.get("topic") == "trade"),
}

problems = []
for venue, (path, fn, is_depth, is_trade) in FILES.items():
    for mt, pred in (("DEPTH1", is_depth), ("TRADES", is_trade)):
        n_ok = n_frame = 0
        for line in open(path):
            line = line.strip()
            if not line:
                continue
            try:
                o = json.loads(line)
            except Exception:
                continue
            if not isinstance(o, dict) or not pred(o):
                continue
            n_frame += 1
            before = len(out)
            try:
                fn(root(o), mt)
            except NoSuchField as e:
                problems.append(f"[{venue}/{mt}] 顺序错误 → {e}")
                continue
            except BadType as e:
                problems.append(f"[{venue}/{mt}] 类型错误 → {e}")
                continue
            if len(out) == before:
                problems.append(f"[{venue}/{mt}] 帧未产出记录: {line[:160]}")
            n_ok += 1
        print(f"{venue:7s} {mt:6s} frames={n_frame:3d} parsed_ok={n_ok:3d}")

# ---------- 数值 sanity ----------
def _ts_ms(venue, ts):
    """KuCoin 的 ts 是纳秒，其余是毫秒。统一成毫秒再判量级。"""
    return ts // 1_000_000 if venue == "KUCOIN" else ts

bad = 0
for r in out:
    if r[1] == "DEPTH1":
        _, _, inst, bp, ap, bv, av, ts = r
        if not (bp > 0 and ap > 0 and bp < ap and bv > 0 and av > 0):
            problems.append(f"[{r[0]}/DEPTH1] 数值异常 {r}"); bad += 1
        if not (1_600_000_000_000 <= _ts_ms(r[0], ts) <= 2_000_000_000_000):
            problems.append(f"[{r[0]}/DEPTH1] ts 量级异常 {ts}"); bad += 1
    else:
        # TRADES 记录统一是 (venue, mt, inst, px, sz, ts, ...)
        px, sz, ts = r[3], r[4], r[5]
        if not (px > 0 and sz > 0):
            problems.append(f"[{r[0]}/TRADES] 数值异常 {r}"); bad += 1
        if not (1_600_000_000_000 <= _ts_ms(r[0], ts) <= 2_000_000_000_000):
            problems.append(f"[{r[0]}/TRADES] ts 量级异常 {ts}"); bad += 1

print()
for p in problems[:20]:
    print("  ✗", p)
print(f"\nproblems = {len(problems)}")
