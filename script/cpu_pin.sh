#!/usr/bin/env bash
# =============================================================================
# cpu_pin.sh —— 服务器 CPU 隔离 + 热线程绑核（docs/全系统性能与延迟审查.md 的 S1）
#
# 【解决什么问题】
#   不绑核时，一个自旋线程会被内核在核之间来回搬（migration），而且随时可能被
#   别的进程/中断抢走 CPU。这两件事**几乎不改平均延迟**，只把 p99/p999 拉高 ——
#   所以它在 p50 报表里完全看不见，只有看尾部才看得出来。
#
# 【三层设置，缺一不可】
#   ① 内核层  setup   ：isolcpus / nohz_full / rcu_nocbs 把若干核从调度器手里拿走，
#                       再把中断赶到 housekeeping 核上。**需要重启**。
#   ② 运行时层 setup  ：governor=performance、关 THP、关 irqbalance、IRQ 亲和、ulimit。
#                       重启会丢 ⇒ 装成 systemd service 自动重放。
#   ③ 进程层  pin     ：把每个热线程钉到一颗隔离核上。**进程重启就要重做**。
#
# 【用法】
#   ./cpu_pin.sh check                       # 只读体检，不改任何东西（先跑这个）
#   ./cpu_pin.sh setup --isolate 2-9         # 写内核参数 + 装开机 service（要 root）
#   reboot                                   # 内核参数生效
#   ./cpu_pin.sh pin --tag DB_binance_swap_p1
#   ./cpu_pin.sh status --tag DB_binance_swap_p1
#   ./cpu_pin.sh unpin --tag DB_binance_swap_p1
#
#   setup 的选项：
#     --isolate LIST   要隔离的 CPU 列表，如 2-9。缺省自动挑 4 颗**不同物理核**的高编号 CPU
#     --house LIST     housekeeping 列表，缺省 = 除隔离核之外的全部
#     --no-kernel      只做运行时调优，不碰内核参数（不改 grub、不用重启）
#     --no-runtime     只写内核参数，不做运行时调优
#     --rt             同时关掉 RT 限流（sched_rt_runtime_us=-1，配合 pin --rt 用）
#     --dry-run        只打印将要执行的动作
#
#   pin 的选项：
#     --tag TAG        用 /run/<TAG>.pid 找进程（程序自己写的 pid 文件）
#     --pid PID        直接给 pid
#     --spin-cores LIST 自旋线程用哪些核，缺省取 /proc/cmdline 里的 isolcpus
#     --map TID=CPU,... 手工指定某几个线程（优先级最高，用来兜底）
#     --rt [PRIO]      自旋线程同时设 SCHED_FIFO（缺省 80）
#     --wait SEC       等进程把线程起全再采样，缺省 2 秒
#     --sample SEC     CPU 采样窗口，缺省 1 秒
#     --dry-run
#
# 【两个常见问题的答案】
#   Q: 一个核可以绑多个线程吗？
#   A: 可以。亲和性只是个位图，几个线程共享一颗核完全合法。但**共享 = 分时**：
#      两个都在跑的时候，一个必然等另一个，p99 直接翻倍。所以规矩是：
#        · 会 100% 烧核的自旋线程 → 一核一线程，绝不共享；
#        · 大部分时间在睡的线程（WS io / main / 日志）→ 可以共享，因为它们
#          几乎不会同时醒着，共享的代价接近 0。
#   Q: 只有关键线程需要绑吧？
#   A: 对。绑核买的是"确定性"而不是"速度"——它只消除迁移和邻居干扰，
#      不减少任何计算量。给一个 1 ms 睡一次的 main 线程绑核是纯浪费。
#      判断标准只有一条：**这条线程被打断，会不会变成交易延迟**。
#        db 里：consume() 必须绑；main（1 ms 循环）和日志排水不用绑。
#        ⚠️ WS io 线程是个例外，别照"关键路径就绑"往上套：
#           它确实在最前端（收包时打 tsNet），但 99.9% 时间睡在 epoll 里，
#           而本脚本把网卡中断赶到了 housekeeping。把 io 线程钉到隔离核上
#           = 每次收包都要跨核 IPI 把一颗 idle 的 nohz_full 核叫醒，比不绑更慢；
#           而 nohz_full 的设计前提本来就是"这颗核只跑一个不睡的任务"。
#           所以 io 线程**留在 housekeeping**。真要压它，正确做法是让
#           "io 线程和它那条网卡 RSS 队列的中断同核"，那是另一件事
#           （得先查 /proc/interrupts 的队列→核映射，本脚本目前不做）。
#
# 【需要几颗隔离核 = 有几个 unit **值得独占**】
#   一个 unit 恰好一个 consume 自旋线程。unit 数**不是**币对数，是：
#     Σ over (marketType × instType)  ceil(该组合约数 / tokenLot)
#   （SWAP 还要先按 quote 是否 USD 拆成两组；SPOT 的 FUNDING_RATE 不建 unit。
#     出处 `BaseMarket::generateUnitInfo`，`db/src/base/BaseMarket.cpp:262-360`）
#   例：binance USDT_SWAP 500 个合约、tokenLot=80、marketType=DEPTH1+TRADES+FUNDING_RATE
#       ⇒ 每组 ceil(500/80)=7 ⇒ 3×7 = 21 个 unit。
#   调大 tokenLot 减少 unit（省核，但单线程负载变重）；调小则相反。
#   ⚠️ setup 缺省只挑 4 颗核 —— 需要独占的 unit 多于 4 时必须显式 --isolate 给够，
#      否则 pin 只能让几个自旋线程共享一颗核，独占就没了（p99 直接翻倍）。
#
# 【但不是每个 marketType 都值得独占 —— 分三档】
#   判据不是"这个 marketType 听起来重不重要"，而是**这条数据在不在 tick→trade
#   路径上**。以 `db/etc/small_swap_db.json`（13 家 × DEPTH1+TRADES+FUNDING_RATE，
#   共 38 个 unit）为例：
#
#   ① 值得独占：**DEPTH1**。价差就是拿主动腿/被动腿的 DEPTH1 算的
#      （`dbp/etc/dbptopicid.csv` 里只有 DEPTH1 和 FUNDING_RATE 两种 topic），
#      它晚一微秒，报价就晚一微秒 ⇒ 独占隔离核。
#   ② 不用独占但要保干净：DEPTH1 的 **WS io 线程**（见上面那段，留 housekeeping）。
#   ③ 不值得独占：**TRADES / FUNDING_RATE / KLINE_* / DEPTH20**。
#      · FUNDING_RATE 只在算资金费成本时用，秒级，不在 tick 路径；
#      · TRADES 在 dbp 里**连 topic id 都没有**（`dbptopicid.csv` 只有 4 行），
#        策略侧 `AlgoContext::OnMarketTrade()` 是空函数
#        （`utrade_hft/quant_library/algo/AlgoContext.cpp:399-401`）⇒ 现在没人读；
#      · KLINE_* 没有任何进程订阅；DEPTH20 只有 spot 配置用，而策略只做 USDT_SWAP。
#
#   🔴 **但"不独占" ≠ "不用管"**：`consume()` 里是无条件 `spin::cpu_relax()`
#   （`db/src/base/BaseMarket.cpp:228`），**没消息也 100% 烧核**。38 个 unit 就是
#   38 颗核的需求量；只给 13 个 DEPTH1 独占、剩下 25 个放任自旋，它们会把
#   housekeeping 占满，反过来把 DEPTH1 的 io 线程饿死 —— 比不隔离还糟。
#   所以第三档必须有个去处，两条路（代码改一条，配置改一条）：
#     (a) 首选：让它们睡。已在 `BaseUnit::consume()` 里落地（按 marketType 分档：
#         DEPTH5/10/20 → 200 µs，FUNDING_RATE → 1 ms，KLINE_* → 10 ms），
#         实测 CPU 从 ~100% 掉到 0.0~1.2% ⇒ 核基本还回去了；
#     (b) 退一步：钉到一组"批量核"上互相共享（N 个线程挤 M 颗核），
#         只要别碰 housekeeping 就行 —— 它们本来就不要求延迟。
#
#   ✅ (a) 落地之后，本脚本**不需要按名字分类了**：非关键 unit 不再烧到半核，
#      CPU 采样自然把它们归到 housekeeping，只有 DEPTH1（和暂时还保持自旋的
#      TRADES）会被认成自旋线程去抢隔离核。所以隔离核数 = DEPTH1 unit 数
#      （+ TRADES unit 数，如果你让 TRADES 也自旋）。
#
#   ⚠️ 但这仍然是个"间接"判据：如果哪天有人把退避删了、或某个 unit 突然变忙，
#      它会重新被认成自旋。要精确分类还是得给线程起名
#      （`BaseUnit::start()` 里一行 `pthread_setname_np(pthread_self(), "db.DEPTH1")`，
#      comm 上限 16 字节），脚本再按名字匹配。
#
# 【各进程的线程清单 —— 谁该独占、谁不用绑】
#   db（tag 见 json 的 "tag"，如 DB_small_swap；pid 文件 /run/<tag>.pid）
#     · consume()          × unit 数  → 只有 DEPTH1 / TRADES 这两档要**独占**；
#                                       其余已退避睡眠，留 housekeeping 即可
#     · WS io_thread_      × unit 数  → 不绑（要跟网卡中断同核，见上面那段）
#     · main              × 1         → 不绑（1 ms 循环）
#   dbp（tag = dbprocess；pid 文件 /run/dbprocess.pid）
#     · Execute()          × 1        → **独占 1 颗**（dbp 只有这一个热线程，
#                                       扫全部订阅者 + 算价差 + Commit，纯 while(1) 自旋）
#     · main              × 1         → 不绑（1 ms：log_maintain + metricsPoller）
#     · UpdatePairList()   × 1        → 不绑（sleep 10 s，每 5 分钟读一次 pairlist.xml）
#     ⇒ dbp 不管多少对子都只要 **1 颗**：它是"一个线程扫全部订阅者"
#       （`dbpserver.cpp:445-470` 的三个 for），不是 db 那种"一 unit 一线程"。
#   tb / utrade_hft：尚未逐个盘点，别照抄上面的结论。
#
#   🔴 布局：db 的 DEPTH1 consume（生产者）和 dbp 的 Execute（消费者）是**跨进程**
#      共享内存的上下游 ⇒ 放同一个 NUMA node，最好相邻核；跨 socket 要过 UPI。
#      反过来，两个 100% 自旋的线程**绝不能同核**（分时 ⇒ p99 翻倍）。
#
# 【为什么 pin 是靠"CPU 占用"认线程，而不是靠名字】
#   这套代码里没有任何 pthread_setname_np，所有线程的 comm 都是进程名（"DB"），
#   从 /proc 上分不出谁是 consume、谁是 io 线程。所以这里用"1 秒内烧掉多少 CPU"
#   来认：自旋的 consume() 必然 ~100%；睡了 / 在 epoll 上的接近 0。
#   这个判据只能分出"自旋 vs 睡"两类，**分不出自旋线程之间的 marketType**
#   —— 但它顺带把 io 线程和已退避的 unit 自动归到 housekeeping，
#   正好是上两条想要的（见上面 WS io 线程那段、以及第三档）。
#   想在脚本里单独处理 io 线程（比如跟网卡中断同核），最干净的办法是给线程
#   起名（每个线程一行 pthread_setname_np(pthread_self(), "db.parse")），
#   然后这里按 comm 匹配。注意 comm 只有 16 字节，名字超 15 字符会被截断。
#
# 【注意】
#   · 容器里 cpuset 受限时 taskset 会静默失败 —— 所以每次绑完都回读校验。
#   · isolcpus 在新内核里被标记为 deprecated（推荐 cgroup cpuset），但仍然可用，
#     而且对现有启动脚本零侵入，所以这里用它。
# =============================================================================
set -euo pipefail

# 允许自测时指向假文件树（macOS 上验证纯逻辑用）
PROC="${CPU_PIN_PROC:-/proc}"
SYSFS="${CPU_PIN_SYSFS:-/sys}"
SELF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
INSTALL_PATH="/usr/local/sbin/cpu_pin.sh"
UNIT_PATH="/etc/systemd/system/cpu-pin-tune.service"

# 我们管的内核参数：setup 每次先把自己写过的这几个删掉再加，保证幂等
CMDLINE_KEYS=(isolcpus nohz_full rcu_nocbs irqaffinity skew_tick
              intel_idle.max_cstate processor.max_cstate tsc clocksource idle)

C_RED=$'\033[31m'; C_GRN=$'\033[32m'; C_YEL=$'\033[33m'; C_DIM=$'\033[2m'; C_END=$'\033[0m'
[ -t 1 ] || { C_RED=""; C_GRN=""; C_YEL=""; C_DIM=""; C_END=""; }

info()  { printf '%s\n' "$*"; }
ok()    { printf '%s[ OK ]%s %s\n' "$C_GRN" "$C_END" "$*"; }
warn()  { printf '%s[WARN]%s %s\n' "$C_YEL" "$C_END" "$*"; }
bad()   { printf '%s[FAIL]%s %s\n' "$C_RED" "$C_END" "$*"; }
dim()   { printf '%s%s%s\n' "$C_DIM" "$*" "$C_END"; }
die()   { printf '%s[ERR ]%s %s\n' "$C_RED" "$C_END" "$*" >&2; exit 1; }

need_linux() {
    [ "$(uname -s)" = "Linux" ] || die "只支持 Linux（当前 $(uname -s)）。这个脚本要改内核参数和 /proc，macOS 上没法跑。"
}
need_root() {
    [ "$(id -u)" -eq 0 ] || die "这个子命令要 root。用 sudo 再跑一次。"
}

# ---------------------------------------------------------------------------
# CPU 列表工具
# ---------------------------------------------------------------------------
# 把 "0-3,8,10" 展开成每行一个
expand_list() {
    local part a b i
    IFS=',' read -ra __parts <<< "$1"
    for part in "${__parts[@]}"; do
        [ -z "$part" ] && continue
        if [[ "$part" == *-* ]]; then
            a=${part%-*}; b=${part#*-}
            for (( i = a; i <= b; i++ )); do printf '%s\n' "$i"; done
        else
            printf '%s\n' "$part"
        fi
    done
}

# 把任意顺序的 cpu 列表（每行一个）压成 "0-3,8,10"
fmt_list() {
    sort -n -u | awk '
        NR == 1 { s = $1; p = $1; next }
        $1 == p + 1 { p = $1; next }
        { printf "%s%s", (o++ ? "," : ""), (s == p ? s : s "-" p); s = $1; p = $1 }
        END { if (NR) printf "%s%s\n", (o++ ? "," : ""), (s == p ? s : s "-" p) }
    '
}

# CPU 列表 -> smp_affinity 用的十六进制位图。
# 格式是**逗号分隔的 32 位字**、高位字在前（不是一个大整数！）。
# 踩过的坑：直接用 (1<<c) 累加，c=63 时在 bash 的有符号 64 位里溢出成负数，
# 结果输出一个 16 位的字，内核解析出来的亲和性是错的。
mask_hex() {
    local c wi bi
    local -a w=()
    local maxw=0
    for c in "$@"; do
        wi=$(( c / 32 )); bi=$(( c % 32 ))
        w[wi]=$(( ${w[wi]:-0} | (1 << bi) ))
        (( wi > maxw )) && maxw=$wi
    done
    local out="" i
    for (( i = maxw; i >= 0; i-- )); do
        out+=$(printf '%x' "${w[$i]:-0}")
        (( i > 0 )) && out+=","
    done
    printf '%s\n' "$out"
}

# 挑 want 颗**物理核互不相同**的逻辑 CPU，优先高编号（低编号通常被中断和内核线程占着）。
# 开了超线程时，同一颗物理核的两个逻辑 CPU 共享执行单元 —— 各挑一个等于没隔离。
# 注意：这里刻意只用**下标数组 + 字符串集合**，不用 local -A —— 老 bash（macOS 自带的
# 3.2）不支持关联数组，用了这个脚本在开发机上就没法自测。
pick_cpus() {
    local want=$1
    local max=0 i pkg core key d
    local seen=" "
    local -a out=()
    # 最大 CPU 号从 sysfs 目录名数出来，不用 getconf：容器里 getconf 可能报宿主机的核数
    for d in "${SYSFS}"/devices/system/cpu/cpu[0-9]*; do
        [ -d "$d" ] || continue
        i=${d##*cpu}
        (( i > max )) && max=$i
    done
    for (( i = max; i >= 0; i-- )); do
        [ -d "${SYSFS}/devices/system/cpu/cpu${i}" ] || continue
        if [ -f "${SYSFS}/devices/system/cpu/cpu${i}/topology/core_id" ]; then
            pkg=$(cat "${SYSFS}/devices/system/cpu/cpu${i}/topology/physical_package_id" 2>/dev/null || echo 0)
            core=$(cat "${SYSFS}/devices/system/cpu/cpu${i}/topology/core_id" 2>/dev/null || echo "$i")
            key=" ${pkg}:${core} "
            case "$seen" in
                *"$key"*) continue ;;
            esac
            seen="$seen$pkg:$core "
        fi
        out+=("$i")
        [ "${#out[@]}" -ge "$want" ] && break
    done
    [ "${#out[@]}" -gt 0 ] && printf '%s\n' "${out[@]}"
}

# 当前机器是否开了超线程（任一 cpu 的 thread_siblings_list 多于一个）
smt_on() {
    local f n
    for f in "${SYSFS}"/devices/system/cpu/cpu[0-9]*/topology/thread_siblings_list; do
        [ -f "$f" ] || continue
        n=$(tr ',' '\n' < "$f" | wc -l)
        [ "$n" -gt 1 ] && return 0
    done
    return 1
}

# 从 /proc/cmdline 里取某个参数的值（取不到输出空）
cmdline_get() {
    local k=$1 tok
    [ -r "${PROC}/cmdline" ] || return 0
    for tok in $(tr '\0' '\n' < "${PROC}/cmdline"); do
        case "$tok" in
            "$k"=*) printf '%s\n' "${tok#*=}"; return 0 ;;
        esac
    done
}

# 去掉 isolcpus 里的 managed_irq,domain 前缀，只留 cpu 列表
strip_isolcpus_flags() {
    local v=$1 out=()
    IFS=',' read -ra __p <<< "$v"
    local p
    for p in "${__p[@]}"; do
        case "$p" in
            managed_irq|domain|nohz) ;;
            *) out+=("$p") ;;
        esac
    done
    ( IFS=','; printf '%s\n' "${out[*]}" )
}

# 全部 cpu（每行一个，**按数值排序**）
# comm 是按字典序比较的，两边排序方式必须一致 —— seq 的默认序是字典序
# （10 会排在 2 前面），所以这里必须显式 sort -n，否则 comm 结果会漏/多。
all_cpus() {
    seq 0 "$(( $(nproc) - 1 ))" | sort -n
}

# 在 all_cpus 里去掉给定列表
cpus_except() {
    comm -23 <(all_cpus) <(expand_list "$1" | sort -n) | fmt_list
}

# ---------------------------------------------------------------------------
# check —— 只读体检
# ---------------------------------------------------------------------------
cmd_check() {
    need_linux
    local nproc_n smt="" iso house gov thp irqb
    nproc_n=$(nproc 2>/dev/null || echo '?')

    info "================ 服务器 CPU / 隔离体检 ================"
    info "内核      : $(uname -r)"
    info "逻辑 CPU  : ${nproc_n}"
    if [ -r /etc/os-release ]; then
        # shellcheck disable=SC1091
        . /etc/os-release
        info "发行版    : ${PRETTY_NAME:-$ID}"
    fi
    smt_on && smt="开（会争抢执行单元，建议 BIOS 关掉）" || smt="关"
    info "超线程    : ${smt}"

    # NUMA
    if [ -d "${SYSFS}/devices/system/node" ]; then
        local nodes
        nodes=$(ls -d "${SYSFS}"/devices/system/node/node[0-9]* 2>/dev/null | wc -l)
        info "NUMA 节点 : ${nodes}$([ "$nodes" -gt 1 ] && echo '  ← 双路！进程/内存/中断要绑同一个 node')"
    fi

    info ""
    info "---- ① 内核层（isolcpus 等，需要重启）----"
    iso=$(cmdline_get isolcpus)
    if [ -n "$iso" ]; then
        ok "isolcpus = ${iso}"
    else
        bad "没有 isolcpus —— 隔离核没设，绑核只能防迁移，防不住别的进程来抢"
    fi
    if [ -n "$(cmdline_get nohz_full)" ]; then
        ok "nohz_full = $(cmdline_get nohz_full)"
    else
        warn "没有 nohz_full —— 隔离核上仍会每 1~4 ms 被打一次调度 tick（尾延迟 +几 µs）"
    fi
    if [ -n "$(cmdline_get rcu_nocbs)" ]; then
        ok "rcu_nocbs = $(cmdline_get rcu_nocbs)"
    else
        warn "没有 rcu_nocbs —— RCU 回调会在隔离核上跑，nohz_full 的效果打折"
    fi
    if [ -e "${SYSFS}/devices/system/cpu/nohz_full" ]; then
        ok "内核支持 NO_HZ_FULL"
    else
        warn "这个内核没有 NO_HZ_FULL（CONFIG_NO_HZ_FULL=n），nohz_full 参数会被忽略"
    fi

    info ""
    info "---- ② 运行时层（重启会丢，需开机重放）----"
    local cpus_bad=()
    for f in "${SYSFS}"/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_governor; do
        [ -f "$f" ] || continue
        gov=$(cat "$f" 2>/dev/null || echo '?')
        [ "$gov" != "performance" ] && cpus_bad+=("$(basename "$(dirname "$(dirname "$f")")")")
    done
    if [ "${#cpus_bad[@]}" -eq 0 ]; then
        ok "governor = performance"
    else
        bad "governor 不是 performance（${#cpus_bad[@]} 颗核）—— 变频会带来几十 µs 的抖动"
    fi

    thp=$(cat "${SYSFS}/kernel/mm/transparent_hugepage/enabled" 2>/dev/null || echo '?')
    case "$thp" in
        *"[never]"*) ok "透明大页 = never" ;;
        *)           warn "透明大页未关（$thp）—— khugepaged 会在后台抢 CPU" ;;
    esac

    if command -v systemctl >/dev/null 2>&1; then
        irqb=$(systemctl is-active irqbalance 2>/dev/null || true)
        case "$irqb" in
            active) bad "irqbalance 在跑 —— 它会把中断随时搬到隔离核上，必须关掉" ;;
            *)      ok "irqbalance 未运行" ;;
        esac
    fi

    local shm
    shm=$(df -h /dev/shm 2>/dev/null | awk 'NR==2{print $2}')
    [ -n "$shm" ] && info "        /dev/shm = ${shm}（所有 SHM 队列都在这上面）"

    info "        ulimit -n = $(ulimit -n)  memlock = $(ulimit -l)"

    info ""
    info "---- ③ 进程层（进程重启就要重做）----"
    local pids
    pids=$(pgrep -x DB 2>/dev/null || true)
    if [ -z "$pids" ]; then
        dim "        db 没在跑（找进程名 DB）"
    else
        for p in $pids; do
            local n_spin=0
            for t in "${PROC}/$p"/task/*; do
                [ -d "$t" ] || continue
                n_spin=$((n_spin + 1))
            done
            info "        DB pid=${p} 线程数=${n_spin}  CPU 亲和=$(taskset -pc "$p" 2>/dev/null | sed 's/.*: //')"
        done
    fi

    info ""
    info "---- 结论 ----"
    if [ -z "$iso" ]; then
        info "  还没做隔离：先 ./cpu_pin.sh setup --isolate <列表>，然后 reboot。"
    else
        info "  隔离已配置：再跑 ./cpu_pin.sh pin --tag <TAG> 把热线程钉上去。"
    fi
    dim "  提示：绑核前后必须对比 p99，而不是 p50 —— 绑核改善的是尾部。"
}

# ---------------------------------------------------------------------------
# runtime —— 重启会丢的那部分（开机 service 调它）
# ---------------------------------------------------------------------------
cmd_runtime() {
    need_linux
    local quiet=0
    [ "${1:-}" = "--quiet" ] && quiet=1
    say() { [ "$quiet" -eq 1 ] || info "$*"; }

    # governor
    local n=0
    for f in "${SYSFS}"/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_governor; do
        [ -f "$f" ] || continue
        echo performance > "$f" 2>/dev/null && n=$((n + 1)) || true
    done
    say "governor: 已设 performance（$n 颗核）"

    # 透明大页
    for f in "${SYSFS}/kernel/mm/transparent_hugepage/enabled" "${SYSFS}/kernel/mm/transparent_hugepage/defrag"; do
        [ -f "$f" ] && echo never > "$f" 2>/dev/null || true
    done
    say "透明大页: never"

    # irqbalance：它专门跟隔离核对着干
    if command -v systemctl >/dev/null 2>&1 && systemctl is-enabled irqbalance >/dev/null 2>&1; then
        systemctl disable --now irqbalance >/dev/null 2>&1 || true
        say "irqbalance: 已关闭并禁用开机自启"
    fi

    # 把所有中断的亲和性从隔离核上挪走
    local iso house house_mask moved=0
    iso=$(cmdline_get isolcpus); iso=$(strip_isolcpus_flags "$iso")
    if [ -n "$iso" ]; then
        house=$(cmdline_get irqaffinity)
        if [ -z "$house" ]; then
            house=$(cpus_except "$iso")
        fi
        # shellcheck disable=SC2046
        house_mask=$(mask_hex $(expand_list "$house"))
        for d in "${PROC}"/irq/[0-9]*; do
            [ -f "$d/smp_affinity" ] || continue
            echo "$house_mask" > "$d/smp_affinity" 2>/dev/null && moved=$((moved + 1)) || true
        done
        say "中断亲和: ${moved} 个 IRQ 已挪到 housekeeping（${house}）"
    fi
}

# ---------------------------------------------------------------------------
# setup —— 内核参数 + 开机 service
# ---------------------------------------------------------------------------
cmd_setup() {
    need_linux; need_root
    local iso="" house="" do_kernel=1 do_runtime=1 rt=0 dry=0
    while [ $# -gt 0 ]; do
        case "$1" in
            --isolate)    iso=${2:?}; shift 2 ;;
            --house)      house=${2:?}; shift 2 ;;
            --no-kernel)  do_kernel=0; shift ;;
            --no-runtime) do_runtime=0; shift ;;
            --rt)         rt=1; shift ;;
            --dry-run)    dry=1; shift ;;
            *) die "setup 不认识的选项: $1" ;;
        esac
    done

    local ncpu; ncpu=$(nproc)
    [ -z "$iso" ] && iso=$(pick_cpus 4 | fmt_list)
    [ -z "$house" ] && house=$(cpus_except "$iso")
    [ -z "$house" ] && die "housekeeping 为空 —— 必须留至少一颗核给内核和中断"

    # 隔离核和 house 不能重叠
    if [ -n "$(comm -12 <(expand_list "$iso" | sort -n) <(expand_list "$house" | sort -n))" ]; then
        die "--isolate 和 --house 有重叠：iso=${iso} house=${house}"
    fi

    info "================ CPU 隔离配置 ================"
    info "隔离核 (isolated)   : ${iso}"
    info "管家核 (housekeeping): ${house}"
    smt_on && warn "超线程开着：已按物理核去重挑核，但最好在 BIOS 里关掉（S5）"
    if [ "$ncpu" -gt 64 ]; then
        warn "逻辑核 > 64：smp_affinity 的位图需要多字（>2 个 32 位字），脚本只处理到 64"
    fi

    if [ "$do_kernel" -eq 1 ]; then
        local args="isolcpus=managed_irq,domain,${iso} nohz_full=${iso} rcu_nocbs=${iso} irqaffinity=${house} skew_tick=1 intel_idle.max_cstate=0 processor.max_cstate=1 tsc=reliable"

        info ""
        info "---- 内核参数 ----"
        info "  将写入: ${args}"

        # 先确认内核支持 NO_HZ_FULL，不支持就别写（写了会被静默忽略，排查时很坑）
        if [ ! -e "${SYSFS}/devices/system/cpu/nohz_full" ]; then
            warn "内核没有 CONFIG_NO_HZ_FULL，nohz_full 会被忽略（其余参数照常生效）"
        fi

        if [ "$dry" -eq 1 ]; then
            dim "  (dry-run，不改 grub)"
        elif command -v grubby >/dev/null 2>&1; then
            # RHEL / CentOS / Alma / Rocky
            local k
            for k in "${CMDLINE_KEYS[@]}"; do
                grubby --update-kernel=ALL --remove-args="$k" >/dev/null 2>&1 || true
            done
            grubby --update-kernel=ALL --args="$args" >/dev/null
            ok "已写入 grubby（--update-kernel=ALL）"
        else
            # Debian / Ubuntu：用 drop-in，不直接改 /etc/default/grub（好回滚：删一个文件）
            mkdir -p /etc/default/grub.d
            local dropin=/etc/default/grub.d/99-hft-cpu-pin.cfg
            cat > "$dropin" <<EOF
# 由 cpu_pin.sh 生成。删掉这个文件 + update-grub 即可完整回滚。
GRUB_CMDLINE_LINUX="\$GRUB_CMDLINE_LINUX ${args}"
EOF
            if command -v update-grub >/dev/null 2>&1; then
                update-grub >/dev/null 2>&1 || die "update-grub 失败"
            else
                grub-mkconfig -o /boot/grub/grub.cfg >/dev/null 2>&1 || die "grub-mkconfig 失败"
            fi
            # 回读校验：不校验的话，drop-in 没被 source 到会静默无效
            if grep -q "isolcpus=" /boot/grub/grub.cfg 2>/dev/null; then
                ok "已写入 ${dropin} 并重建 grub.cfg（回读校验通过）"
            else
                warn "grub.cfg 里没找到 isolcpus —— 这个发行版的 grub 可能不读 /etc/default/grub.d"
                warn "回滚办法：rm ${dropin} && update-grub，然后手工把参数加到 GRUB_CMDLINE_LINUX"
            fi
        fi
        info "  ${C_YEL}★ 必须重启才生效${C_END}"
    fi

    if [ "$do_runtime" -eq 1 ]; then
        info ""
        info "---- 运行时调优 + 开机重放 ----"
        if [ "$dry" -eq 1 ]; then
            dim "  (dry-run，不执行)"
        else
            # 把脚本装到固定路径，service 才能引用（repo 挪走了也不会失效）
            if [ "$(readlink -f "$SELF")" != "$(readlink -f "$INSTALL_PATH" 2>/dev/null || echo)" ]; then
                install -m 0755 "$SELF" "$INSTALL_PATH"
            fi
            cat > "$UNIT_PATH" <<EOF
[Unit]
Description=HFT CPU pinning runtime tuning (governor / THP / irqbalance / IRQ affinity)
After=multi-user.target network-online.target
Wants=network-online.target

[Service]
Type=oneshot
ExecStart=${INSTALL_PATH} runtime --quiet
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF
            systemctl daemon-reload
            systemctl enable cpu-pin-tune.service >/dev/null 2>&1 || true
            cmd_runtime
            ok "已安装 ${UNIT_PATH}（每次开机自动重放运行时调优）"

            # RT 限流：**不是内核启动参数**，是 sysctl。写在 cmdline 上会被静默忽略。
            # 为什么要关：SCHED_FIFO 的自旋线程会被内核按 950000/1000000 限流 ——
            # 每 1 秒硬停 50 ms，对自旋线程是灾难。只在明确要开 --rt 时才关。
            if [ "$rt" -eq 1 ]; then
                echo 'kernel.sched_rt_runtime_us = -1' > /etc/sysctl.d/99-hft-rt.conf
                sysctl -p /etc/sysctl.d/99-hft-rt.conf >/dev/null 2>&1 || true
                ok "已关 RT 限流（/etc/sysctl.d/99-hft-rt.conf）"
            fi
            # ulimit 也要持久化
            cat > /etc/security/limits.d/99-hft.conf <<'EOF'
# 由 cpu_pin.sh 生成。SHM 队列 + ws 连接多，默认 1024 不够。
*    soft nofile  1048576
*    hard nofile  1048576
*    soft memlock unlimited
*    hard memlock unlimited
EOF
            ok "已写入 /etc/security/limits.d/99-hft.conf（重新登录生效）"
        fi
    fi

    info ""
    info "下一步："
    [ "$do_kernel" -eq 1 ] && info "  1) reboot"
    info "  2) ./cpu_pin.sh check      # 确认 isolcpus 生效"
    info "  3) ./cpu_pin.sh pin --tag <进程 tag>"
}

# ---------------------------------------------------------------------------
# pin —— 把热线程钉到隔离核
# ---------------------------------------------------------------------------
thread_ticks() {   # pid tid -> utime+stime（单位 tick）
    local pid=$1 tid=$2 v
    # 线程可能在枚举之后、采样之前退出 ⇒ stat 读不到。这里必须兜成 0，
    # 否则上层 $(( )) 会拿到空串直接语法错误（set -u 也救不了）。
    v=$(awk -F')' '{print $2}' "${PROC}/$pid/task/$tid/stat" 2>/dev/null \
        | awk '{print $12 + $13}' 2>/dev/null) || v=""
    printf '%s\n' "${v:-0}"
}
thread_comm() {
    cat "${PROC}/$1/task/$2/comm" 2>/dev/null || echo '?'
}
thread_allowed() {
    awk '/^Cpus_allowed_list/{print $2}' "${PROC}/$1/task/$2/status" 2>/dev/null || echo '?'
}

cmd_pin() {
    need_linux; need_root
    local tag="" pid="" spin="" map="" rt=0 prio=80 wait=2 sample=1 dry=0
    while [ $# -gt 0 ]; do
        case "$1" in
            --tag)        tag=${2:?}; shift 2 ;;
            --pid)        pid=${2:?}; shift 2 ;;
            --spin-cores) spin=${2:?}; shift 2 ;;
            --map)        map=${2:?}; shift 2 ;;
            --rt)         rt=1; shift; [ $# -gt 0 ] && [[ "${1:-}" =~ ^[0-9]+$ ]] && { prio=$1; shift; } ;;
            --wait)       wait=${2:?}; shift 2 ;;
            --sample)     sample=${2:?}; shift 2 ;;
            --dry-run)    dry=1; shift ;;
            *) die "pin 不认识的选项: $1" ;;
        esac
    done

    if [ -z "$pid" ]; then
        [ -n "$tag" ] || die "要给 --tag <tag>（读 /run/<tag>.pid）或 --pid <pid>"
        [ -r "/run/${tag}.pid" ] || die "/run/${tag}.pid 不存在 —— 进程没起，或者 tag 写错了"
        pid=$(cat "/run/${tag}.pid")
        kill -0 "$pid" 2>/dev/null || die "/run/${tag}.pid 里的 pid=${pid} 已经不在了"
    fi
    [ -d "${PROC}/${pid}/task" ] || die "${PROC}/${pid}/task 不存在"

    # 自旋线程用哪些核：命令行 > isolcpus > 报错
    if [ -z "$spin" ]; then
        spin=$(strip_isolcpus_flags "$(cmdline_get isolcpus)")
        [ -n "$spin" ] || die "没设 --spin-cores，/proc/cmdline 里也没有 isolcpus。先跑 setup + reboot，或显式给 --spin-cores 2-9"
        ok "从 /proc/cmdline 读到隔离核 ${spin}"
    else
        [ -n "$(strip_isolcpus_flags "$(cmdline_get isolcpus)")" ] || \
            warn "内核参数里没有 isolcpus —— 现在只是把线程钉住了，核本身还是公用的（别的进程/中断能抢）。真正的隔离要 setup + reboot"
    fi
    # shellcheck disable=SC2207
    local -a spin_cpus=($(expand_list "$spin" | sort -n))

    # 其余线程：给 housekeeping（有 irqaffinity 就用它，否则用全部非隔离核）
    local house
    house=$(cmdline_get irqaffinity)
    [ -z "$house" ] && house=$(cpus_except "$spin")
    [ -n "$house" ] || die "housekeeping 为空（所有核都被隔离了？）—— 必须留至少一颗核"

    info "进程 ${pid}（$(cat "${PROC}/${pid}/comm" 2>/dev/null || echo '?')）"
    info "自旋线程核 : ${spin_cpus[*]}"
    info "其余线程核 : ${house}"
    info "等 ${wait}s 让线程起全..."
    sleep "$wait"

    local -a tids=()
    local t
    for t in "${PROC}/${pid}"/task/*; do [ -d "$t" ] && tids+=("$(basename "$t")"); done
    [ "${#tids[@]}" -gt 0 ] || die "没枚举到任何线程"

    # ---- 采样 CPU 占用，认出自旋线程 ----
    # 用**下标数组按 tid 索引**（稀疏下标），不用关联数组：tid 是数字，够用，
    # 而且老 bash 也支持。macOS 自带的 bash 3.2 不支持 local -A。
    local -a t0=() want=()
    for t in "${tids[@]}"; do t0[t]=$(thread_ticks "$pid" "$t"); done
    sleep "$sample"
    local hz; hz=$(getconf CLK_TCK 2>/dev/null || echo 100)
    local thr=$(( hz * sample / 2 ))   # 半个核以上就算自旋

    local -a spin_tids=() other_tids=()
    local d
    for t in "${tids[@]}"; do
        d=$(( $(thread_ticks "$pid" "$t") - ${t0[t]:-0} ))
        if [ "$d" -ge "$thr" ]; then spin_tids+=("$t"); else other_tids+=("$t"); fi
    done

    info ""
    info "识别结果：自旋线程 ${#spin_tids[@]} 个，其余 ${#other_tids[@]} 个"
    if [ "${#spin_tids[@]}" -eq 0 ]; then
        warn "一个自旋线程都没认出来（阈值 ${thr} tick/${sample}s）—— 进程可能还没开始跑，或已经不烧核了"
    fi
    if [ "${#spin_tids[@]}" -gt "${#spin_cpus[@]}" ]; then
        warn "自旋线程比隔离核多（${#spin_tids[@]} > ${#spin_cpus[@]}）⇒ 只能共享核，独占没了，p99 会翻倍。"
        warn "  先别急着加核 —— 先看这些自旋线程里有多少是 TRADES/FUNDING_RATE/KLINE 这类"
        warn "  不在 tick→trade 路径上的 unit（见脚本头部【但不是每个 marketType 都值得独占】）。"
        warn "  它们本该改成 spin::backoff_sleep 或直接从配置里去掉，而不是抢隔离核。"
        warn "  剩下的真关键 unit（DEPTH1）才是该给独占核的；还多就 setup --isolate 加核，"
        warn "  或调大 tokenLot 减少 unit。"
    fi

    # ---- 应用 ----
    local i=0 applied=0
    for t in "${spin_tids[@]}"; do
        want[t]=${spin_cpus[$(( i % ${#spin_cpus[@]} ))]}
        i=$(( i + 1 ))
    done
    for t in "${other_tids[@]}"; do want[t]=$house; done
    # --map 优先级最高
    if [ -n "$map" ]; then
        local kv
        for kv in ${map//,/ }; do want[${kv%%=*}]=${kv##*=}; done
    fi

    for t in "${tids[@]}"; do
        local cpu=${want[t]:-$house}
        local comm; comm=$(thread_comm "$pid" "$t")
        if [ "$dry" -eq 1 ]; then
            printf '  %-8s %-16s -> %s\n' "$t" "$comm" "$cpu"
            continue
        fi
        if taskset -pc "$cpu" "$t" >/dev/null 2>&1; then
            [ "$rt" -eq 1 ] && [ "${want[t]:-}" != "$house" ] && chrt -f -p "$prio" "$t" >/dev/null 2>&1 || true
            applied=$((applied + 1))
        else
            bad "线程 ${t}（${comm}）绑定失败 —— 容器 cpuset 受限？"
        fi
    done
    [ "$dry" -eq 1 ] && { dim "(dry-run，未实际绑定)"; return 0; }

    info "已绑定 ${applied}/${#tids[@]} 个线程"
    info ""
    cmd_status --pid "$pid" --quiet
}

# ---------------------------------------------------------------------------
# status / unpin
# ---------------------------------------------------------------------------
cmd_status() {
    need_linux
    local tag="" pid="" quiet=0
    while [ $# -gt 0 ]; do
        case "$1" in
            --tag)   tag=${2:?}; shift 2 ;;
            --pid)   pid=${2:?}; shift 2 ;;
            --quiet) quiet=1; shift ;;
            *) die "status 不认识的选项: $1" ;;
        esac
    done
    if [ -z "$pid" ]; then
        [ -n "$tag" ] || die "要给 --tag 或 --pid"
        pid=$(cat "/run/${tag}.pid" 2>/dev/null) || die "/run/${tag}.pid 不存在"
    fi

    local iso; iso=$(strip_isolcpus_flags "$(cmdline_get isolcpus)")
    local -a iso_set=()
    local c
    for c in $(expand_list "$iso"); do iso_set[c]=1; done

    [ "$quiet" -eq 0 ] && printf '%-8s %-18s %-12s %s\n' TID COMM 亲和 CPU是否隔离
    local t allowed mark
    for t in "${PROC}/${pid}"/task/*; do
        [ -d "$t" ] || continue
        t=$(basename "$t")
        allowed=$(thread_allowed "$pid" "$t")
        # 单个 cpu 且在隔离集合里 = 正确绑好
        mark="-"
        if [[ "$allowed" =~ ^[0-9]+$ ]] && [ -n "${iso_set[allowed]:-}" ]; then mark="隔离核"; fi
        printf '%-8s %-18s %-12s %s\n' "$t" "$(thread_comm "$pid" "$t")" "$allowed" "$mark"
    done
    if [ "$quiet" -eq 0 ] && [ -z "$iso" ]; then
        dim "（没有 isolcpus ⇒ 就算绑上了，核也可能被别的进程抢）"
    fi
}

cmd_unpin() {
    need_linux; need_root
    local tag="" pid=""
    while [ $# -gt 0 ]; do
        case "$1" in
            --tag) tag=${2:?}; shift 2 ;;
            --pid) pid=${2:?}; shift 2 ;;
            *) die "unpin 不认识的选项: $1" ;;
        esac
    done
    [ -z "$pid" ] && { [ -n "$tag" ] || die "要给 --tag 或 --pid"; pid=$(cat "/run/${tag}.pid"); }
    local all; all=$(seq 0 "$(( $(nproc) - 1 ))" | fmt_list)
    local t
    for t in "${PROC}/${pid}"/task/*; do
        [ -d "$t" ] || continue
        taskset -pc "$all" "$(basename "$t")" >/dev/null 2>&1 || true
        chrt -o -p 0 "$(basename "$t")" >/dev/null 2>&1 || true
    done
    ok "已解除 pid=${pid} 的绑核（全部线程放回 ${all}）"
}

# ---------------------------------------------------------------------------
# selftest —— 用假文件树验证纯逻辑（macOS 上也能跑）
# ---------------------------------------------------------------------------
cmd_selftest() {
    local tmp; tmp=$(mktemp -d)
    mkdir -p "$tmp/sys/devices/system/cpu"
    # 造 16 个逻辑核、8 颗物理核（开超线程：cpu i 与 cpu i+8 是兄弟）
    local i
    for i in $(seq 0 15); do
        mkdir -p "$tmp/sys/devices/system/cpu/cpu$i/topology"
        echo 0 > "$tmp/sys/devices/system/cpu/cpu$i/topology/physical_package_id"
        echo $(( i % 8 )) > "$tmp/sys/devices/system/cpu/cpu$i/topology/core_id"
    done
    for i in $(seq 0 7); do
        echo "$i,$(( i + 8 ))" > "$tmp/sys/devices/system/cpu/cpu$i/topology/thread_siblings_list"
        echo "$i,$(( i + 8 ))" > "$tmp/sys/devices/system/cpu/cpu$(( i + 8 ))/topology/thread_siblings_list"
    done

    local fail=0
    check() { if [ "$2" = "$3" ]; then ok "$1 => $2"; else bad "$1 => 得到 '$2'，期望 '$3'"; fail=1; fi; }

    check "expand_list 0-3,8"   "$(expand_list '0-3,8' | tr '\n' ' ' | sed 's/ $//')" "0 1 2 3 8"
    check "fmt_list 0 1 2 3 8 10" "$(printf '0\n1\n2\n3\n8\n10\n' | fmt_list)" "0-3,8,10"
    check "fmt_list 单核"        "$(printf '5\n' | fmt_list)" "5"
    check "mask_hex 0,1"        "$(mask_hex 0 1)" "3"
    check "mask_hex 4"          "$(mask_hex 4)" "10"
    # cpu 32 落在第二个 32 位字里 ⇒ "字1,字0" = "1,0"（高位字在前，不是一个大整数）
    check "mask_hex 32（跨字）"  "$(mask_hex 32)" "1,0"
    check "mask_hex 0..63 全选"  "$(mask_hex $(seq 0 63))" "ffffffff,ffffffff"
    check "strip_isolcpus_flags" "$(strip_isolcpus_flags 'managed_irq,domain,2-9')" "2-9"
    check "strip_isolcpus_flags 无前缀" "$(strip_isolcpus_flags '2-9')" "2-9"

    # 挑核：应该挑出物理核互不相同的 4 颗，且都是高编号
    CPU_PIN_SYSFS="$tmp/sys" bash -c "
        SYSFS='$tmp/sys'
        $(declare -f pick_cpus)
        pick_cpus 4 | sort -n | tr '\n' ' '
    " > "$tmp/picked"
    check "pick_cpus 4（16 逻辑核/8 物理核）" "$(cat "$tmp/picked")" "12 13 14 15 "

    CPU_PIN_SYSFS="$tmp/sys" bash -c "
        SYSFS='$tmp/sys'
        $(declare -f smt_on)
        smt_on && echo yes || echo no
    " > "$tmp/smt"
    check "smt_on" "$(cat "$tmp/smt")" "yes"

    rm -rf "$tmp"
    [ "$fail" -eq 0 ] && { info ""; ok "selftest 全部通过"; return 0; } || { info ""; bad "selftest 有失败"; return 1; }
}

usage() {
    sed -n '/^# 【用法】/,/^# 【两个常见问题/p' "$SELF" | sed 's/^# \?//' | head -40
}

main() {
    local cmd=${1:-help}; shift || true
    case "$cmd" in
        check)    cmd_check "$@" ;;
        setup)    cmd_setup "$@" ;;
        runtime)  cmd_runtime "$@" ;;
        pin)      cmd_pin "$@" ;;
        status)   cmd_status "$@" ;;
        unpin)    cmd_unpin "$@" ;;
        selftest) cmd_selftest ;;
        help|-h|--help) usage ;;
        *) die "不认识的子命令: $cmd（check|setup|runtime|pin|status|unpin|selftest）" ;;
    esac
}

main "$@"
