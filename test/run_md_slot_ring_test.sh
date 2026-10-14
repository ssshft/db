#!/usr/bin/env bash
# MdSlotRing 验证脚本（方案文档 §7.1 的"本机能验的"）
#
#   ./run_md_slot_ring_test.sh
#
# 四步：
#   1. 功能测试        —— 原生 arm64 直接跑（环只依赖 std）
#   2. TSan            —— 本方案最强的正确性证据（读写区间不相交）
#   3. ASan + UBSan
#   4. x86-64 codegen  —— 交叉编译 + 反汇编，验证热路径 locked RMW = 0
#
# 第 4 步是"编译 + 反汇编"而不是"运行"：这台机器是 macOS/arm64，
# 能编出 x86-64 机器码并读它，但跑不了（无 Rosetta）。
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
DB="$(cd "$HERE/.." && pwd)"
HFT="$(cd "$DB/.." && pwd)"
CXX="${CXX:-clang++}"
PY="${PY:-python3}"

INC=(-I"$DB/include")
OUT="$HERE/.build"
mkdir -p "$OUT"

FLAGS=(-std=c++17 -O2 -pthread -Wall -Wextra)
fail=0
step() { printf '\n== %s ==\n' "$1"; }

step "1. 功能测试（原生 $(uname -m)）"
"$CXX" "${FLAGS[@]}" "${INC[@]}" "$HERE/md_slot_ring_test.cpp" -o "$OUT/t" \
  && "$OUT/t" || fail=1

step "2. TSan（含 --quick：TSan 慢，用更小的量）"
"$CXX" "${FLAGS[@]}" -fsanitize=thread -g "${INC[@]}" \
  "$HERE/md_slot_ring_test.cpp" -o "$OUT/t_tsan" \
  && "$OUT/t_tsan" --quick || fail=1

step "3. ASan + UBSan"
"$CXX" "${FLAGS[@]}" -fsanitize=address,undefined -fno-omit-frame-pointer -g "${INC[@]}" \
  "$HERE/md_slot_ring_test.cpp" -o "$OUT/t_asan" \
  && "$OUT/t_asan" --quick || fail=1

step "4. x86-64 codegen：locked RMW 必须为 0"
"$CXX" -std=c++17 -O2 -arch x86_64 -S "${INC[@]}" \
  -o "$OUT/probe.s" "$HERE/md_slot_ring_probe.cpp" \
  && "$PY" "$HFT/tests/shm_queue/codegen/rmwcount.py" "$OUT/probe.s" \
     | grep -E "^(function|---)|probe" || fail=1

printf '\n'
if [[ "$fail" == 0 ]]; then
  echo "全部通过。"
else
  echo "有失败项（见上）。"
fi
exit "$fail"
