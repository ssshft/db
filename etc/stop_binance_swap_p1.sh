ps aux | grep db_binance_swap_p1  | grep -v grep | grep root|  awk '{print $2}' | xargs kill -TERM
