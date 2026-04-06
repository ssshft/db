#!/bin/bash
source /inc/version.inc
echo $VERSION
/opt/version/$VERSION/DB/bin/DB /inc/db_binance_swap_p1.json&
