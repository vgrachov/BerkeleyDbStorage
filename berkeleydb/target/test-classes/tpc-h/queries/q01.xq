for
  $l in collection("lineitem")
let
  $retflag := $l/l_returnflag,
  $linstat := $l/l_linestatus
where
  $l/l_shipdate le "1998-09-02"
let
  $disc_charge := $l/l_extendedprice * (1 - $l/l_discount),
  $charge := $l/l_extendedprice * (1 - $l/l_discount) * (1 + $l/l_tax)
order by
  $retflag,
  $linstat
group by
  $retflag,
  $linstat
return
  element lineitem {
    $retflag,
    $linstat,
    element sum_qty { sum($l/l_quantity) },
    element sum_base_price { sum($l/l_extendedprice) },
    element sum_disc_charge { sum($disc_charge) },
    element sum_charge { sum($charge) },
    element avg_qty { avg($l/l_quantity) },
    element avg_price { avg($l/l_extendedprice) },
    element avg_disc { avg($l/l_discount) },
    element count_order { count($retflag) }
  }
