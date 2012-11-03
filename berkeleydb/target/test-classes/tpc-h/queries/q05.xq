for
  $c in collection("customer"),
  $o in collection("orders"),
  $l in collection("lineitem"),
  $s in collection("supplier"),
  $n in collection("nation"),
  $r in collection("region")
let
  $nation_name := $n/n_name
where
  $c/c_custkey = $o/o_custkey
  and $l/l_orderkey = $o/o_orderkey
  and $l/l_suppkey = $s/s_suppkey
  and $c/c_nationkey = $s/s_nationkey
  and $s/s_nationkey = $n/n_nationkey
  and $n/n_regionkey = $r/r_regionkey
  and $r/r_name = "ASIA"
  and $o/o_orderdate >= "1994-01-01"
  and $o/o_orderdate < "1995-01-01"
group by
  $nation_name
let
  $revenue := sum($l/l_extendedprice * (1 - $l/l_discount))
order by
  $revenue descending
return
  element local_supplier_volume {
    $nation_name,
    element revenue { $revenue }
  } 
