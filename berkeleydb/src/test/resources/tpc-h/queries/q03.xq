for
  $l in collection("lineitem"),
  $o in collection("orders"),
  $c in collection("customer")
let
  $orderkey := $l/l_orderkey,
  $orderdate := $o/o_orderdate,
  $shippriority := $o/o_shippriority
where
  $c/c_mktsegment = "BUILDING"
  and $c/c_custkey = $o/o_custkey
  and $l/l_orderkey = $o/o_orderkey
  and $o/o_orderdate < "1995-03-15"
  and $l/l_shipdate > "1995-03-15"
group by
  $orderkey,
  $orderdate,
  $shippriority
let
  $revenue := sum($l/l_extendedprice * (1 - $l/l_discount))
order by
  $revenue descending,
  $o/o_orderdate
count $rank where $rank le 10
return
  element shipping-priority {
    $orderkey,
    element revenue {$revenue},
    $orderdate,
    $shippriority
  }
