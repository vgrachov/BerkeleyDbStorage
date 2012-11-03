for
  $p in collection("part"),
  $s in collection("supplier"),
  $l in collection("lineitem"),
  $ps in collection("partsupp"),
  $o in collection("orders"),
  $n in collection("nation")
where
  $s/s_suppkey = $l/l_suppkey
  and $ps/ps_suppkey = $l/l_suppkey
  and $ps/ps_partkey = $l/l_partkey
  and $p/p_partkey = $l/l_partkey
  and $o/o_orderkey = $l/l_orderkey
  and $s/s_nationkey = $n/n_nationkey
  and contains($p/p_name, "green")
let
  $nation := $n/n_name,
  $o_year := substring($o/o_orderdate, 1, 5),
  $amount := $l/l_extendedprice * (1- $l/l_discount) - $ps/ps_supplycost * $l/l_quantity
group by
  $nation,
  $o_year
order by
  $nation,
  $o_year descending
return
  element product_type_profit_measure {
    $nation,
    element o_year { $o_year },
    element sum_profit { sum(amount) }
  }
