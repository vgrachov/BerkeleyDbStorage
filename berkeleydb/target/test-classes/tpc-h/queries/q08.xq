for
  $p in collection("part"),
  $s in collection("supplier"),
  $l in collection("lineitem"),
  $o in collection("orders"),
  $c in collection("customer"),
  $n1 in collection("nation"),
  $n2 in collection("nation"),
  $r in collection("region")
where
  $p/p_partkey = $l/l_partkey
  and $s/s_suppkey = $l/l_suppkey
  and $l/l_orderkey = $o/o_orderkey
  and $o/o_custkey = $c/c_custkey
  and $c/c_nationkey = $n1/n_nationkey
  and $n1/n_regionkey = $n2/n_regionkey
  and $r/r_name = "AMERICA"
  and $s/s_nationkey = $n2/n_nationkey
  and $o/o_orderdate >= "1995-01-01"
  and $o/o_orderdate < "1996-12-31"
  and $p/p_type = "ECONOMY ANODIZED STEEL"
let
  $o_year := substring($o/o_orderdate, 1, 5),
  $volume := $l/l_extendedprice * (1 - $l/l_discount),
  $nation := $n2/n_name
group by
  $o_year
order by
  $o_year
let
  $mkt_share := sum(if ($nation = "BRAZIL") then $volume else 0) / sum($volume)
return
  element national_market_share {
    element o_year { $o_year },
    element mkt_share { $mkt_share }
  }
