for
  $c in collection("customer"),
  $o in collection("orders"),
  $l in collection("lineitem"),
  $n in collection("nation")
where
  $c/c_custkey = $o/o_custkey
  and $l/l_orderkey = $o/o_orderkey
  and $o/o_orderdate >= "1993-10-01"
  and $o/o_orderdate < "1994-01-01"
  and $l/l_returnflag = "R"
  and $c/c_nationkey = $n/n_nationkey
let
  $revenue := sum($l/l_extendedprice * (1 - $l/l_discount)),
  $c_custkey := $c/c_custkey,
  $c_name := $c/c_name,
  $c_acctbal := $c/c_acctbal,
  $c_phone := $c/c_phone,
  $n_name := $n/n_name,
  $c_address := $c/c_address,
  $c_comment := $c/c_comment
group by
  $c_custkey,
  $c_name,
  $c_acctbal,
  $c_phone,
  $n_name,
  $c_address,
  $c_comment
order by
  $revenue descending
return
  element returned_item {
    $c_custkey,
    $c_name,
    element revenue { $revenue },
    $c_acctbal,
    $n_name,
    $c_address,
    $c_phone,
    $c_comment
  }
