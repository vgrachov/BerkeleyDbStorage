for
  $ps in collection("partsupp"),
  $s in collection("supplier"),
  $n in collection("nation")
where
  $ps/ps_suppkey = $s/s_suppkey
  and $s/s_nationkey = $n/n_nationkey
  and $n/n_name = "GERMANY"
let
  $ps_partkey := $ps/ps_partkey
group by
  $ps_partkey
let
  $value := sum($ps/ps_supplycost * $ps/ps_availqty)
where
  $value > 0.0001 * (
    for
      $ps in collection("partsupp"),
      $s in collection("supplier"),
      $n in collection("nation")
    where
      $ps/ps_suppkey = $s/s_suppkey
      and $s/s_nationkey = $n/n_nationkey
      and $n/n_name = "GERMANY"
    return
      $ps/ps_supplycost * $ps/ps_availqty
  )
order by
  $value
return
  element important_stock {
    $ps_partkey,
    element value { $value }
  }
