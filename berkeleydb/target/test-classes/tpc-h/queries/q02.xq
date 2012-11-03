for
  $p in collection("part"),
  $s in collection("supplier"),
  $ps in collection("partsupp"),
  $n in collection("nation"),
  $r in collection("region")
where
  $p/p_partkey = $ps/ps_partkey
  and $s/s_suppkey = $ps/ps_suppkey
  and $s/s_nationkey = $n/n_nationkey
  and $n/n_regionkey = $r/r_regionkey
  and $ps/ps_supplycost = (
    for
      $ps in collection("partsupp"),
      $s in collection("supplier"),
      $n in collection("nation"),
      $r in collection("region")
    where
      $p/p_partkey = $ps/ps_partkey
      and $s/s_suppkey = $ps/ps_suppkey
      and $s/s_nationkey = $n/n_nationkey
      and $n/n_regionkey = $r/r_regionkey
    return
      min($ps/ps_supplycost)
   )
order by
  $s/s_acctbal descending,
  $n/n_name,
  $s/s_name,
  $p/p_partkey
return
  element minimum_cost_supplier {
    $s/s_acctbal,
    $s/s_name,
    $n/n_name,
    $p/p_partkey,
    $p/p_mfgr,
    $s/s_address,
    $s/s_phone,
    $s/s_comment
  }
