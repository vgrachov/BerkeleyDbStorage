for
  $o in collection("orders")
let
  $orderpriority := $o/o_orderpriority
where
  $o/o_orderdate >= "1993-07-01" and
  $o/o_orderdate < "1993-10-01" and
  count(
    for
      $l in collection("lineitem")
    where
      $l/l_orderkey = $o/o_orderkey and
      $l/l_commitdate < $l/l_receiptdate
    return
      $l
  ) > 0
order by
  $orderpriority
group by
  $orderpriority
return
  element order_priority {
    $orderpriority,
    element order_count { count($o) }
  }
