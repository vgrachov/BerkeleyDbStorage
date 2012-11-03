for
	$s in collection("supplier"),
	$l1 in collection("lineitem"),
	$o in collection("orders"),
	$n in collection("nation")
where
	$s/s_suppkey = $l1/l_suppkey
	and $o/o_orderkey = $l1/l_orderkey
	and $o/o_orderstatus = 'F'
	and $l1/l_receiptdate > $l1/l_commitdate
	and count (
		for
			$l2 in collection("lineitem")
		where
			$l2/l_orderkey = $l1/l_orderkey
			and $l2/l_suppkey != $l1/l_suppkey		
		return
			$l2
	) > 0
	and count (
		for
			$l3 in collection("lineitem")
		where
			$l3/l_orderkey = $l1/l_orderkey
			and $l3/l_suppkey != $l1/l_suppkey		
			and $l3/l_receiptdate > $l3/l_commitdate
		return
			$l3
	) = 0
	and $s/s_nationkey = $n/n_nationkey
	and $n/n_name = "teste"
let
  $s_name := $s/s_name,
	$numwait := count($s_name)
group by
	$s_name
order by
	$numwait descending,
	$s_name
return
  element suppliers_orders_waiting{ 
		element s_name{ $s_name },
		element numwait { $numwait }
	}
