for
	$c in collection("customer"),
	$o in collection("orders"),
	$l in collection("lineitem")	
where
	$o/o_orderkey = (
		for
			$l in collection("lineitem")
		let
  		    $l_orderkey := $l/l_orderkey
		group by
  		    $l_orderkey
		where
			sum($l/l_quantity) > 10
		return
			$l_orderkey
	)
	and $c/c_custkey = $o/o_custkey
	and $o/o_orderkey = $l/l_orderkey
let
	$c_name := $c/c_name,
	$c_custkey := $c/c_custkey,
	$o_orderkey := $o/o_orderkey,
	$o_orderdate := $o/o_orderdate,
	$o_totalprice := $o/o_totalprice
group by
	$c_name,
	$c_custkey,
	$o_orderkey,
	$o_orderdate,
	$o_totalprice
order by
	$o_totalprice descending,
	$o_orderdate
return
	element large_volume_customer {
       element c_name { $c_name },
       element c_custkey { $c_custkey },
	   element o_orderkey { $o_orderkey },
	   element o_orderdate { $o_orderdate },
	   element o_totalprice { $o_totalprice },
	   element value { sum($l/l_quantity) }
    }
