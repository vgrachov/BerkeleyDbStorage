for
	$l in collection("lineitem"),
	$p in collection("part")
where
	$p/p_partkey = $l/l_partkey
	and $p/p_brand = "teste"
	and $p/p_container = "teste2"
	and $l/l_quantity < (
		for 
			$l in collection("lineitem")
		where 
			$p/p_partkey = $l/l_partkey
		return
			0.2 * avg($l/l_quantity)
	)
let
	$avg_yearly := sum($l/l_extendedprice) / 7.0
return
	element avg_yearly { $avg_yearly }
