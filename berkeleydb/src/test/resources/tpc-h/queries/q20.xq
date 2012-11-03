for
	$s in collection("supplier"),
	$n in collection("nation")
where
	s_suppkey = (
		for
			$ps in collection("partsupp")
		where
			ps_partkey = (
				for
					$p in collection("part")
				where
					starts-with($p/p_name,teste)
				return
					$p/p_partkey		
			)
			and $ps/ps_availqty > (
				for				
					$l in collection("lineitem")	
				where
					$l/l_partkey = $ps/ps_partkey
					and $l/l_suppkey = $ps/ps_suppkey
					and $l/l_shipdate >= "1993-07-01"
					and $l/l_shipdate < "1994-07-01"			
				return
					0.5 * sum($l/l_quantity)				
			)
		return
			$ps/ps_suppkey	
	)
	and $s/s_nationkey = $n/n_nationkey
	and $n/n_name = "teste"
let
  $s_name := $s/s_name,
	$s_address := $s/s_address
order by
	$s_name
return
  element potential_part_promotion{ 
		element s_name{ $s_name },
		element s_address { $s_address }
	}
