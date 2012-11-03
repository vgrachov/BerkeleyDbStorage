for
	$co in (for
		 				$c in collection("customer"),
		 				$o allowing empty in (for 
														 				$o2 in collection("orders")
																	where
														 				$c/c_custkey = $o2/o_custkey
														 				and not (contains($o2/o_comment,"teste"))
																	let
																		$c_count := count($o2)													 				
																	return
														 				$c_count
													 				)
					let
						$c_custkey := $c/c_custkey,
						$c_count := $o/c_count
					group by
						$c_custkey
					return
						($c_custkey, $c_count)		
					)
let
	$c_custkey := $co/c_custkey,
	$c_count := $co/c_count,
	$custdist := count($co)
group by
	$c_count
order by
	$custdist descending,
	$c_count descending
return
  element customer_distribution{
    element c_count { $c_count },
    element custdist { $custdist }
 }
