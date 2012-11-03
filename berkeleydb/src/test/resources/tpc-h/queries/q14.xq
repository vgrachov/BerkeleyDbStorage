for
	$l in collection("lineitem"),
	$p in collection("part")
where
	$l/l_partkey = $p/p_partkey
	and $l/l_shipdate >= "1993-07-01"
	and $l/l_shipdate < "1993-08-01"	
let
  $promo_revenue := 100 * (sum(if (starts-with($p/p_type,PROMO)) then ($l/l_extendedprice * (1 - $l/l_discount)) else 0)/sum($l/l_extendedprice * (1 - $l/l_discount)))
return
	element promo_revenue{ $promo_revenue }
