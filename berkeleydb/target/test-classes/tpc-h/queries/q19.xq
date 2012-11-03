for
	$l in collection("lineitem"),
	$p in collection("part")
where
	(
		$p/p_partkey = $l/l_partkey
		and $p/p_brand = "test"
		and $p/p_container = ("SM CASE", "SM BOX", "SM PACK", "SM PKG")
		and $l/l_quantity >= 10.0 and $l/l_quantity <= 20.0
		and $p/p_size >= 1.0
		and $p/p_size <= 5.0
		and $l/l_shipmode = ("AIR", "AIR REG")
		and $l/l_shipinstruct = "DELIVER IN PERSON"
	)
	or
	(
		$p/p_partkey = $l/l_partkey
		and $p/p_brand = "test2"
		and $p/p_container = ("MED BAG", "MED BOX", "MED PKG", "MED PACK")
		and $l/l_quantity >= 15.0 and $l/l_quantity <= 25.0
		and $p/p_size >= 1.0
		and $p/p_size <= 10.0
		and $l/l_shipmode = ("AIR", "AIR REG")
		and $l/l_shipinstruct = "DELIVER IN PERSON"
	)
	or
	(
		$p/p_partkey = $l/l_partkey
		and $p/p_brand = "test3"
		and $p/p_container = ("LG CASE", "LG BOX", "LG PACK", "LG PKG")
		and $l/l_quantity >= 40.0 and $l/l_quantity <= 50.0
		and $p/p_size >= 1.0
		and $p/p_size <= 15.0
		and $l/l_shipmode = ("AIR", "AIR REG")
		and $l/l_shipinstruct = "DELIVER IN PERSON"
	)
let
  $revenue := sum($l/l_extendedprice* (1 - $l/l_discount))
return
  element revenue{ $revenue }
