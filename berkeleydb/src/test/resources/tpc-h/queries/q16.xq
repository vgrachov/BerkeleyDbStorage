for
	$p in collection("part"),
	$ps in collection("partsupp")
where
	$p/p_partkey = $ps/ps_partkey
	and $p/p_brand != "teste1"
	and not(starts-with($p/p_type,PROMO))
	and $p/p_size = (3, 4, 5, 6, 7, 8, 9, 10)
	and not ($ps/ps_suppkey = (
		for
			$s in collection("supplier")		
		where
			contains($s/s_comment,"Customer") 
			and contains($s/s_comment,"Complaints")
		return
			$s/s_suppkey
	))	
let
	$p_brand := $p/p_brand,
	$p_type := $p/p_type,
	$p_size := $p/size,
	$supplier_cnt := count(distinct-values($ps/ps_suppkey))
group by
	$p_brand,
	$p_type,
	$p_size 
order by
	$supplier_cnt descending,
	$p_brand,
	$p_type,
	$p_size
return
	element supplier_relationship {
    element p_brand { $p_brand },
    element p_type { $p_type },
		element p_size { $p_size },
		element supplier_cnt { $supplier_cnt }
  }
