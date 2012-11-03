for 
	$s in collection("supplier"),
	$re in (
        for
            $l in collection("lineitem")
        where	
            $l/l_shipdate >= "1993-07-01"
            and $l/l_shipdate < "1993-10-01"
        let
            $supplier_no := $l/l_suppkey,
            $total_revenue := sum($l/l_extendedprice * (1 - $l/l_discount))
        group by
            $supplier_no
        return
            element revenue {
                element supplier_no { $supplier_no },
                element total_revenue { $total_revenue }
            }					 
    )
where
	$s/s_suppkey = $re/supplier_no
	and $re/total_revenue = max($re/total_revenue)
let
	$s_suppkey := $s/s_suppkey,
	$s_name := $s/s_name,
	$s_address := $s/s_address,
	$s_phone := $s/s_phone,
	$total_revenue := $re/total_revenue
order by
	$s_suppkey
return
	element top_supplier{ 
		element s_supkey{ $s_suppkey },
		element s_name{ $s_name },
		element s_address{ $s_address },
		element s_phone{ $s_phone },
		element total_revenue{ $total_revenue }
	}
