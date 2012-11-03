for
	$cs in (
		for
			$c in collection("customer")
		where
			substring($c/c_phone,1,2) = ("1", "2", "3", "4", "5", "6", "7")
			and $c/c_acctbal > (
				for 	
					$c in collection("customer")
				where
					$c/c_acctbal > 0.00
					and substring($c/c_phone,1,2) = ("1", "2", "3", "4", "5", "6", "7")		
				return
					avg($c/c_acctbal)
			)
			and count (
				for
					$o in collection("orders")
				where
					$o/o_custkey = $c/c_custkey
				return
					$o
			) = 0
		let
			$cntrycode := substring($c/c_phone,1,2),
			$c_acctbal := $c/c_acctbal
		return
			element custsale{
				element cntrycode { $cntrycode },
				element c_acctbal { $c_acctbal }
			}
	)
let
	$cntrycode := $cs/cntrycode,
	$totacctbal := sum($cs/c_acctbal)
group by
	$cntrycode
order by
	$cntrycode
return
	element global_sales_opportunity{
		element cntrycode { $cntrycode },
		element count_order { count($cntrycode) },
		element totacctbal { $totacctbal }
	}
