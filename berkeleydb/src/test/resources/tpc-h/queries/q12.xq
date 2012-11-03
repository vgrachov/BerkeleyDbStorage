for
	$o in collection("orders"),
	$l in collection("lineitem")
where
	$o/o_orderkey = $l/l_orderkey
	and $l/l_shipmode = ("TRUCK","MAIL")
	and $l/l_commitdate < $l/l_receiptdate
	and $l/l_shipdate < $l/l_commitdate
	and $l/l_receiptdate >= "1993-07-01"
	and $l/l_receiptdate < "1994-07-01"
let
  $l_shipmode := $l/l_shipmode,
	$high_line_count := sum(if ($o/o_orderpriority = "1-URGENT" or $o/o_orderpriority = "2-HIGH") then 1 else 0),
	$low_line_count := sum(if ($o/o_orderpriority != "1-URGENT" or $o/o_orderpriority != "2-HIGH") then 1 else 0)
group by
  $l_shipmode
order by
  $l_shipmode
return
  element ships_mode_order_priority{
    $l_shipmode,
    element high_line_count { $high_line_count },
    element low_line_count { $low_line_count }
 }
