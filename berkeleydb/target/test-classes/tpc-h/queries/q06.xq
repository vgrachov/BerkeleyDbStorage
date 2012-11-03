for
  $l in collection("lineitem")
where
  $l/l_shipdate >= "1994-01-01" and
  $l/l_shipdate < "1995-01-01" and
  $l/l_discount >= 0.04 and
  $l/l_discount <= 0.05 and
  $l/l_quantity < 24
let
  $revenue := sum($l/l_extendedprice * $l/l_discount)
return
  element revenue { $revenue }
