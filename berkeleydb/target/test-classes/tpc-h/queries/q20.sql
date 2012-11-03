select
	s_name,
	s_address
from
	supplier,
	nation
where
	s_suppkey = (
		select
			ps_suppkey
		from
			partsupp
		where
			ps_partkey = (
				select
					p_partkey
				from
					part
				where
					contains(p_name,'chocolate')
			)
			and ps_availqty > (
				select
					0.5 * sum(l_quantity)
				from
					lineitem
				where
					l_partkey = ps_partkey
					and l_suppkey = ps_suppkey
					and l_shipdate >= '1993-07-01'
					and l_shipdate < '1994-07-01'
			)
	)
	and s_nationkey = n_nationkey
	and n_name = 'ARGENTINA'
order by
	s_name;
