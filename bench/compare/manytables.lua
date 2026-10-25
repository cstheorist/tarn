-- sysbench script for bench/compare/manytables.sh: point selects on random
-- tables t0000..t(N-1), each holding ids 1..10.
sysbench.cmdline.options = {
  tables = {"Number of tables", 1000},
}

function thread_init()
  drv = sysbench.sql.driver()
  con = drv:connect()
end

function thread_done()
  con:disconnect()
end

function event()
  local t = sysbench.rand.uniform(0, sysbench.opt.tables - 1)
  local id = sysbench.rand.uniform(1, 10)
  con:query(string.format("SELECT c FROM t%04d WHERE id = %d", t, id))
end
