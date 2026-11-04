import json
import os
import pymysql
from pushdown_contracts import Connection, observe

c = Connection(host='127.0.0.1', port=3407, user='root', password='', autocommit=True, charset='utf8mb4')
cur = c.cursor()
cur.execute('SELECT VERSION(), @@sql_mode, @@character_set_connection, @@collation_connection')
result = {'schema': 1, 'reference': list(cur.fetchone()), 'cases': []}
cur.execute('CREATE DATABASE IF NOT EXISTS agg_probe')
cur.execute('USE agg_probe')
cur.execute('DROP TABLE IF EXISTS t')
cur.execute('CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, s BIGINT NOT NULL, u BIGINT UNSIGNED NOT NULL, n BIGINT, z BIGINT NOT NULL DEFAULT 0, d DECIMAL(10,2) NOT NULL, f DOUBLE NOT NULL, a INT, KEY(a))')
cur.execute('INSERT INTO t(s,u,n,d,f,a) VALUES (1,2,3,1.1,1.2,1),(3,4,NULL,3.3,3.4,1),(5,6,7,5.5,5.6,2)')
items = 'SUM(s) AS total,MIN(u) AS lo,MAX(s) AS hi,COUNT(*) AS nrows,MIN(n),MAX(z),MIN(id),SUM(d),MIN(d),SUM(f),MIN(f),MIN(s+0),MAX(u+0)'
for suffix in ['', ' ORDER BY total', ' GROUP BY id', ' GROUP BY id ORDER BY id', ' GROUP BY id ORDER BY id DESC', ' GROUP BY id ORDER BY total', ' GROUP BY id ORDER BY total LIMIT 1', ' GROUP BY a', ' GROUP BY a ORDER BY a', ' GROUP BY a ORDER BY total', ' GROUP BY n', ' GROUP BY n ORDER BY total', ' WHERE id < 0 GROUP BY id ORDER BY total', ' GROUP BY id HAVING total > 0 ORDER BY total', ' GROUP BY id ORDER BY MIN(u)', ' GROUP BY id ORDER BY s', ' GROUP BY id ORDER BY id, total', ' GROUP BY id ORDER BY total, id']:
    sql = 'SELECT '+items+' FROM t'+suffix
    entry = {'sql': sql, 'text': observe(c, sql), 'binary': observe(c, sql, 'binary')}
    cur.execute('EXPLAIN '+sql)
    entry['explain_columns'] = [x[0] for x in cur.description]
    entry['explain'] = cur.fetchall()
    result['cases'].append(entry)
cur.close()
c.close()
print(json.dumps(result, indent=2, default=str))
