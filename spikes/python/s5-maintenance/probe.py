import sys
from common import *
import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import *
cat = catalog()
name = "spike.s5_probe"
try: cat.drop_table(name)
except Exception: pass
sch = Schema(NestedField(1,"id",LongType(),False), NestedField(2,"cat",IntegerType(),False), NestedField(3,"d",DateType(),False), NestedField(4,"amount",DoubleType(),False), NestedField(5,"payload",StringType(),False))
t = cat.create_table(name, schema=sch, properties={"format-version":"2"})
con = duck()
for i in range(3):
    con.execute(f"INSERT INTO polaris.{name} SELECT i, (i%100)::INT, DATE '2024-01-01' + (i%365)::INT, i*1.0, 'p'||i FROM range({i*1000},{(i+1)*1000}) t(i)")
t.refresh()
print(file_counts(t))
con.execute(f"MERGE INTO polaris.{name} t USING (SELECT i AS id, 999.0 AS amount FROM range(0,500) t(i)) b ON t.id=b.id WHEN MATCHED THEN UPDATE SET amount=b.amount")
t.refresh(); print(file_counts(t)); print([ (s.snapshot_id, s.summary.operation.value) for s in t.snapshots()])
sid = t.snapshots()[0].snapshot_id
print(con.execute(f"SELECT count(*) FROM polaris.{name} AT (VERSION => {sid})").fetchall())
print(con.execute(f"SELECT count(*), sum(amount) FROM polaris.{name}").fetchall())
print(t.scan().to_arrow().num_rows)
print([m for m in dir(t.inspect) if not m.startswith('_')])
