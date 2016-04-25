# CassCompaction

Intended use - investigate compaction process.

For example:
1) Write batches of records(ex:~95000 records per batch; 10 batches ) with TTL. Initialize compaction(ex:write records without TTL) after ttl+gc_grace_seconds time.
  a. Check how many time need for processing such data.
  b. How such compaction process affects hardware(with using external hardware monitoring tools).

2) Visualize compaction process for different compaction strategies. 
For example:
Initialise some compaction process and check it progress by calling info methods.

Example of work:

1)
---------------------------TRACE------------------------------
keyspace: comp_v1;   compacted_at: 2016-04-06 17:07:02.0156
keyspace: comp_v1;   compacted_at: 2016-04-06 17:07:49.0198
keyspace: comp_v1;   compacted_at: 2016-04-06 17:09:51.0281
-1-
-2-
SSTable count: 1
SSTable count: 2
SSTable count: 1
-3-
JSON(in SSTables /Programs/dse-4.6.5/store/comp_v1/testevents/comp_v1-testevents-jb-3-Data.db):
{d=2349}
Read 201 live and 3759 tombstoned cells
-4-
JSON(in SSTables /Programs/dse-4.6.5/store/comp_v1/testevents/comp_v1-testevents-jb-3-Data.db):
{d=2349}
Read 0 live and 4362 tombstoned cells
-5-
SSTable count: 2
SSTable count: 3
SSTable count: 4
keyspace: comp_v1;   compacted_at: 2016-04-06 17:14:08.0465
SSTable count: 2
SSTable count: 3
SSTable count: 4
-6-
JSON(in SSTables /Programs/dse-4.6.5/store/comp_v1/testevents/comp_v1-testevents-jb-10-Data.db):
no tombstones
JSON(in SSTables /Programs/dse-4.6.5/store/comp_v1/testevents/comp_v1-testevents-jb-4-Data.db):
{d=2013}
JSON(in SSTables /Programs/dse-4.6.5/store/comp_v1/testevents/comp_v1-testevents-jb-8-Data.db):
no tombstones
JSON(in SSTables /Programs/dse-4.6.5/store/comp_v1/testevents/comp_v1-testevents-jb-9-Data.db):
no tombstones
Read 0 live and 2013 tombstoned cells

2)
--------------------------TRACE------------------------------
keyspace: comp_v1;   compacted_at: 2016-04-06 17:07:02.0156
keyspace: comp_v1;   compacted_at: 2016-04-06 17:07:49.0198
keyspace: comp_v1;   compacted_at: 2016-04-06 17:09:51.0281
keyspace: comp_v1;   compacted_at: 2016-04-06 17:14:08.0465
-1-
-2-
SSTable count: 1
SSTable count: 2
SSTable count: 1
SSTable count: 2
SSTable count: 1
-3-
JSON(in SSTables /Programs/dse-4.6.5/store/comp_v1/testevents/comp_v1-testevents-jb-3-Data.db):
{d=2247}
Read 182 live and 3915 tombstoned cells
-4-
JSON(in SSTables /Programs/dse-4.6.5/store/comp_v1/testevents/comp_v1-testevents-jb-4-Data.db):
{d=2175, e=39}
Read 0 live and 2214 tombstoned cells
-5-
SSTable count: 2
SSTable count: 1
SSTable count: 2
keyspace: comp_v1;   compacted_at: 2016-04-06 17:21:00.0781
SSTable count: 1
SSTable count: 2
SSTable count: 3
SSTable count: 4
keyspace: comp_v1;   compacted_at: 2016-04-06 17:22:09.0825
SSTable count: 2
-6-
JSON(in SSTables /Programs/dse-4.6.5/store/comp_v1/testevents/comp_v1-testevents-jb-11-Data.db):
no tombstones
JSON(in SSTables /Programs/dse-4.6.5/store/comp_v1/testevents/comp_v1-testevents-jb-7-Data.db):
no tombstones
