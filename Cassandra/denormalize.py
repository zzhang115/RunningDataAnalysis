from pyspark.sql import SQLContext
from pyspark import SparkConf
import sys
import pyspark_cassandra
import uuid

# initialize spark cassandra
cluster_seeds = ['199.60.17.136', '199.60.17.173']
conf = SparkConf().set('spark.cassandra.connection.host', ','.join(cluster_seeds)) \
    .set('spark.dynamicAllocation.maxExecutors', 20)
sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
sqlContext = SQLContext(sc)


def df_for(keyspace, table, split_size=None):
    df = sqlContext.createDataFrame(sc.cassandraTable(keyspace, table, split_size=split_size).setName(table))
    # df.registerTempTable(table)
    return df


def rows_to_list(key_vals, key_col, val_col, list_col):
    def listappend(lst, v):
        lst.append(v)
        return lst

    def listjoin(lst1, lst2):
        lst1.extend(lst2)
        return lst1

    assert key_vals.columns == [key_col, val_col], 'key_vals must have two columns: your key_col and val_col'
    key_val_rdd = key_vals.rdd.map(tuple)
    key_list_rdd = key_val_rdd.aggregateByKey([], listappend, listjoin)
    return sqlContext.createDataFrame(key_list_rdd, schema=[key_col, list_col])


def main(argv=None):
    if argv is None:
        keyspace = sys.argv[1]
        keyspace2 = sys.argv[2]

    # get stock, lineitem and part tables
    stock = df_for(keyspace, 'stocks')
    stock.cache()
    stock.registerTempTable('stocks')

    lineItem = df_for(keyspace, 'lineitem')
    lineItem.registerTempTable('lineitem')

    part = df_for(keyspace, 'part')
    part.registerTempTable('part')

    # join
    stock_part = sqlContext.sql("SELECT a.stockkey, c.name " + \
                                "FROM stock a JOIN lineitem b ON (a.stockkey = b.stockkey) " + \
                                "JOIN part c on (b.partkey = c.partkey)")

    stock_part2 = rows_to_list(stock_part, 'stockkey', 'name', 'part_names')
    stock_part2.registerTempTable('stock_part')

    result = sqlContext.sql("SELECT a.*, b.part_names " + \
                            "FROM stock a JOIN stock_part b ON (a.stockkey = b.stockkey)")

    # Save result to Cassandra
    result.rdd.map(lambda x: (x['stockkey'], x['custkey'], x['stockstatus'], \
                              x['totalprice'], x['stockdate'], x['stock_priority'], x['clerk'],
                              x['stock_priority'], x['comment'], x['part_names'])) \
        .saveToCassandra(keyspace2, 'stocks_parts', columns=["stockkey", "custkey", \
                                                             "stockstatus", "totalprice", "stockdate", "stock_priority",
                                                             "clerk", \
                                                             "stock_priority", "comment", "part_names"], batch_size=300,
                         parallelism_level=1000)

if __name__ == "__main__":
    main()