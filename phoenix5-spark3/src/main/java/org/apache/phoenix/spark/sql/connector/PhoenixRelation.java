package org.apache.phoenix.spark.sql.connector;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.InsertableRelation;
import org.apache.spark.sql.sources.PrunedFilteredScan;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Map;
import java.util.HashMap;

public class PhoenixRelation extends BaseRelation implements PrunedFilteredScan, InsertableRelation {
    SQLContext sqlContext;
    Map<String, String> parameters;
    StructType schema;

    PhoenixRelation(SQLContext sqlContext, Map<String, String> parameters, StructType schema) {
        this.sqlContext = sqlContext;
        this.parameters = parameters;
        this.schema = schema;
    }

    @Override
    public SQLContext sqlContext() {
        return sqlContext;
    }

    @Override
    public StructType schema() {
        return schema;
    }


    @Override
    public RDD<Row> buildScan(String[] requiredColumns, Filter[] filters) {
        String requiredColumnsString = String.join(",", requiredColumns);

        HashMap<String, String> test = new HashMap();
        test.put("requiredColumn", requiredColumnsString);

        Dataset<Row> ds = sqlContext.sparkSession().read().format("phoenix").options(parameters).options(test).load();
        return ds.toDF().rdd();
    }

    @Override
    public void insert(Dataset<Row> data, boolean overwrite) {
        data.write().format("phoenix").options(parameters).mode(SaveMode.Append).save();
    }
}
