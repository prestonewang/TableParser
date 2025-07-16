package com.scb.ratan;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.*;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import scala.Option;
import scala.collection.JavaConverters;

import java.util.HashSet;
import java.util.Set;

public class SqlTableExtractor {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SQL Table Extractor")
                .master("local[*]")
                .getOrCreate();

        // 测试SQL（包含CTE、JOIN、INSERT等各种情况）
        String sql = "WITH cte AS (SELECT * FROM source_table)\n" +
                "INSERT INTO target_table\n" +
                "SELECT a.* FROM cte a JOIN db1.other_table b ON a.id = b.id";

        try {
            Set<String> tables = extractTables(spark, sql);
            System.out.println("提取的表名:");
            tables.forEach(System.out::println);
        } catch (Exception e) {
            System.err.println("解析错误: " + e.getMessage());
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }

    public static Set<String> extractTables(SparkSession spark, String sql) throws ParseException {
        LogicalPlan logicalPlan = spark.sessionState().sqlParser().parsePlan(sql);
        return collectTables(logicalPlan);
    }

    private static Set<String> collectTables(LogicalPlan plan) {
        Set<String> tables = new HashSet<>();

        // 1. 处理未解析的表引用
        if (plan instanceof UnresolvedRelation) {
            UnresolvedRelation relation = (UnresolvedRelation) plan;
            tables.add(relation.tableName());
        }
        // 2. 处理表别名和子查询
        else if (plan instanceof SubqueryAlias) {
            SubqueryAlias alias = (SubqueryAlias) plan;
            tables.addAll(collectTables(alias.child()));
        }
        // 3. 处理JOIN操作
        else if (plan instanceof Join) {
            Join join = (Join) plan;
            tables.addAll(collectTables(join.left()));
            tables.addAll(collectTables(join.right()));
        }
        // 4. 处理UNION操作
        else if (plan instanceof Union) {
            Union union = (Union) plan;
            for (LogicalPlan child : JavaConverters.seqAsJavaList(union.children())) {
                tables.addAll(collectTables(child));
            }
        }
        // 5. 处理INSERT INTO语句（Spark 3.x新API）
        else if (plan instanceof InsertIntoStatement) {
            InsertIntoStatement insert =
                    (InsertIntoStatement) plan;
            tables.addAll(collectTables(insert.table()));
            tables.addAll(collectTables(insert.query()));
        }
        // 6. 处理已解析的数据源表
        else if (plan instanceof LogicalRelation) {
            LogicalRelation relation = (LogicalRelation) plan;
            Option<CatalogTable> catalogTable = relation.catalogTable();
            if (catalogTable.isDefined()) {
                TableIdentifier identifier = catalogTable.get().identifier();
                tables.add(identifier.toString());
            }
        }
        // 7. 处理CTE引用
        else if (plan.getClass().getSimpleName().equals("CTERelationRef")) {
            // 使用反射处理CTE引用，因为CTERelationRef在Spark 3.x中是私有类
            try {
                Object cteId = plan.getClass().getMethod("cteId").invoke(plan);
                tables.add("cte_" + cteId.toString());
            } catch (Exception e) {
                // 忽略反射错误
            }
        }

        // 递归处理所有子节点
        for (LogicalPlan child : JavaConverters.seqAsJavaList(plan.children())) {
            tables.addAll(collectTables(child));
        }

        return tables;
    }
}