package com.scb.ratan;

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import scala.collection.JavaConverters;

import java.util.HashSet;
import java.util.Set;

public class SqlParserWithoutSpark {

    private static final ParserInterface sqlParser = new CatalystSqlParser();

    public static void main(String[] args) {
        String sql = "WITH cte AS (SELECT * FROM src) INSERT INTO target SELECT * FROM cte";

        try {
            Set<String> tables = extractTables(sql);
            System.out.println("提取的表名: " + tables);
        } catch (Exception e) {
            System.err.println("解析失败: " + e.getMessage());
        }
    }

    public static Set<String> extractTables(String sql) throws ParseException {
        LogicalPlan plan = sqlParser.parsePlan(sql);
        return collectTables(plan);
    }

    private static Set<String> collectTables(LogicalPlan plan) {
        Set<String> tables = new HashSet<>();

        // 1. 处理基础表引用
        if (plan instanceof UnresolvedRelation) {
            tables.add(((UnresolvedRelation) plan).tableName());
        }
        // 2. 处理子查询别名
        else if (plan instanceof SubqueryAlias) {
            tables.addAll(collectTables(((SubqueryAlias) plan).child()));
        }
        // 3. 处理JOIN操作
        else if (plan instanceof Join) {
            Join join = (Join) plan;
            tables.addAll(collectTables(join.left()));
            tables.addAll(collectTables(join.right()));
        }
        // 4. 处理INSERT（Spark 3.x+）
        else if (isInsertIntoStatement(plan)) {
            try {
                Object table = plan.getClass().getMethod("table").invoke(plan);
                Object query = plan.getClass().getMethod("query").invoke(plan);
                tables.addAll(collectTables((LogicalPlan) table));
                tables.addAll(collectTables((LogicalPlan) query));
            } catch (Exception e) {
                // 反射失败处理
            }
        }

        // 递归处理子节点
        JavaConverters.asJavaCollection(plan.children())
                .forEach(child -> tables.addAll(collectTables((LogicalPlan) child)));

        return tables;
    }

    private static boolean isInsertIntoStatement(LogicalPlan plan) {
        return plan.getClass().getSimpleName().equals("InsertIntoStatement");
    }
}