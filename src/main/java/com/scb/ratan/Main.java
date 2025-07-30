package com.scb.ratan;

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.internal.SQLConf;

import java.util.Set;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        SQLConf conf = new SQLConf();
        CatalystSqlParser parser = new CatalystSqlParser();
        String sql = "WITH cte AS (SELECT * FROM table3) " +
                "SELECT * FROM table1 JOIN cte ON table1.id = cte.id " +
                "WHERE EXISTS (SELECT 1 FROM table2 WHERE table2.id = table1.id)";

        LogicalPlan plan = parser.parsePlan(sql);
        Set<String> tables = TableExtractor.extractTables(plan);
        System.out.println("提取的表名：" + tables); // 输出：[table1, table2, table3]
    }
}
