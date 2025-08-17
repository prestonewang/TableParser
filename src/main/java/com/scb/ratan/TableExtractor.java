package com.scb.ratan;

import org.apache.spark.sql.catalyst.plans.logical.*;
import org.apache.spark.sql.execution.datasources.LogicalRelation;

import java.util.HashSet;
import java.util.Set;

public class TableExtractor {
    // 提取所有表名（去重）
    public static Set<String> extractTables(LogicalPlan plan) {
        Set<String> tables = new HashSet<>();
        traverse(plan, tables);
        return tables;
    }

    // 递归遍历逻辑计划节点
    private static void traverse(LogicalPlan plan, Set<String> tables) {
        if (plan == null) return;

        // 处理基础表节点（LogicalRelation 通常对应物理表）
        if (plan instanceof LogicalRelation) {
//            String tableName = ((LogicalRelation) plan).catalogTable().identifier().table();
//            tables.add(tableName);
        }
        // 处理子查询别名（如 "SELECT ... FROM (子查询) AS a" 中的子查询）
        else if (plan instanceof SubqueryAlias) {
            traverse(((SubqueryAlias) plan).child(), tables);
        }
        // 处理 CTE（公用表表达式，如 "WITH cte AS (...) SELECT ..."）
        else if (plan instanceof WithCTE) {
            WithCTE withCTE = (WithCTE) plan;
            // 遍历 CTE 定义中的子计划
//            withCTE.cteRelations().forEach((k, v) -> traverse(v, tables));
            // 遍历主查询计划
//            traverse(withCTE.child(), tables);
        }
        // 处理 JOIN 等组合节点（递归处理左右子节点）
        else {
            // 遍历所有子节点（不同类型的计划可能有多个子节点）
            /*for (LogicalPlan child : plan.children()) {
                traverse(child, tables);
            }*/
        }
    }
}

