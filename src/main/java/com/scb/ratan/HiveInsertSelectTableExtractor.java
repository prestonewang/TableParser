package com.scb.ratan;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;

import java.lang.reflect.Field;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 基于 HiveParser 提取 INSERT SELECT 中的表名，区分插入表和查询来源表
 */
public class HiveInsertSelectTableExtractor {

    // 存储结果：key为"insert"或"select"，value为表名列表
    private Map<String, List<String>> tableMap = new HashMap<>();

    public HiveInsertSelectTableExtractor() {
        tableMap.put("insert", new ArrayList<>());
        tableMap.put("select", new ArrayList<>());
    }

    /**
     * 解析 INSERT SELECT 语句并提取表名
     * @param sql Hive SQL 语句（INSERT SELECT 结构）
     * @return 包含插入表和查询表的映射
     * @throws ParseException 解析异常
     */
    public Map<String, List<String>> extract(String sql) throws Exception {

       /* System.out.println(this.getClass().getClassLoader());
        System.out.println(Thread.currentThread().getContextClassLoader());
//        fixClassLoader();

        // 初始化 Hive 会话状态（解析依赖）
        if (SessionState.get() == null) {
            SessionState.start(new HiveConf());
//            CliSessionState.start(new HiveConf());
//            CliSessionState ss = new CliSessionState(new HiveConf());
        }*/

        // 解析 SQL 生成 AST 根节点
        ASTNode root = ParseUtils.parse(sql, null);

        // 递归遍历 AST，提取表名
        traverseAST2(root);

        return tableMap;
    }

    private void fixClassLoader() throws Exception {
        // 获取当前线程的上下文类加载器
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        // 如果不是 URLClassLoader，尝试创建一个新的
        if (!(classLoader instanceof URLClassLoader)) {
            // 获取系统类加载器的 URL 路径（通过反射）
            System.out.println(classLoader.getClass());
            System.out.println(classLoader.getClass().getSuperclass());
            System.out.println(classLoader.getClass().getSuperclass().getDeclaredFields().length);
            for(Field f : classLoader.getClass().getSuperclass().getDeclaredFields()) {
                System.out.println("field : " + f);
            }
            Field ucpField = classLoader.getClass().getSuperclass().getDeclaredField("ucp");
            ucpField.setAccessible(true);
            Object ucp = ucpField.get(classLoader);

            Field pathField = ucp.getClass().getDeclaredField("path");
            pathField.setAccessible(true);
            @SuppressWarnings("unchecked")
            List<java.net.URL> path = (List<java.net.URL>) pathField.get(ucp);

            // 创建新的 URLClassLoader
            URLClassLoader newClassLoader = new URLClassLoader(
                    path.toArray(new java.net.URL[0]),
                    classLoader.getParent()
            );

            // 设置回线程上下文
            Thread.currentThread().setContextClassLoader(newClassLoader);
        }
    }

    /**
     * 递归遍历 AST 节点
     */
    private void traverseAST(ASTNode node) {
        if (node == null) {
            return;
        }

        // 识别 INSERT 节点（TOK_INSERT），区分目标表和来源表
        if (node.getType() == HiveParser.TOK_INSERT) {
            handleInsertNode(node);
        } else {
            // 非 INSERT 节点，递归处理子节点（适用于嵌套场景）
            for (int i = 0; i < node.getChildCount(); i++) {
                Object child = node.getChild(i);
                if (child instanceof ASTNode) {
                    traverseAST((ASTNode) child);
                }
            }
        }
    }

    private void traverseAST2(ASTNode node) {
        if (node == null) {
            return;
        }

        System.out.println(node.toString());
        for (int i = 0; i < node.getChildCount(); i++) {
            Object child = node.getChild(i);
            if (child instanceof ASTNode) {
                traverseAST2((ASTNode) child);
            } else {
                System.out.println("Not ASTNode : " + child);
            }
        }
    }

    /**
     * 处理 TOK_INSERT 节点，提取插入目标表和查询来源表
     */
    private void handleInsertNode(ASTNode insertNode) {
        // 遍历 INSERT 节点的子节点，区分目标表和查询部分
        for (int i = 0; i < insertNode.getChildCount(); i++) {
            ASTNode child = (ASTNode) insertNode.getChild(i);

            // 1. 提取插入目标表（TOK_DESTINATION → TOK_TAB → TOK_TABNAME）
            if (child.getType() == HiveParser.TOK_DESTINATION) {
                extractInsertTable(child);
            }

            // 2. 提取查询来源表（TOK_SELECT 或 TOK_QUERY 中的表）
            if (child.getType() == HiveParser.TOK_SELECT || child.getType() == HiveParser.TOK_QUERY) {
                extractSelectTables(child);
            }
        }
    }

    /**
     * 从 TOK_DESTINATION 节点提取插入目标表
     */
    private void extractInsertTable(ASTNode destinationNode) {
        for (int i = 0; i < destinationNode.getChildCount(); i++) {
            ASTNode child = (ASTNode) destinationNode.getChild(i);
            // TOK_TAB 节点包含目标表信息
            if (child.getType() == HiveParser.TOK_TAB) {
                for (int j = 0; j < child.getChildCount(); j++) {
                    ASTNode tabChild = (ASTNode) child.getChild(j);
                    // TOK_TABNAME 节点存储表名（可能带数据库前缀）
                    if (tabChild.getType() == HiveParser.TOK_TABNAME) {
                        String tableName = buildTableName(tabChild);
                        if (!tableMap.get("insert").contains(tableName)) {
                            tableMap.get("insert").add(tableName);
                        }
                    }
                }
            }
        }
    }

    /**
     * 从 SELECT/QUERY 节点提取查询来源表
     */
    private void extractSelectTables(ASTNode selectNode) {
        // 递归遍历查询部分的所有节点，提取 TOK_TABREF 中的表名
        if (selectNode.getType() == HiveParser.TOK_TABREF) {
            // TOK_TABREF 对应表引用（如 FROM table 或 JOIN table）
            String tableName = extractTableNameFromTabRef(selectNode);
            if (tableName != null && !tableMap.get("select").contains(tableName)) {
                tableMap.get("select").add(tableName);
            }
        }

        // 递归处理子节点（处理嵌套查询、JOIN 等）
        for (int i = 0; i < selectNode.getChildCount(); i++) {
            Object child = selectNode.getChild(i);
            if (child instanceof ASTNode) {
                extractSelectTables((ASTNode) child);
            }
        }
    }

    /**
     * 从 TOK_TABREF 节点提取表名（来源表）
     */
    private String extractTableNameFromTabRef(ASTNode tabRefNode) {
        for (int i = 0; i < tabRefNode.getChildCount(); i++) {
            ASTNode child = (ASTNode) tabRefNode.getChild(i);
            if (child.getType() == HiveParser.TOK_TABNAME) {
                return buildTableName(child);
            }
        }
        return null;
    }

    /**
     * 拼接 TOK_TABNAME 节点为完整表名（如 "db.table"）
     */
    private String buildTableName(ASTNode tabNameNode) {
        List<String> parts = new ArrayList<>();
        for (int i = 0; i < tabNameNode.getChildCount(); i++) {
            ASTNode partNode = (ASTNode) tabNameNode.getChild(i);
            parts.add(partNode.getText());
        }
        return String.join(".", parts);
    }

    // 测试示例
    public static void main(String[] args) throws Exception {
        String sql = "INSERT INTO TABLE db.target_table " +
                "SELECT a.id," +
                "b.name," +
                "b.code -- db.target_table2 as xxx" +
                "FROM db1.source1 a " +
                "JOIN source2 b ON a.id = b.id " +
                "WHERE a.age > (SELECT max(age) FROM db2.source3)";

        HiveInsertSelectTableExtractor extractor = new HiveInsertSelectTableExtractor();
        Map<String, List<String>> result = extractor.extract(sql);

        System.out.println("插入目标表（insert table）：" + result.get("insert"));
        System.out.println("查询来源表（select table）：" + result.get("select"));
        // 输出结果：
        // 插入目标表（insert table）：[db.target_table]
        // 查询来源表（select table）：[db1.source1, source2, db2.source3]
    }
}
