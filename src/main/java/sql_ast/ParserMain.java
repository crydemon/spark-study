//package sql_ast;
//
//import com.alibaba.druid.sql.ast.SQLStatement;
//import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
//import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
//import com.alibaba.druid.sql.parser.SQLStatementParser;
//
//public class ParserMain {
//
//    public static void main(String[] args) {
//        String sql = "select apl.push_method,\n" +
//                "       apl.push_result,\n" +
//                "       apl.switch_on\n" +
//                "from app_push_logs_11 apl\n" +
//                "         inner join app_push_task apt on apl.task_id = apt.id";
//
//        // 新建 MySQL Parser
//        SQLStatementParser parser = new MySqlStatementParser(sql);
//
//        // 使用Parser解析生成AST，这里SQLStatement就是AST
//        SQLStatement statement = parser.parseStatement();
//
//        // 使用visitor来访问AST
//        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
//        statement.accept(visitor);
//
//        // 从visitor中拿出你所关注的信息
//        System.out.println(visitor.getColumns());
//        System.out.println(visitor.getTables());
//        System.out.println(visitor.getRelationships());
//    }
//}