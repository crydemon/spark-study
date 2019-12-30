package sql_ast;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.hive.visitor.HiveSchemaStatVisitor;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.util.JdbcConstants;

import java.util.List;

public class ParserMain {

    public static void main(String[] args) {
        final String dbType = JdbcConstants.HIVE; // 可以是ORACLE、POSTGRESQL、SQLSERVER、ODPS等
        String sql = "insert overwrite table tmp.tmp_buyer_first_pay\n" +
                "select 'vova' as datasource,\n" +
                "    oi.user_id as buyer_id,\n" +
                "    min(order_id) as first_order_id,\n" +
                "    min(order_time) as first_order_time,\n" +
                "    min(pay_time) as first_pay_time\n" +
                "from ods.vova_order_info oi\n" +
                "where oi.pay_status >= 1\n" +
                "group by user_id;";

        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);



        // 使用Parser解析生成AST，这里SQLStatement就是AST
        SQLStatement statement = stmtList.get(0);

        // 使用visitor来访问AST
        HiveSchemaStatVisitor visitor = new HiveSchemaStatVisitor();
        statement.accept(visitor);
                
        // 从visitor中拿出你所关注的信息        
        System.out.println(visitor.getColumns());
        System.out.println(visitor.getTables());
        System.out.println(visitor.getRelationships());
        System.out.println(visitor.getConditions());
    }
}