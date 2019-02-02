package local.tests.upya.jdbc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

@SpringBootApplication
public class Application {

    private interface StatementFactory {
        PreparedStatement create() throws SQLException;
    }

    private static final Logger LOG = LoggerFactory.getLogger(Application.class);
    private static final String STUDENT_TABLE = "student";
    private static final String TEACHER_TABLE = "teacher";
    private static final String[] TABLES = new String[]{STUDENT_TABLE, TEACHER_TABLE};

    @Autowired
    private DataSource dataSource;

    @PostConstruct
    public void executeChecks() throws SQLException {
        // With sonar alert
        executeStatementFrom(() -> dataSource.getConnection().prepareStatement("select * from " + TABLES[0] + " where id = ?"));
        // No Sonar alert:
        final String sql = String.format("select * from %s where id = ?", TABLES[1]);
        executeStatementFrom(() -> dataSource.getConnection().prepareStatement(sql));
        for (String tableName : TABLES) {
            // With sonar WTF:
            executeStatementFrom(() -> dataSource.getConnection().prepareStatement("select * from " + tableName + " where id = ?"));
            // Still no Sonar alert:
            executeStatementFrom(() -> dataSource.getConnection().prepareStatement(String.format("select * from %s where id = ?", tableName)));
        }
    }

    private void executeStatementFrom(StatementFactory statementFactory) throws SQLException {
        Validate.notNull(dataSource);
        try (PreparedStatement ps = statementFactory.create()) {
            ps.setInt(1, 10_001);
            final List<Object> rows = Lists.newArrayList();
            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columns = metaData.getColumnCount();
                while (rs.next()) {
                    HashMap<Object, Object> map = Maps.newHashMap();
                    for (int i = 1; i <= columns; ++i) {
                        map.put(metaData.getColumnName(i), rs.getObject(i));
                    }
                    rows.add(map);
                }
            }
            LOG.info("Result set received: rows={}", rows);
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(local.tests.upya.jdbc.Application.class, args);
    }
}
