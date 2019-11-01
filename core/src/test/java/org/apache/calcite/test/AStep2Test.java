package org.apache.calcite.test;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptMaterializations;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class AStep2Test {

    @Test
    public void test() throws Exception {
        String mv = "SELECT \"empid\", \"deptno\", SUM(\"salary\") FROM \"emps\" GROUP BY \"empid\", \"deptno\"";
        String query = "SELECT * FROM (\n" +
            "\t\tSELECT \"deptno\", SUM(\"salary\") FROM \"emps\" GROUP BY \"deptno\"\n" +
            "\t) AS A\n" +
            "\tJOIN \n" +
            "\t(\n" +
            "\t\tSELECT \"empid\", SUM(\"salary\") FROM \"emps\" GROUP BY \"empid\"\n" +
            "\t) AS B\n" +
            "\tON A.\"deptno\" = B.\"empid\"";

        RelNode rel_mv = compile(mv);
        RelNode rel_query = compile(query);

        rootSchema.getSubSchema("hr").add("mv", new ASomeTest.SimpleTable(rel_mv.getRowType()));

        RelNode tableScan = relBuilder.scan("hr", "mv").build();

        System.out.println("query:");
        show(rel_query);
        System.out.println("mv:");
        show(rel_mv);
        System.out.println("scan:");
        show(tableScan);

        RelOptMaterialization mat1 = new RelOptMaterialization(
                tableScan, rel_mv, null, ImmutableList.of("hr", "mv"));
        List<Pair<RelNode, List<RelOptMaterialization>>> result =
                RelOptMaterializations.useMaterializedViews(rel_query, Arrays.asList(mat1));
        System.out.println("###");
        System.out.println("size: " + result.size());
        result.forEach(pair -> {
            System.out.println("---");
            System.out.println("result: ");
            show(pair.left);

            pair.right.forEach(m -> {
                System.out.println("used mv: " + m.qualifiedTableName);
            });
        });
        System.out.println("###");
    }

    private RelNode compile(String sql) throws SqlParseException, ValidationException, RelConversionException {
        SqlNode parse = planner.parse(sql);
        SqlNode validate = planner.validate(parse);
        RelNode convert = planner.rel(validate).rel;
        planner.close();
        return convert;
    }

    private void show(RelNode relNode) {
        System.out.println(RelOptUtil.toString(relNode));
    }

    // Before
    private SchemaPlus rootSchema;
    private Planner planner;
    private RelBuilder relBuilder;

    @Before
    public void setUp() {
        rootSchema = Frameworks.createRootSchema(true);
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.Config.DEFAULT)
                .executor(RexUtil.EXECUTOR)
                .defaultSchema(
                        CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.HR))
                .build();
        planner = Frameworks.getPlanner(config);
        relBuilder = RelBuilder.create(config);
    }

    @After
    public void tearDown() {
        rootSchema = null;
        planner = null;
    }
}
