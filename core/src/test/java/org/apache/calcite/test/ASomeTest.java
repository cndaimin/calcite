package org.apache.calcite.test;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.MaterializedViewSubstitutionVisitor;
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

public class ASomeTest {

    @Test
    public void test() throws Exception {
        String mv = "SELECT * FROM (\n" +
            "    SELECT \"empid\", \"deptno\", SUM(\"salary\") AS \"m\" FROM \"emps\" GROUP BY \"empid\", \"deptno\"\n" +
            ") WHERE \"m\" > 0";
        String query = "SELECT \"empid\", SUM(\"salary\") AS \"m\" FROM \"emps\" GROUP BY \"empid\"";


        RelNode rel_mv = compile(mv);
        RelNode rel_query = compile(query);

        SchemaPlus hr = rootSchema.getSubSchema("hr");
        hr.add("mv", ViewTable.viewMacro(hr, mv,
                Collections.singletonList("hr"),
                Arrays.asList("hr", "mv"), false));

        RelNode tableScan = relBuilder.scan("hr", "mv").build();

        System.out.println("query:");
        show(rel_query);
        System.out.println("mv:");
        show(rel_mv);
        System.out.println("scan:");
        show(tableScan);

        List<RelNode> relNodes = new MaterializedViewSubstitutionVisitor(rel_mv, rel_query).go(tableScan);
        relNodes.forEach(relNode -> {
            System.out.println(RelOptUtil.toString(relNode));
        });
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
