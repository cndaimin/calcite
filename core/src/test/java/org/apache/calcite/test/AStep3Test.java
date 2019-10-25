package org.apache.calcite.test;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.AbstractMaterializedViewRule;
import org.apache.calcite.rel.rules.MaterializedViewFilterScanRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class AStep3Test {

    static List<RelOptRule> rules = Arrays.asList(
            MaterializedViewFilterScanRule.INSTANCE,
            AbstractMaterializedViewRule.INSTANCE_PROJECT_FILTER,
            AbstractMaterializedViewRule.INSTANCE_FILTER,
            AbstractMaterializedViewRule.INSTANCE_PROJECT_JOIN,
            AbstractMaterializedViewRule.INSTANCE_JOIN,
            AbstractMaterializedViewRule.INSTANCE_PROJECT_AGGREGATE,
            AbstractMaterializedViewRule.INSTANCE_AGGREGATE
    );

    @Test
    public void test() throws Exception {
        String mv = "SELECT \"empid\" FROM \"emps\" JOIN \"depts\" USING (\"deptno\")";
        String query = "SELECT \"empid\" FROM \"depts\" JOIN (\n" +
                "  SELECT \"empid\", \"deptno\"\n" +
                "  FROM \"emps\"\n" +
                "  WHERE \"empid\" = 1) AS \"subq\"\n" +
                "ON \"depts\".\"deptno\" = \"subq\".\"deptno\"";

        RelNode rel_mv = compile(mv);
        RelNode rel_query = compile(query);

        SchemaPlus hr = rootSchema.getSubSchema("hr");
        hr.add("mv", ViewTable.viewMacro(hr, mv,
                Collections.singletonList("hr"),
                Arrays.asList("hr", "mv"), false));

        RelNode tableScan = relBuilder.scan("hr", "mv").build();
        show(tableScan);

        show(compile("SELECT * from \"mv\""));

        RelOptMaterialization mat1 = new RelOptMaterialization(
                tableScan, rel_mv, null, ImmutableList.of("hr", "mv"));


        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addMatchOrder(HepMatchOrder.TOP_DOWN);
        for(RelOptRule rule: rules) {
            builder.addRuleInstance(rule);
        }
        HepPlanner planner = new HepPlanner(builder.build(), null, true, null, RelOptCostImpl.FACTORY);

        planner.addMaterialization(mat1);
        planner.setRoot(rel_query);
        System.out.println("# Result: ");
        RelNode result = planner.findBestExp();
        show(result);

        RelToSqlConverter converter = new RelToSqlConverter(new SqlDialect(SqlDialect.EMPTY_CONTEXT));
        SqlNode sqlNode = converter.visitChild(0, result).asStatement();
        System.out.println(sqlNode.toString());
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
