package org.apache.calcite.test;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.SubstitutionVisitor;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.schema.SchemaPlus;
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

import java.util.List;

public class ASomeTest {

  @Test
  public void test() throws Exception {
    String mv = "SELECT * FROM \"emps\" WHERE \"salary\" < 3000";
    String query = "SELECT * FROM \"emps\" WHERE \"salary\" > 5000";

    System.out.println(mv);
    System.out.println(query);

    RelNode rel_mv = convertSqlToRel(mv);
    RelNode rel_query = convertSqlToRel(query);

    rootSchema.getSubSchema("hr").add("mv", new ScannableTableTest.SimpleTable());
    RelNode tableScan = relBuilder.scan("hr", "mv").build();

    System.out.println("query:");
    show(rel_query);
    System.out.println("mv:");
    show(rel_mv);
    System.out.println("scan:");
    show(tableScan);
    System.out.println("--- canonicalize -->");

    rel_query = canonicalize(rel_query);
    rel_mv = canonicalize(rel_mv);

    System.out.println("query:");
    show(rel_query);
    System.out.println("mv:");
    show(rel_mv);

    System.out.println("--- results ------>");

    List<RelNode> relNodes = new SubstitutionVisitor(rel_mv, rel_query).go(tableScan);
    relNodes.forEach(rel -> {
      show(rel);

    });
    if (relNodes.isEmpty()) {
      System.out.println("nothing");
    }
  }

  private RelNode convertSqlToRel(String sql) throws SqlParseException, ValidationException, RelConversionException {
    SqlNode parse = planner.parse(sql);
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.rel(validate).rel;
    planner.close();
    return convert;
  }

  private RelNode canonicalize(RelNode rel) {
    hepPlanner.setRoot(rel);
    return hepPlanner.findBestExp();
  }

  private void show(RelNode relNode) {
    System.out.println(RelOptUtil.toString(relNode));
  }

  // Before
  private SchemaPlus rootSchema;
  private Planner planner;
  private RelBuilder relBuilder;
  private HepPlanner hepPlanner;

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

    HepProgram program =
        new HepProgramBuilder()
            .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
            .addRuleInstance(FilterMergeRule.INSTANCE)
            .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
            .addRuleInstance(FilterJoinRule.JOIN)
            .addRuleInstance(FilterAggregateTransposeRule.INSTANCE)
            .addRuleInstance(ProjectMergeRule.INSTANCE)
            .addRuleInstance(ProjectRemoveRule.INSTANCE)
            .addRuleInstance(ProjectJoinTransposeRule.INSTANCE)
            .addRuleInstance(FilterToCalcRule.INSTANCE)
            .addRuleInstance(ProjectToCalcRule.INSTANCE)
            .addRuleInstance(FilterCalcMergeRule.INSTANCE)
            .addRuleInstance(ProjectCalcMergeRule.INSTANCE)
            .addRuleInstance(CalcMergeRule.INSTANCE)
            .build();

    hepPlanner = new HepPlanner(program);
  }

  @After
  public void tearDown() {
    rootSchema = null;
    planner = null;
  }
}
