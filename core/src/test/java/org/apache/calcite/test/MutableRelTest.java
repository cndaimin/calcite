/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.test;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.mutable.MutableRel;
import org.apache.calcite.rel.mutable.MutableRels;
import org.apache.calcite.rel.mutable.MutableScan;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.SemiJoinRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Litmus;

import com.google.common.collect.ImmutableList;

import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link MutableRel} sub-classes.
 */
public class MutableRelTest {

  @Test public void testConvertAggregate() {
    checkConvertMutableRel(
        "Aggregate",
        "select empno, sum(sal) from emp group by empno");
  }

  @Test public void testConvertFilter() {
    checkConvertMutableRel(
        "Filter",
        "select * from emp where ename = 'DUMMY'");
  }

  @Test public void testConvertProject() {
    checkConvertMutableRel(
        "Project",
        "select ename from emp");
  }

  @Test public void testConvertSort() {
    checkConvertMutableRel(
        "Sort",
        "select * from emp order by ename");
  }

  @Test public void testConvertCalc() {
    checkConvertMutableRel(
        "Calc",
        "select * from emp where ename = 'DUMMY'",
        false,
        ImmutableList.of(FilterToCalcRule.INSTANCE));
  }

  @Test public void testConvertWindow() {
    checkConvertMutableRel(
        "Window",
        "select sal, avg(sal) over (partition by deptno) from emp",
        false,
        ImmutableList.of(ProjectToWindowRule.PROJECT));
  }

  @Test public void testConvertCollect() {
    checkConvertMutableRel(
        "Collect",
        "select multiset(select deptno from dept) from (values(true))");
  }

  @Test public void testConvertUncollect() {
    checkConvertMutableRel(
        "Uncollect",
        "select * from unnest(multiset[1,2])");
  }

  @Test public void testConvertTableModify() {
    checkConvertMutableRel(
        "TableModify",
        "insert into dept select empno, ename from emp");
  }

  @Test public void testConvertSample() {
    checkConvertMutableRel(
        "Sample",
        "select * from emp tablesample system(50) where empno > 5");
  }

  @Test public void testConvertTableFunctionScan() {
    checkConvertMutableRel(
        "TableFunctionScan",
        "select * from table(ramp(3))");
  }

  @Test public void testConvertValues() {
    checkConvertMutableRel(
        "Values",
        "select * from (values (1, 2))");
  }

  @Test public void testConvertJoin() {
    checkConvertMutableRel(
        "Join",
        "select * from emp join dept using (deptno)");
  }

  @Test public void testConvertSemiJoin() {
    final String sql = "select * from dept where exists (\n"
        + "  select * from emp\n"
        + "  where emp.deptno = dept.deptno\n"
        + "  and emp.sal > 100)";
    checkConvertMutableRel(
        "Join", // with join type as semi
        sql,
        true,
        ImmutableList.of(
            FilterProjectTransposeRule.INSTANCE,
            FilterJoinRule.FILTER_ON_JOIN,
            ProjectMergeRule.INSTANCE,
            SemiJoinRule.PROJECT));
  }

  @Test public void testConvertCorrelate() {
    final String sql = "select * from dept where exists (\n"
        + "  select * from emp\n"
        + "  where emp.deptno = dept.deptno\n"
        + "  and emp.sal > 100)";
    checkConvertMutableRel("Correlate", sql);
  }

  @Test public void testConvertUnion() {
    checkConvertMutableRel(
        "Union",
        "select * from emp where deptno = 10"
        + "union select * from emp where ename like 'John%'");
  }

  @Test public void testConvertMinus() {
    checkConvertMutableRel(
        "Minus",
        "select * from emp where deptno = 10"
        + "except select * from emp where ename like 'John%'");
  }

  @Test public void testConvertIntersect() {
    checkConvertMutableRel(
        "Intersect",
        "select * from emp where deptno = 10"
        + "intersect select * from emp where ename like 'John%'");
  }

  @Test public void testUpdateInputOfUnion() {
    MutableRel mutableRel = createMutableRel(
        "select sal from emp where deptno = 10"
            + "union select sal from emp where ename like 'John%'");
    MutableRel childMutableRel = createMutableRel(
        "select sal from emp where deptno = 12");
    mutableRel.setInput(0, childMutableRel);
    String actual = RelOptUtil.toString(MutableRels.fromMutable(mutableRel));
    String expected = ""
        + "LogicalUnion(all=[false])\n"
        + "  LogicalProject(SAL=[$5])\n"
        + "    LogicalFilter(condition=[=($7, 12)])\n"
        + "      LogicalTableScan(table=[[CATALOG, SALES, EMP]])\n"
        + "  LogicalProject(SAL=[$5])\n"
        + "    LogicalFilter(condition=[LIKE($1, 'John%')])\n"
        + "      LogicalTableScan(table=[[CATALOG, SALES, EMP]])\n";
    MatcherAssert.assertThat(actual, Matchers.isLinux(expected));
  }

  @Test public void testParentInfoOfUnion() {
    MutableRel mutableRel = createMutableRel(
        "select sal from emp where deptno = 10"
            + "union select sal from emp where ename like 'John%'");
    for (MutableRel input: mutableRel.getInputs()) {
      Assert.assertTrue(input.getParent() == mutableRel);
    }
  }

  @Test public void testMutableTableFunctionScanEquals() {
    final String sql = "SELECT * FROM TABLE(RAMP(3))";
    final MutableRel mutableRel1 = createMutableRel(sql);
    final MutableRel mutableRel2 = createMutableRel(sql);
    final String actual = RelOptUtil.toString(MutableRels.fromMutable(mutableRel1));
    final String expected = ""
        + "LogicalProject(I=[$0])\n"
        + "  LogicalTableFunctionScan(invocation=[RAMP(3)], rowType=[RecordType(INTEGER I)])\n";
    MatcherAssert.assertThat(actual, Matchers.isLinux(expected));
    Assert.assertEquals(mutableRel1, mutableRel2);
  }

  /** Verifies that after conversion to and from a MutableRel, the new
   * RelNode remains identical to the original RelNode. */
  private static void checkConvertMutableRel(String rel, String sql) {
    checkConvertMutableRel(rel, sql, false, null);
  }

  /** Verifies that after conversion to and from a MutableRel, the new
   * RelNode remains identical to the original RelNode. */
  private static void checkConvertMutableRel(
      String rel, String sql, boolean decorrelate, List<RelOptRule> rules) {
    final SqlToRelTestBase test = new SqlToRelTestBase() {
    };
    RelNode origRel = test.createTester().convertSqlToRel(sql).rel;
    if (decorrelate) {
      final RelBuilder relBuilder =
          RelFactories.LOGICAL_BUILDER.create(origRel.getCluster(), null);
      origRel = RelDecorrelator.decorrelateQuery(origRel, relBuilder);
    }
    if (rules != null) {
      final HepProgram hepProgram =
          new HepProgramBuilder().addRuleCollection(rules).build();
      final HepPlanner hepPlanner = new HepPlanner(hepProgram);
      hepPlanner.setRoot(origRel);
      origRel = hepPlanner.findBestExp();
    }
    // Convert to and from a mutable rel.
    final MutableRel mutableRel = MutableRels.toMutable(origRel);
    final RelNode newRel = MutableRels.fromMutable(mutableRel);

    // Check if the mutable rel digest contains the target rel.
    final String mutableRelStr = mutableRel.deep();
    final String msg1 =
        "Mutable rel: " + mutableRelStr + " does not contain target rel: " + rel;
    Assert.assertTrue(msg1, mutableRelStr.contains(rel));

    // Check if the mutable rel's row-type is identical to the original
    // rel's row-type.
    final RelDataType origRelType = origRel.getRowType();
    final RelDataType mutableRelType = mutableRel.rowType;
    final String msg2 =
        "Mutable rel's row type does not match with the original rel.\n"
        + "Original rel type: " + origRelType
        + ";\nMutable rel type: " + mutableRelType;
    Assert.assertTrue(
        msg2,
        RelOptUtil.equal(
            "origRelType", origRelType,
            "mutableRelType", mutableRelType,
            Litmus.IGNORE));

    // Check if the new rel converted from the mutable rel is identical
    // to the original rel.
    final String origRelStr = RelOptUtil.toString(origRel);
    final String newRelStr = RelOptUtil.toString(newRel);
    final String msg3 =
        "The converted new rel is different from the original rel.\n"
        + "Original rel: " + origRelStr + ";\nNew rel: " + newRelStr;
    Assert.assertEquals(msg3, origRelStr, newRelStr);
  }

  private static MutableRel createMutableRel(String sql) {
    final SqlToRelTestBase test = new SqlToRelTestBase() {
    };
    RelNode rel = test.createTester().convertSqlToRel(sql).rel;
    return MutableRels.toMutable(rel);
  }

  /** Verifies equivalence of {@link MutableScan} */
  @Test public void testMutableScanEquivalence() {
    final FrameworkConfig config = RelBuilderTest.config().build();
    final SchemaPlus schema = config.getDefaultSchema();
    final RelBuilder builder = RelBuilder.create(config);
    schema.add("scott.EMP", new ScannableTableTest.SimpleTable());

    assertThat(mutableScanOf(builder, "EMP"),
        equalTo(mutableScanOf(builder, "EMP")));
    assertThat(mutableScanOf(builder, "scott.EMP"),
        equalTo(mutableScanOf(builder, "scott.EMP")));
    assertThat(mutableScanOf(builder, "scott", "scott.EMP"),
        equalTo(mutableScanOf(builder, "scott.EMP")));
    assertThat(mutableScanOf(builder, "scott", "scott.EMP"),
        equalTo(mutableScanOf(builder, "scott", "scott.EMP")));

    assertThat(mutableScanOf(builder, "EMP"),
        not(equalTo(mutableScanOf(builder, "DEPT"))));
    assertThat(mutableScanOf(builder, "EMP"),
        not(equalTo(mutableScanOf(builder, "scott.EMP"))));
    assertThat(mutableScanOf(builder, "scott", "EMP"),
        not(equalTo(mutableScanOf(builder, "scott.EMP"))));
    assertThat(mutableScanOf(builder, "scott", "EMP"),
        not(equalTo(mutableScanOf(builder, "scott", "scott.EMP"))));
  }

  private MutableScan mutableScanOf(RelBuilder builder, String... tableNames) {
    final RelNode scan = builder.scan(tableNames).build();
    return (MutableScan) MutableRels.toMutable(scan);
  }

}

// End MutableRelTest.java
