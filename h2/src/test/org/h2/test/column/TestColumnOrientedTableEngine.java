/**
 * CSC 560 - Column-oriented table storage tests
 */
package org.h2.test.column;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.h2.api.TableEngine;
import org.h2.command.ddl.CreateTableData;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.index.BaseIndex;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexLookupBatch;
import org.h2.index.IndexType;
import org.h2.index.SingleRowCursor;
import org.h2.jdbc.JdbcConnection;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.*;
import org.h2.test.TestBase;
import org.h2.util.DoneFuture;
import org.h2.util.New;
import org.h2.value.Value;
import org.h2.value.ValueInt;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;

import org.h2.column.ColumnarTable;
import org.h2.column.ColumnarTableEngine;

/**
 * Test the column-oriented table engine
 */
public class TestColumnOrientedTableEngine extends TestBase {

    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        testQueryExpressionFlag();
        testSubQueryInfo();
        testColumnOrientedIndex();
    }

    /**
     * An index that internally uses a tree set.
     */
    private static class ColumnOrientedIndex extends BaseIndex implements Comparator<SearchRow> {
        /**
         * Executor service to test batched joins.
         */
        static ExecutorService exec;

        static AtomicInteger lookupBatches = new AtomicInteger();

        int preferredBatchSize;

        final TreeSet<SearchRow> set = new TreeSet<>(this);

	final Map<Long,Long> keyToColumnarIndex = new HashMap<Long,Long>();   // allows conversion of a row key to an index in columnStore
	final Map<String,ArrayList> columnStore = new LinkedHashMap<String,ArrayList>();
	
        ColumnOrientedIndex(Table t, String name, IndexColumn[] cols, IndexType type) {
            initBaseIndex(t, 0, name, cols, type);
        }

        @Override
        public int compare(SearchRow o1, SearchRow o2) {
            int res = compareRows(o1, o2);
            if (res == 0) {
                if (o1.getKey() == Long.MAX_VALUE || o2.getKey() == Long.MIN_VALUE) {
                    res = 1;
                } else if (o1.getKey() == Long.MIN_VALUE || o2.getKey() == Long.MAX_VALUE) {
                    res = -1;
                }
            }
            return res;
        }

        @Override
        public IndexLookupBatch createLookupBatch(TableFilter[] filters, int f) {
            final TableFilter filter = filters[f];
            assert0(filter.getMasks() != null || "tableScan".equals(getName()), "masks");
            final int preferredSize = preferredBatchSize;
            if (preferredSize == 0) {
                return null;
            }
            lookupBatches.incrementAndGet();
            return new IndexLookupBatch() {
                List<SearchRow> searchRows = New.arrayList();

                @Override
                public String getPlanSQL() {
                    return "test";
                }

                @Override public boolean isBatchFull() {
                    return searchRows.size() >= preferredSize * 2;
                }

                @Override
                public List<Future<Cursor>> find() {
                    List<Future<Cursor>> res = findBatched(filter, searchRows);
                    searchRows.clear();
                    return res;
                }

                @Override
                public boolean addSearchRows(SearchRow first, SearchRow last) {
                    assert !isBatchFull();
                    searchRows.add(first);
                    searchRows.add(last);
                    return true;
                }

                @Override
                public void reset(boolean beforeQuery) {
                    searchRows.clear();
                }
            };
        }

        public List<Future<Cursor>> findBatched(final TableFilter filter,
                List<SearchRow> firstLastPairs) {
            ArrayList<Future<Cursor>> result = New.arrayList(firstLastPairs.size());
            final Random rnd = new Random();
            for (int i = 0; i < firstLastPairs.size(); i += 2) {
                final SearchRow first = firstLastPairs.get(i);
                final SearchRow last = firstLastPairs.get(i + 1);
                Future<Cursor> future;
                if (rnd.nextBoolean()) {
                    IteratorCursor c = (IteratorCursor) find(filter, first, last);
                    if (c.it.hasNext()) {
                        future = new DoneFuture<Cursor>(c);
                    } else {
                        // we can return null instead of future of empty cursor
                        future = null;
                    }
                } else {
                    future = exec.submit(new Callable<Cursor>() {
                        @Override
                        public Cursor call() throws Exception {
                            if (rnd.nextInt(50) == 0) {
                                Thread.sleep(0, 500);
                            }
                            return find(filter, first, last);
                        }
                    });
                }
                result.add(future);
            }
            return result;
        }

        @Override
        public void close(Session session) {
            // No-op.
        }

        @Override
        public void add(Session session, Row row) {
            set.add(row);
        }

        @Override
        public void remove(Session session, Row row) {
            set.remove(row);
        }

        private static SearchRow mark(SearchRow row, boolean first) {
            if (row != null) {
                // Mark this row to be a search row.
                row.setKey(first ? Long.MIN_VALUE : Long.MAX_VALUE);
            }
            return row;
        }

        @Override
        public Cursor find(Session session, SearchRow first, SearchRow last) {
            Set<SearchRow> subSet;
            if (first != null && last != null && compareRows(last, first) < 0) {
                subSet = Collections.emptySet();
            } else {
                if (first != null) {
                    first = set.floor(mark(first, true));
                }
                if (last != null) {
                    last = set.ceiling(mark(last, false));
                }
                if (first == null && last == null) {
                    subSet = set;
                } else if (first != null) {
                    if (last != null) {
                        subSet = set.subSet(first,  true, last, true);
                    } else {
                        subSet = set.tailSet(first, true);
                    }
                } else if (last != null) {
                    subSet = set.headSet(last, true);
                } else {
                    throw new IllegalStateException();
                }
            }
            return new IteratorCursor(subSet.iterator());
        }

        private static String alias(SubQueryInfo info) {
            return info.getFilters()[info.getFilter()].getTableAlias();
        }

        private void checkInfo(SubQueryInfo info) {
            if (info.getUpper() == null) {
                // check 1st level info
                assert0(info.getFilters().length == 1, "getFilters().length " +
                        info.getFilters().length);
                String alias = alias(info);
                assert0("T5".equals(alias), "alias: " + alias);
            } else {
                // check 2nd level info
                assert0(info.getFilters().length == 2, "getFilters().length " +
                        info.getFilters().length);
                String alias = alias(info);
                assert0("T4".equals(alias), "alias: " + alias);
                checkInfo(info.getUpper());
            }
        }

        protected void doTests(Session session) {
            if (getTable().getName().equals("SUB_QUERY_TEST")) {
                checkInfo(session.getSubQueryInfo());
            } else if (getTable().getName().equals("EXPR_TEST")) {
                assert0(session.getSubQueryInfo() == null, "select expression");
            } else if (getTable().getName().equals("EXPR_TEST2")) {
                String alias = alias(session.getSubQueryInfo());
                assert0(alias.equals("ZZ"), "select expression sub-query: " + alias);
                assert0(session.getSubQueryInfo().getUpper() == null, "upper");
            } else if (getTable().getName().equals("QUERY_EXPR_TEST")) {
                assert0(session.isPreparingQueryExpression(), "preparing query expression");
            } else if (getTable().getName().equals("QUERY_EXPR_TEST_NO")) {
                assert0(!session.isPreparingQueryExpression(), "not preparing query expression");
            }
        }

        @Override
        public double getCost(Session session, int[] masks,
                TableFilter[] filters, int filter, SortOrder sortOrder,
                HashSet<Column> allColumnsSet) {
            doTests(session);
            return getCostRangeIndex(masks, set.size(), filters, filter,
                    sortOrder, false, allColumnsSet);
        }

        @Override
        public void remove(Session session) {
            // No-op.
        }

        @Override
        public void truncate(Session session) {
            set.clear();
        }

        @Override
        public boolean canGetFirstOrLast() {
            return true;
        }

        @Override
        public Cursor findFirstOrLast(Session session, boolean first) {
            return new SingleRowCursor((Row)
                    (set.isEmpty() ? null : first ? set.first() : set.last()));
        }

        @Override
        public boolean needRebuild() {
            return true;
        }

        @Override
        public long getRowCount(Session session) {
	    List<ArrayList> vals = (List<ArrayList>) columnStore.values();
	    return vals.get(0).size();
        }

        @Override
        public long getRowCountApproximation() {
            return getRowCount(null);
        }

        @Override
        public long getDiskSpaceUsed() {
            return 0;
        }

        @Override
        public void checkRename() {
            // No-op.
        }
    }

    /**
     */
    private static class IteratorCursor implements Cursor {
        Iterator<SearchRow> it;
        private Row current;

        IteratorCursor(Iterator<SearchRow> it) {
            this.it = it;
        }

        @Override
        public boolean previous() {
            throw DbException.getUnsupportedException("prev");
        }

        @Override
        public boolean next() {
            if (it.hasNext()) {
                current = (Row) it.next();
                return true;
            }
            current = null;
            return false;
        }

        @Override
        public SearchRow getSearchRow() {
            return get();
        }

        @Override
        public Row get() {
            return current;
        }

        @Override
        public String toString() {
            return "IteratorCursor->" + current;
        }
    }

    /**
     * A comparator for rows (lists of comparable objects).
     */
    private static class RowComparator implements Comparator<List<Object>> {
        private int[] cols;
        private boolean descending;

        RowComparator(int... cols) {
            this.descending = false;
            this.cols = cols;
        }

        RowComparator(boolean descending, int... cols) {
            this.descending = descending;
            this.cols = cols;
        }

        @SuppressWarnings("unchecked")
        @Override
        public int compare(List<Object> row1, List<Object> row2) {
            for (int i = 0; i < cols.length; i++) {
                int col = cols[i];
                Comparable<Object> o1 = (Comparable<Object>) row1.get(col);
                Comparable<Object> o2 = (Comparable<Object>) row2.get(col);
                if (o1 == null) {
                    return applyDescending(o2 == null ? 0 : -1);
                }
                if (o2 == null) {
                    return applyDescending(1);
                }
                int res = o1.compareTo(o2);
                if (res != 0) {
                    return applyDescending(res);
                }
            }
            return 0;
        }

        private int applyDescending(int v) {
            if (!descending) {
                return v;
            }
            if (v == 0) {
                return v;
            }
            return -v;
        }
    }

    /**
     * A filter for rows (lists of objects).
     */
    abstract static class RowFilter {

        /**
         * Check whether the row needs to be processed.
         *
         * @param row the row
         * @return true if yes
         */
        protected abstract boolean accept(List<Object> row);

        /**
         * Get an integer from a row.
         *
         * @param row the row
         * @param col the column index
         * @return the value
         */
        protected Integer getInt(List<Object> row, int col) {
            return (Integer) row.get(col);
        }

        /**
         * Get a long from a row.
         *
         * @param row the row
         * @param col the column index
         * @return the value
         */
        protected Long getLong(List<Object> row, int col) {
            return (Long) row.get(col);
        }

        /**
         * Get a string from a row.
         *
         * @param row the row
         * @param col the column index
         * @return the value
         */
        protected String getString(List<Object> row, int col) {
            return (String) row.get(col);
        }

    }

    
    
    private void testStatements(Statement stat) throws SQLException {
        assertEquals(stat.executeUpdate("INSERT INTO t1 VALUES(2, 'abc')"), 1);
        assertEquals(stat.executeUpdate("UPDATE t1 SET name = 'abcdef' WHERE id=2"), 1);
        assertEquals(stat.executeUpdate("INSERT INTO t1 VALUES(3, 'abcdefghi')"), 1);

        assertEquals(stat.executeUpdate("DELETE FROM t1 WHERE id=2"), 0);
        assertEquals(stat.executeUpdate("DELETE FROM t1 WHERE id=3"), 1);

        ResultSet rs = stat.executeQuery("SELECT name FROM t1");
        assertFalse(rs.next());
        rs.close();

        assertEquals(stat.executeUpdate("INSERT INTO t1 VALUES(2, 'abc')"), 1);
        assertEquals(stat.executeUpdate("UPDATE t1 SET name = 'abcdef' WHERE id=2"), 1);
        assertEquals(stat.executeUpdate("INSERT INTO t1 VALUES(3, 'abcdefghi')"), 1);

        rs = stat.executeQuery("SELECT name FROM t1");
        assertTrue(rs.next());
        assertEquals(rs.getString(1), "abcdefghi");
        assertFalse(rs.next());
        rs.close();

    }

    private void testColumnOrientedIndex() throws SQLException {
        deleteDb("tableEngine");
        Connection conn = getConnection("tableEngine;MV_STORE=FALSE");
        Statement stat = conn.createStatement();

        stat.executeUpdate("CREATE TABLE T(A INT, B VARCHAR, C BIGINT, " +
			   "D BIGINT DEFAULT 0) ENGINE \"" +
			   ColumnarTableEngine.class.getName() + "\"");

        stat.executeUpdate("CREATE INDEX IDX_C_B_A ON T(C, B, A)");
        stat.executeUpdate("CREATE INDEX IDX_B_A ON T(B, A)");

        List<List<Object>> dataSet = New.arrayList();

        dataSet.add(Arrays.<Object>asList(1, "1", 1L));
        dataSet.add(Arrays.<Object>asList(1, "0", 2L));
        dataSet.add(Arrays.<Object>asList(2, "0", -1L));
        dataSet.add(Arrays.<Object>asList(0, "0", 1L));
        dataSet.add(Arrays.<Object>asList(0, "1", null));
        dataSet.add(Arrays.<Object>asList(2, null, 0L));

        PreparedStatement prep = conn.prepareStatement("INSERT INTO T(A,B,C) VALUES(?,?,?)");
        for (List<Object> row : dataSet) {
            for (int i = 0; i < row.size(); i++) {
                prep.setObject(i + 1, row.get(i));
            }
            assertEquals(1, prep.executeUpdate());
        }
        prep.close();

        checkPlan(stat, "select max(c) from t", "direct lookup");
        checkPlan(stat, "select min(c) from t", "direct lookup");
        checkPlan(stat, "select count(*) from t", "direct lookup");

        checkPlan(stat, "select * from t", "tableScan");

        //checkPlan(stat, "select * from t order by c", "IDX_C_B_A");
        //checkPlan(stat, "select * from t order by c, b", "IDX_C_B_A");
        //checkPlan(stat, "select * from t order by b", "IDX_B_A");
        //checkPlan(stat, "select * from t order by b, a", "IDX_B_A");
        checkPlan(stat, "select * from t order by b, c", "tableScan");
        checkPlan(stat, "select * from t order by a, b", "tableScan");
        checkPlan(stat, "select * from t order by a, c, b", "tableScan");

        //checkPlan(stat, "select * from t where b > ''", "IDX_B_A");
        //checkPlan(stat, "select * from t where a > 0 and b > ''", "IDX_B_A");
        //checkPlan(stat, "select * from t where b < ''", "IDX_B_A");
        //checkPlan(stat, "select * from t where b < '' and c < 1", "IDX_C_B_A");
        checkPlan(stat, "select * from t where a = 0", "tableScan");
        //checkPlan(stat, "select * from t where a > 0 order by c, b", "IDX_C_B_A");
        //checkPlan(stat, "select * from t where a = 0 and c > 0", "IDX_C_B_A");
        //checkPlan(stat, "select * from t where a = 0 and b < 0", "IDX_B_A");

        assertEquals(6, ((Number) query(stat, "select count(*) from t").get(0).get(0)).intValue());

        checkResultsNoOrder(stat, 6, "select * from t", "select * from t order by a");
        checkResultsNoOrder(stat, 6, "select * from t", "select * from t order by b");
        checkResultsNoOrder(stat, 6, "select * from t", "select * from t order by c");
        checkResultsNoOrder(stat, 6, "select * from t", "select * from t order by c, a");
        checkResultsNoOrder(stat, 6, "select * from t", "select * from t order by b, a");
        checkResultsNoOrder(stat, 6, "select * from t", "select * from t order by c, b, a");
        checkResultsNoOrder(stat, 6, "select * from t", "select * from t order by a, c, b");

        checkResultsNoOrder(stat, 4, "select * from t where a > 0",
                "select * from t where a > 0 order by a");
        checkResultsNoOrder(stat, 4, "select * from t where a > 0",
                "select * from t where a > 0 order by b");
        checkResultsNoOrder(stat, 4, "select * from t where a > 0",
                "select * from t where a > 0 order by c");
        checkResultsNoOrder(stat, 4, "select * from t where a > 0",
                "select * from t where a > 0 order by c, a");
        checkResultsNoOrder(stat, 4, "select * from t where a > 0",
                "select * from t where a > 0 order by b, a");
        checkResultsNoOrder(stat, 4, "select * from t where a > 0",
                "select * from t where a > 0 order by c, b, a");
        checkResultsNoOrder(stat, 4, "select * from t where a > 0",
                "select * from t where a > 0 order by a, c, b");

        checkResults(6, dataSet, stat,
                "select * from t order by a", null, new RowComparator(0));
        checkResults(6, dataSet, stat,
                "select * from t order by a desc", null, new RowComparator(true, 0));
        checkResults(6, dataSet, stat,
                "select * from t order by b, c", null, new RowComparator(1, 2));
        checkResults(6, dataSet, stat,
                "select * from t order by c, a", null, new RowComparator(2, 0));
        checkResults(6, dataSet, stat,
                "select * from t order by b, a", null, new RowComparator(1, 0));
        checkResults(6, dataSet, stat,
                "select * from t order by c, b, a", null, new RowComparator(2, 1, 0));

        checkResults(4, dataSet, stat,
                "select * from t where a > 0", new RowFilter() {
            @Override
            protected boolean accept(List<Object> row) {
                return getInt(row, 0) > 0;
            }
        }, null);
        checkResults(3, dataSet, stat, "select * from t where b = '0'", new RowFilter() {
            @Override
            protected boolean accept(List<Object> row) {
                return "0".equals(getString(row, 1));
            }
        }, null);
        checkResults(5, dataSet, stat, "select * from t where b >= '0'", new RowFilter() {
            @Override
            protected boolean accept(List<Object> row) {
                String b = getString(row, 1);
                return b != null && b.compareTo("0") >= 0;
            }
        }, null);
        checkResults(2, dataSet, stat, "select * from t where b > '0'", new RowFilter() {
            @Override
            protected boolean accept(List<Object> row) {
                String b = getString(row, 1);
                return b != null && b.compareTo("0") > 0;
            }
        }, null);
        checkResults(1, dataSet, stat, "select * from t where b > '0' and c > 0", new RowFilter() {
            @Override
            protected boolean accept(List<Object> row) {
                String b = getString(row, 1);
                Long c = getLong(row, 2);
                return b != null && b.compareTo("0") > 0 && c != null && c > 0;
            }
        }, null);
        checkResults(1, dataSet, stat, "select * from t where b > '0' and c < 2", new RowFilter() {
            @Override
            protected boolean accept(List<Object> row) {
                String b = getString(row, 1);
                Long c = getLong(row, 2);
                return b != null && b.compareTo("0") > 0 && c != null && c < 2;
            }
        }, null);
        checkResults(2, dataSet, stat, "select * from t where b > '0' and a < 2", new RowFilter() {
            @Override
            protected boolean accept(List<Object> row) {
                Integer a = getInt(row, 0);
                String b = getString(row, 1);
                return b != null && b.compareTo("0") > 0 && a != null && a < 2;
            }
        }, null);
        checkResults(1, dataSet, stat, "select * from t where b > '0' and a > 0", new RowFilter() {
            @Override
            protected boolean accept(List<Object> row) {
                Integer a = getInt(row, 0);
                String b = getString(row, 1);
                return b != null && b.compareTo("0") > 0 && a != null && a > 0;
            }
        }, null);
        checkResults(2, dataSet, stat, "select * from t where b = '0' and a > 0", new RowFilter() {
            @Override
            protected boolean accept(List<Object> row) {
                Integer a = getInt(row, 0);
                String b = getString(row, 1);
                return "0".equals(b) && a != null && a > 0;
            }
        }, null);
        checkResults(2, dataSet, stat, "select * from t where b = '0' and a < 2", new RowFilter() {
            @Override
            protected boolean accept(List<Object> row) {
                Integer a = getInt(row, 0);
                String b = getString(row, 1);
                return "0".equals(b) && a != null && a < 2;
            }
        }, null);
        conn.close();
        deleteDb("tableEngine");
    }

    private void testQueryExpressionFlag() throws SQLException {
        deleteDb("testQueryExpressionFlag");
        Connection conn = getConnection("testQueryExpressionFlag;MV_STORE=FALSE");
        Statement stat = conn.createStatement();
        stat.execute("create table QUERY_EXPR_TEST(id int) ENGINE \"" +
                ColumnarTableEngine.class.getName() + "\"");
        stat.execute("create table QUERY_EXPR_TEST_NO(id int) ENGINE \"" +
                ColumnarTableEngine.class.getName() + "\"");
        stat.executeQuery("select 1 + (select 1 from QUERY_EXPR_TEST)").next();
        stat.executeQuery("select 1 from QUERY_EXPR_TEST_NO where id in "
                + "(select id from QUERY_EXPR_TEST)");
        stat.executeQuery("select 1 from QUERY_EXPR_TEST_NO n "
                + "where exists(select 1 from QUERY_EXPR_TEST y where y.id = n.id)");
        conn.close();
        deleteDb("testQueryExpressionFlag");
    }

    private void testSubQueryInfo() throws SQLException {
        deleteDb("testSubQueryInfo");
        Connection conn = getConnection("testSubQueryInfo;MV_STORE=FALSE");
        Statement stat = conn.createStatement();
        stat.execute("create table SUB_QUERY_TEST(id int primary key, name varchar) ENGINE \"" +
                ColumnarTableEngine.class.getName() + "\"");
        // test sub-queries
        stat.executeQuery("select * from "
                + "(select t2.id from "
                + "(select t3.id from sub_query_test t3 where t3.name = '') t4, "
                + "sub_query_test t2 "
                + "where t2.id = t4.id) t5").next();
        // test view 1
        stat.execute("create view t4 as (select t3.id from sub_query_test t3 where t3.name = '')");
        stat.executeQuery("select * from "
                + "(select t2.id from t4, sub_query_test t2 where t2.id = t4.id) t5").next();
        // test view 2
        stat.execute("create view t5 as "
                + "(select t2.id from t4, sub_query_test t2 where t2.id = t4.id)");
        stat.executeQuery("select * from t5").next();
        // test select expressions
        stat.execute("create table EXPR_TEST(id int) ENGINE \"" +
                ColumnarTableEngine.class.getName() + "\"");
        stat.executeQuery("select * from (select (select id from EXPR_TEST x limit 1) a "
                + "from dual where 1 = (select id from EXPR_TEST y limit 1)) z").next();
        // test select expressions 2
        stat.execute("create table EXPR_TEST2(id int) ENGINE \"" +
                ColumnarTableEngine.class.getName() + "\"");
        stat.executeQuery("select * from (select (select 1 from "
                + "(select (select 2 from EXPR_TEST) from EXPR_TEST2) ZZ) from dual)").next();
        // test select expression plan
        stat.execute("create table test_plan(id int primary key, name varchar)");
        stat.execute("create index MY_NAME_INDEX on test_plan(name)");
        checkPlan(stat, "select * from (select (select id from test_plan "
                + "where name = 'z') from dual)",
                "MY_NAME_INDEX");
        conn.close();
        deleteDb("testSubQueryInfo");
    }

    private void setBatchingEnabled(Statement stat, boolean enabled) throws SQLException {
        stat.execute("SET BATCH_JOINS " + enabled);
        if (!config.networked) {
            Session s = (Session) ((JdbcConnection) stat.getConnection()).getSession();
            assertEquals(enabled, s.isJoinBatchEnabled());
        }
    }

    private void testBatchedJoin() throws SQLException {
        deleteDb("testBatchedJoin");
        Connection conn = getConnection("testBatchedJoin;OPTIMIZE_REUSE_RESULTS=0;BATCH_JOINS=1;MV_STORE=FALSE");
        Statement stat = conn.createStatement();
        setBatchingEnabled(stat, false);
        setBatchingEnabled(stat, true);

        ColumnOrientedIndex.exec = Executors.newFixedThreadPool(8, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                return t;
            }
        });

        forceJoinOrder(stat, true);
        try {
            doTestBatchedJoinSubQueryUnion(stat);

            ColumnOrientedIndex.lookupBatches.set(0);
            doTestBatchedJoin(stat, 1, 0, 0);
            doTestBatchedJoin(stat, 0, 1, 0);
            doTestBatchedJoin(stat, 0, 0, 1);

            doTestBatchedJoin(stat, 0, 2, 0);
            doTestBatchedJoin(stat, 0, 0, 2);

            doTestBatchedJoin(stat, 0, 0, 3);
            doTestBatchedJoin(stat, 0, 0, 4);
            doTestBatchedJoin(stat, 0, 0, 5);

            doTestBatchedJoin(stat, 0, 3, 1);
            doTestBatchedJoin(stat, 0, 3, 3);
            doTestBatchedJoin(stat, 0, 3, 7);

            doTestBatchedJoin(stat, 0, 4, 1);
            doTestBatchedJoin(stat, 0, 4, 6);
            doTestBatchedJoin(stat, 0, 4, 20);

            doTestBatchedJoin(stat, 0, 10, 0);
            doTestBatchedJoin(stat, 0, 0, 10);

            doTestBatchedJoin(stat, 0, 20, 0);
            doTestBatchedJoin(stat, 0, 0, 20);
            doTestBatchedJoin(stat, 0, 20, 20);

            doTestBatchedJoin(stat, 3, 7, 0);
            doTestBatchedJoin(stat, 0, 0, 5);
            doTestBatchedJoin(stat, 0, 8, 1);
            doTestBatchedJoin(stat, 0, 2, 1);

            assertTrue(ColumnOrientedIndex.lookupBatches.get() > 0);
        } finally {
            forceJoinOrder(stat, false);
            ColumnOrientedIndex.exec.shutdownNow();
        }
        conn.close();
        deleteDb("testBatchedJoin");
    }

    private static void forceJoinOrder(Statement s, boolean force) throws SQLException {
        s.executeUpdate("SET FORCE_JOIN_ORDER " + force);
    }

    private void checkPlan(Statement stat, String sql) throws SQLException {
        ResultSet rs = stat.executeQuery("EXPLAIN " + sql);
        assertTrue(rs.next());
        String plan = rs.getString(1);
        assertEquals(normalize(sql), normalize(plan));
    }

    private static String normalize(String sql) {
        sql = sql.replace('\n', ' ');
        return sql.replaceAll("\\s+", " ").trim();
    }

    private void doTestBatchedJoinSubQueryUnion(Statement stat) throws SQLException {
        String engine = '"' + ColumnarTableEngine.class.getName() + '"';
        stat.execute("CREATE TABLE t (a int, b int) ENGINE " + engine);
        ColumnarTable t = ColumnarTableEngine.getLastCreated();
        stat.execute("CREATE INDEX T_IDX_A ON t(a)");
        stat.execute("CREATE INDEX T_IDX_B ON t(b)");
        setBatchSize(t, 3);
        for (int i = 0; i < 20; i++) {
            stat.execute("insert into t values (" + i + "," + (i + 10) + ")");
        }
        stat.execute("CREATE TABLE u (a int, b int) ENGINE " + engine);
        ColumnarTable u = ColumnarTableEngine.getLastCreated();
        stat.execute("CREATE INDEX U_IDX_A ON u(a)");
        stat.execute("CREATE INDEX U_IDX_B ON u(b)");
        setBatchSize(u, 0);
        for (int i = 10; i < 25; i++) {
            stat.execute("insert into u values (" + i + "," + (i - 15)+ ")");
        }

	if (engine == "ColumnarTableEngine") {
        checkPlan(stat, "SELECT 1 FROM PUBLIC.T T1 /* PUBLIC.\"tableScan\" */ "
                + "INNER JOIN PUBLIC.T T2 /* batched:test PUBLIC.T_IDX_B: B = T1.A */ "
                + "ON 1=1 WHERE T1.A = T2.B");
        checkPlan(stat, "SELECT 1 FROM PUBLIC.T T1 /* PUBLIC.\"tableScan\" */ "
                + "INNER JOIN PUBLIC.T T2 /* batched:test PUBLIC.T_IDX_B: B = T1.A */ "
                + "ON 1=1 /* WHERE T1.A = T2.B */ "
                + "INNER JOIN PUBLIC.T T3 /* batched:test PUBLIC.T_IDX_B: B = T2.A */ "
                + "ON 1=1 WHERE (T2.A = T3.B) AND (T1.A = T2.B)");
        checkPlan(stat, "SELECT 1 FROM PUBLIC.T T1 /* PUBLIC.\"tableScan\" */ "
                + "INNER JOIN PUBLIC.U /* batched:fake PUBLIC.U_IDX_A: A = T1.A */ "
                + "ON 1=1 /* WHERE T1.A = U.A */ "
                + "INNER JOIN PUBLIC.T T2 /* batched:test PUBLIC.T_IDX_B: B = U.B */ "
                + "ON 1=1 WHERE (T1.A = U.A) AND (U.B = T2.B)");
        checkPlan(stat, "SELECT 1 FROM ( SELECT A FROM PUBLIC.T ) Z "
                + "/* SELECT A FROM PUBLIC.T /++ PUBLIC.T_IDX_A ++/ */ "
                + "INNER JOIN PUBLIC.T /* batched:test PUBLIC.T_IDX_B: B = Z.A */ "
                + "ON 1=1 WHERE Z.A = T.B");
        checkPlan(stat, "SELECT 1 FROM PUBLIC.T /* PUBLIC.T_IDX_B */ "
                + "INNER JOIN ( SELECT A FROM PUBLIC.T ) Z "
                + "/* batched:view SELECT A FROM PUBLIC.T "
                + "/++ batched:test PUBLIC.T_IDX_A: A IS ?1 ++/ "
                + "WHERE A IS ?1: A = T.B */ ON 1=1 WHERE Z.A = T.B");
        checkPlan(stat, "SELECT 1 FROM PUBLIC.T /* PUBLIC.T_IDX_A */ "
                + "INNER JOIN ( ((SELECT A FROM PUBLIC.T) UNION ALL (SELECT B FROM PUBLIC.U)) "
                + "UNION ALL (SELECT B FROM PUBLIC.T) ) Z /* batched:view "
                + "((SELECT A FROM PUBLIC.T /++ batched:test PUBLIC.T_IDX_A: A IS ?1 ++/ "
                + "WHERE A IS ?1) "
                + "UNION ALL "
                + "(SELECT B FROM PUBLIC.U /++ PUBLIC.U_IDX_B: B IS ?1 ++/ WHERE B IS ?1)) "
                + "UNION ALL "
                + "(SELECT B FROM PUBLIC.T /++ batched:test PUBLIC.T_IDX_B: B IS ?1 ++/ "
                + "WHERE B IS ?1): A = T.A */ ON 1=1 WHERE Z.A = T.A");
        checkPlan(stat, "SELECT 1 FROM PUBLIC.T /* PUBLIC.T_IDX_A */ "
                + "INNER JOIN ( SELECT U.A FROM PUBLIC.U INNER JOIN PUBLIC.T ON 1=1 "
                + "WHERE U.B = T.B ) Z "
                + "/* batched:view SELECT U.A FROM PUBLIC.U "
                + "/++ batched:fake PUBLIC.U_IDX_A: A IS ?1 ++/ "
                + "/++ WHERE U.A IS ?1 ++/ INNER JOIN PUBLIC.T "
                + "/++ batched:test PUBLIC.T_IDX_B: B = U.B ++/ "
                + "ON 1=1 WHERE (U.A IS ?1) AND (U.B = T.B): A = T.A */ ON 1=1 WHERE Z.A = T.A");
        checkPlan(stat, "SELECT 1 FROM PUBLIC.T /* PUBLIC.T_IDX_A */ "
                + "INNER JOIN ( SELECT A FROM PUBLIC.U ) Z /* SELECT A FROM PUBLIC.U "
                + "/++ PUBLIC.U_IDX_A: A IS ?1 ++/ WHERE A IS ?1: A = T.A */ "
                + "ON 1=1 WHERE T.A = Z.A");
        checkPlan(stat, "SELECT 1 FROM "
                + "( SELECT U.A FROM PUBLIC.U INNER JOIN PUBLIC.T ON 1=1 WHERE U.B = T.B ) Z "
                + "/* SELECT U.A FROM PUBLIC.U /++ PUBLIC.\"tableScan\" ++/ "
                + "INNER JOIN PUBLIC.T /++ batched:test PUBLIC.T_IDX_B: B = U.B ++/ "
                + "ON 1=1 WHERE U.B = T.B */ "
                + "INNER JOIN PUBLIC.T /* batched:test PUBLIC.T_IDX_A: A = Z.A */ ON 1=1 "
                + "WHERE T.A = Z.A");
        checkPlan(stat, "SELECT 1 FROM "
                + "( SELECT U.A FROM PUBLIC.T INNER JOIN PUBLIC.U ON 1=1 WHERE T.B = U.B ) Z "
                + "/* SELECT U.A FROM PUBLIC.T /++ PUBLIC.T_IDX_B ++/ "
                + "INNER JOIN PUBLIC.U /++ PUBLIC.U_IDX_B: B = T.B ++/ "
                + "ON 1=1 WHERE T.B = U.B */ INNER JOIN PUBLIC.T "
                + "/* batched:test PUBLIC.T_IDX_A: A = Z.A */ "
                + "ON 1=1 WHERE Z.A = T.A");
        checkPlan(stat, "SELECT 1 FROM ( (SELECT A FROM PUBLIC.T) UNION "
                + "(SELECT A FROM PUBLIC.U) ) Z "
                + "/* (SELECT A FROM PUBLIC.T /++ PUBLIC.T_IDX_A ++/) "
                + "UNION "
                + "(SELECT A FROM PUBLIC.U /++ PUBLIC.U_IDX_A ++/) */ "
                + "INNER JOIN PUBLIC.T /* batched:test PUBLIC.T_IDX_A: A = Z.A */ ON 1=1 "
                + "WHERE Z.A = T.A");
        checkPlan(stat, "SELECT 1 FROM PUBLIC.U /* PUBLIC.U_IDX_B */ "
                + "INNER JOIN ( (SELECT A, B FROM PUBLIC.T) UNION (SELECT B, A FROM PUBLIC.U) ) Z "
                + "/* batched:view (SELECT A, B FROM PUBLIC.T "
                + "/++ batched:test PUBLIC.T_IDX_B: B IS ?1 ++/ "
                + "WHERE B IS ?1) UNION (SELECT B, A FROM PUBLIC.U "
                + "/++ PUBLIC.U_IDX_A: A IS ?1 ++/ "
                + "WHERE A IS ?1): B = U.B */ ON 1=1 /* WHERE U.B = Z.B */ "
                + "INNER JOIN PUBLIC.T /* batched:test PUBLIC.T_IDX_A: A = Z.A */ ON 1=1 "
                + "WHERE (U.B = Z.B) AND (Z.A = T.A)");
        checkPlan(stat, "SELECT 1 FROM PUBLIC.U /* PUBLIC.U_IDX_A */ "
                + "INNER JOIN ( SELECT A, B FROM PUBLIC.U ) Z "
                + "/* batched:fake SELECT A, B FROM PUBLIC.U /++ PUBLIC.U_IDX_A: A IS ?1 ++/ "
                + "WHERE A IS ?1: A = U.A */ ON 1=1 /* WHERE U.A = Z.A */ "
                + "INNER JOIN PUBLIC.T /* batched:test PUBLIC.T_IDX_B: B = Z.B */ "
                + "ON 1=1 WHERE (U.A = Z.A) AND (Z.B = T.B)");
}

        // t: a = [ 0..20), b = [10..30)
        // u: a = [10..25), b = [-5..10)
        checkBatchedQueryResult(stat, 10,
                "select t.a from t, (select t.b from u, t where u.a = t.a) z " +
                "where t.b = z.b");
        checkBatchedQueryResult(stat, 5,
                "select t.a from (select t1.b from t t1, t t2 where t1.a = t2.b) z, t " +
                "where t.b = z.b + 5");
        checkBatchedQueryResult(stat, 1,
                "select t.a from (select u.b from u, t t2 where u.a = t2.b) z, t " +
                "where t.b = z.b + 1");
        checkBatchedQueryResult(stat, 15,
                "select t.a from (select u.b from u, t t2 where u.a = t2.b) z " +
                "left join t on t.b = z.b");
        checkBatchedQueryResult(stat, 15,
                "select t.a from (select t1.b from t t1 left join t t2 on t1.a = t2.b) z, t "
                + "where t.b = z.b + 5");
        checkBatchedQueryResult(stat, 1,
                "select t.a from t,(select 5 as b from t union select 10 from u) z "
                + "where t.b = z.b");
        checkBatchedQueryResult(stat, 15, "select t.a from u,(select 5 as b, a from t "
                + "union select 10, a from u) z, t where t.b = z.b and z.a = u.a");

        stat.execute("DROP TABLE T");
        stat.execute("DROP TABLE U");
    }

    private void checkBatchedQueryResult(Statement stat, int size, String sql)
            throws SQLException {
        setBatchingEnabled(stat, false);
        List<List<Object>> expected = query(stat, sql);
        assertEquals(size, expected.size());
        setBatchingEnabled(stat, true);
        List<List<Object>> actual = query(stat, sql);
        if (!expected.equals(actual)) {
            fail("\n" + "expected: " + expected + "\n" + "actual:   " + actual);
        }
    }

    private void doTestBatchedJoin(Statement stat, int... batchSizes) throws SQLException {
        ArrayList<ColumnarTable> tables = New.arrayList(batchSizes.length);

        for (int i = 0; i < batchSizes.length; i++) {
            stat.executeUpdate("DROP TABLE IF EXISTS T" + i);
            stat.executeUpdate("CREATE TABLE T" + i + "(A INT, B INT) ENGINE \"" +
                    ColumnarTableEngine.class.getName() + "\"");
            tables.add(ColumnarTableEngine.getLastCreated());

            stat.executeUpdate("CREATE INDEX IDX_B ON T" + i + "(B)");
            stat.executeUpdate("CREATE INDEX IDX_A ON T" + i + "(A)");

            PreparedStatement insert = stat.getConnection().prepareStatement(
                    "INSERT INTO T"+ i + " VALUES (?,?)");

            for (int j = i, size = i + 10; j < size; j++) {
                insert.setInt(1, j);
                insert.setInt(2, j);
                insert.executeUpdate();
            }

            for (ColumnarTable table : tables) {
                assertEquals(10, table.getRowCount(null));
            }
        }

        int[] zeroBatchSizes = new int[batchSizes.length];
        int tests = 1 << (batchSizes.length * 4);

        for (int test = 0; test < tests; test++) {
            String query = generateQuery(test, batchSizes.length);

            // System.out.println(Arrays.toString(batchSizes) +
            //    ": " + test + " -> " + query);

            setBatchSize(tables, batchSizes);
            List<List<Object>> res1 = query(stat, query);

            setBatchSize(tables, zeroBatchSizes);
            List<List<Object>> res2 = query(stat, query);

            // System.out.println(res1 + " " + res2);

            if (!res2.equals(res1)) {
                System.err.println(Arrays.toString(batchSizes) + ": " + res1 + " " + res2);
                System.err.println("Test " + test);
                System.err.println(query);
                for (ColumnarTable table : tables) {
                    System.err.println(table.getName() + " = " +
                            query(stat, "select * from " + table.getName()));
                }
                fail();
            }
        }
        for (int i = 0; i < batchSizes.length; i++) {
            stat.executeUpdate("DROP TABLE IF EXISTS T" + i);
        }
    }

    /**
     * A static assertion method.
     *
     * @param condition the condition
     * @param message the error message
     */
    static void assert0(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static void setBatchSize(ArrayList<ColumnarTable> tables, int... batchSizes) {
        for (int i = 0; i < batchSizes.length; i++) {
            int batchSize = batchSizes[i];
            setBatchSize(tables.get(i), batchSize);
        }
    }

    private static void setBatchSize(ColumnarTable t, int batchSize) {
        if (t.getIndexes() == null) {
            int preferredBatchSize = batchSize;
        } else {
            for (Index idx : t.getIndexes()) {
                int preferredBatchSize = batchSize;
            }
        }
    }

    private static String generateQuery(int t, int tables) {
        final int withLeft = 1;
        final int withFalse = 2;
        final int withWhere = 4;
        final int withOnIsNull = 8;

        StringBuilder b = new StringBuilder();
        b.append("select count(*) from ");

        StringBuilder where = new StringBuilder();

        for (int i = 0; i < tables; i++) {
            if (i != 0) {
                if ((t & withLeft) != 0) {
                    b.append(" left ");
                }
                b.append(" join ");
            }
            b.append("\nT").append(i).append(' ');
            if (i != 0) {
                boolean even = (i & 1) == 0;
                if ((t & withOnIsNull) != 0) {
                    b.append(" on T").append(i - 1).append(even ? ".B" : ".A").append(" is null");
                } else if ((t & withFalse) != 0) {
                    b.append(" on false ");
                } else {
                    b.append(" on T").append(i - 1).append(even ? ".B = " : ".A = ");
                    b.append("T").append(i).append(even ? ".B " : ".A ");
                }
            }
            if ((t & withWhere) != 0) {
                if (where.length() != 0) {
                    where.append(" and ");
                }
                where.append(" T").append(i).append(".A > 5");
            }
            t >>>= 4;
        }
        if (where.length() != 0) {
            b.append("\n" + "where ").append(where);
        }

        return b.toString();
    }

    private void checkResultsNoOrder(Statement stat, int size, String query1, String query2)
            throws SQLException {
        List<List<Object>> res1 = query(stat, query1);
        List<List<Object>> res2 = query(stat, query2);
        if (size != res1.size() || size != res2.size()) {
            fail("Wrong size: \n" + res1 + "\n" + res2);
        }
        if (size == 0) {
            return;
        }
        int[] cols = new int[res1.get(0).size()];
        for (int i = 0; i < cols.length; i++) {
            cols[i] = i;
        }
        Comparator<List<Object>> comp = new RowComparator(cols);
        Collections.sort(res1, comp);
        Collections.sort(res2, comp);
        assertTrue("Wrong data: \n" + res1 + "\n" + res2, res1.equals(res2));
    }

    private void checkResults(int size, List<List<Object>> dataSet,
            Statement stat, String query, RowFilter filter, RowComparator sort)
            throws SQLException {
        List<List<Object>> res1 = query(stat, query);
        List<List<Object>> res2 = query(dataSet, filter, sort);

        assertTrue("Wrong size: " + size + " \n" + res1 + "\n" + res2,
                res1.size() == size && res2.size() == size);
        assertTrue(filter != null || sort  != null);

        for (int i = 0; i < res1.size(); i++) {
            List<Object> row1 = res1.get(i);
            List<Object> row2 = res2.get(i);

            assertTrue("Filter failed on row " + i + " of \n" + res1 + "\n" + res2,
                    filter == null || filter.accept(row1));
            assertTrue("Sort failed on row "  + i + " of \n" + res1 + "\n" + res2,
                    sort == null || sort.compare(row1, row2) == 0);
        }
    }

    private static List<List<Object>> query(List<List<Object>> dataSet,
            RowFilter filter, RowComparator sort) {
        List<List<Object>> res = New.arrayList();
        if (filter == null) {
            res.addAll(dataSet);
        } else {
            for (List<Object> row : dataSet) {
                if (filter.accept(row)) {
                    res.add(row);
                }
            }
        }
        if (sort != null) {
            Collections.sort(res, sort);
        }
        return res;
    }

    private static List<List<Object>> query(Statement stat, String query) throws SQLException {
        ResultSet rs = stat.executeQuery(query);
        int cols = rs.getMetaData().getColumnCount();
        List<List<Object>> list = New.arrayList();
        while (rs.next()) {
            List<Object> row = New.arrayList(cols);
            for (int i = 1; i <= cols; i++) {
                row.add(rs.getObject(i));
            }
            list.add(row);
        }
        rs.close();
        return list;
    }

    private void checkPlan(Statement stat, String query, String index)
            throws SQLException {
        String plan = query(stat, "EXPLAIN " + query).get(0).get(0).toString();
        assertTrue("Index '" + index + "' is not used in query plan: " + plan,
                plan.contains(index));
    }


}
