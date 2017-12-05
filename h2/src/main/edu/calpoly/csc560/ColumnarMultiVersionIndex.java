package edu.calpoly.csc560;

import java.util.HashSet;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.schema.SchemaObject;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.index.*;

/**
 * Simple columnar wrapper around a generic H2 index. This class is a placeholder for
 * unfinished MVCC implementation work.
 *
 */
public class ColumnarMultiVersionIndex extends BaseIndex {

    private final ColumnarTable table;
    private final Index index;
    
    public ColumnarMultiVersionIndex(Index i, ColumnarTable table) {
	this.table = table;
	this.index = i;
    }

    /**
     * Estimate the cost to search for rows given the search mask.
     * There is one element per column in the search mask.
     * For possible search masks, see IndexCondition.
     *
     * @param session the session
     * @param masks per-column comparison bit masks, null means 'always false',
     *              see constants in IndexCondition
     * @param filters all joined table filters
     * @param filter the current table filter index
     * @param sortOrder the sort order
     * @param allColumnsSet the set of all columns
     * @return the estimated cost
     */
    @Override
    public double getCost(Session session, int[] masks, TableFilter[] filters, int filter,
			  SortOrder sortOrder, HashSet<Column> allColumnsSet) {
	return index.getCost(session, masks, filters, filter, sortOrder, allColumnsSet);
    }

    /**
     * Get the message to show in a EXPLAIN statement.
     *
     * @return the plan
     */
    @Override
    public String getPlanSQL() {
	return index.getPlanSQL();
    }

    /**
     * Close this index.
     *
     * @param session the session used to write data
     */
    @Override
    public void close(Session session) {
	index.close(session);
    }

    /**
     * Add a row to the index.
     *
     * @param session the session to use
     * @param row the row to add
     */
    @Override
    public void add(Session session, Row row) {
	index.add(session, row);
    }

    /**
     * Remove a row from the index.
     *
     * @param session the session
     * @param row the row
     */
    @Override
    public void remove(Session session, Row row) {
	index.remove(session, row);
    }

    /**
     * Find a row or a list of rows and create a cursor to iterate over the
     * result.
     *
     * @param session the session
     * @param first the first row, or null for no limit
     * @param last the last row, or null for no limit
     * @return the cursor to iterate over the results
     */
    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
	return index.find(session, first, last);
    }

    /**
     * Find a row or a list of rows and create a cursor to iterate over the
     * result.
     *
     * @param filter the table filter (which possibly knows about additional
     *            conditions)
     * @param first the first row, or null for no limit
     * @param last the last row, or null for no limit
     * @return the cursor to iterate over the results
     */
    @Override
    public Cursor find(TableFilter filter, SearchRow first, SearchRow last) {
	return index.find(filter, first, last);
    }
    
    @Override
    public void checkRename() {
	// always permitted
    }

    @Override
    public long getDiskSpaceUsed() {
	return 0;  // in-memory
    }

    @Override
    public long getRowCount(Session session) {
	return index.getRowCount(session);
    }
    
    @Override
    public long getRowCountApproximation() {
	return this.getRowCount(null);
    }

    @Override
    public boolean needRebuild() {
	return index.needRebuild();
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
	return index.findFirstOrLast(session, first);
    }

    @Override
    public boolean canGetFirstOrLast() {
	return index.canGetFirstOrLast();
    }

    @Override
    public void truncate(Session session) {
	index.truncate(session);
    }

    @Override
    public void remove(Session session) {
	index.remove(session);
    }

    
}
