package org.h2.column;

import org.h2.table.IndexColumn;
import org.h2.index.IndexType;
import org.h2.result.Row;
import org.h2.result.RowImpl;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.value.Value;
import org.h2.index.BaseIndex;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.LinkedHashMap;
import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.engine.UndoLogRecord;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.RegularTable;
import org.h2.table.TableFilter;
import org.h2.util.New;

import java.util.logging.Logger;

/**
 * Columnar "scan" index. Although it is termed an index, in H2, a scan index is
 * a simple iterator and cannot be used for random lookup of rows.  Every table in H2
 * has a single scan index, which controls the physical storage of records.
 * 
 * @author avondoll
 */
public class ColumnarIndex extends BaseIndex {
    Logger log = Logger.getLogger(ColumnarTable.class.getName());

    private final ColumnarTable tableData;
    private int rowCountDiff;
    private final HashMap<Integer, Integer> sessionRowCount;
    
    private long rowCount;
    private HashSet<Row> delta;  // used for MVCC support

    private long nextKey = 0;
    private ArrayList<Row> rows = New.arrayList();  // row-wise storage
    private String[] columnNames = null;
    private Map<String,List<Value>> columnStore = new LinkedHashMap<String,List<Value>>();  // columnar storage

    private Set<Long> tombstoneKeys = new HashSet<Long>();
    public static final Row TOMBSTONE = new RowImpl(null, 0);  // singleton tombstone marker
    static { TOMBSTONE.setKey(-9999); }

    boolean debugOn = false; 
    
    public ColumnarIndex(ColumnarTable table, int id, IndexColumn[] columns,
			 IndexType indexType) {

	if ("on".equalsIgnoreCase(System.getenv("H2_DEBUG"))) {
	    debugOn = true;
	}
	
	log.info("ColumnarIndex() - table: " + table);

	// store column names, used as keys for the columnar store
	Column[] cols = table.getColumns();
	columnNames = new String[cols.length];
	for (int i = 0; i < cols.length; i++) {
	    columnNames[i] = cols[i].getName();
	    columnStore.put(columnNames[i], new ArrayList<Value>());  // initialize columnStore
	}
	
        initBaseIndex(table, id, table.getName() + "_DATA", columns, indexType);
        if (database.isMultiVersion()) {
            sessionRowCount = New.hashMap();
        } else {
            sessionRowCount = null;
        }
        tableData = table;
    }

    @Override
    public void remove(Session session) {
        truncate(session);
    }

    @Override
    public void truncate(Session session) {

	log.info("ColumnarIndex() - truncate()");
	nextKey = 0;
	columnStore = new LinkedHashMap<String,List<Value>>();
	tombstoneKeys = new HashSet<Long>();
	
        if (tableData.getContainsLargeObject() && tableData.isPersistData()) {
            database.getLobStorage().removeAllForTable(table.getId());
        }
        tableData.setRowCount(0);
        rowCount = 0;
        rowCountDiff = 0;
        if (database.isMultiVersion()) {
            sessionRowCount.clear();
        }
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public Row getRow(Session session, long key) {
	if (key >= nextKey) {
	    return null;
	} else if (tombstoneKeys.contains(key)) {
	    return TOMBSTONE;  // signal to cursor to skip this row
	}
	
	// construct a row from our columnar values
	Value[] data = new Value[columnNames.length];
	if (debugOn) { log.info("getRow() - key: " + key); }
	for (int i = 0; i < columnNames.length; i++) {
	    data[i] = columnStore.get(columnNames[i]).get((int) key);
	    if (debugOn) { log.info("getRow() - column: " + columnNames[i] + " / " + i + ", val: " + data[i]); }
	}

	Row r = new RowImpl(data, 0);  // 0 => in-memory
	r.setKey(key);
	return r;
    }

    @Override
    public void add(Session session, Row row) {

	row.setKey(nextKey);
	Value[] vals = row.getValueList();
	for (int i = 0; i < columnNames.length; i++) {
	    columnStore.get(columnNames[i]).add(vals[i]);
	}
	nextKey++;

	// debug output, print the full column store after adding this row
	StringBuffer sb = new StringBuffer("columnStore: [\n");
	columnStore.forEach((k,v) -> sb.append("  " + k + ": " + v + "\n"));
	sb.append("]");
	if (debugOn) { log.info(sb.toString()); }
	
        row.setDeleted(false);
	
        if (database.isMultiVersion()) {
            if (delta == null) {
                delta = New.hashSet();
            }
            boolean wasDeleted = delta.remove(row);
            if (!wasDeleted) {
                delta.add(row);
            }
            incrementRowCount(session.getId(), 1);
        }
        rowCount++;
    }

    @Override
    public void commit(int operation, Row row) {
        if (database.isMultiVersion()) {
            if (delta != null) {
                delta.remove(row);
            }
            incrementRowCount(row.getSessionId(),
                    operation == UndoLogRecord.DELETE ? 1 : -1);
        }
    }

    private void incrementRowCount(int sessionId, int count) {
        if (database.isMultiVersion()) {
            Integer id = sessionId;
            Integer c = sessionRowCount.get(id);
            int current = c == null ? 0 : c.intValue();
            sessionRowCount.put(id, current + count);
            rowCountDiff += count;
        }
    }

    @Override
    public void remove(Session session, Row row) {

	long key = row.getKey();
	tombstoneKeys.add(key);
	for (int i = 0; i < columnNames.length; i++) {
	    columnStore.get(columnNames[i]).set((int) key, null);
	}
	
        if (database.isMultiVersion()) {
            // if storage is null, the delete flag is not yet set
            row.setDeleted(true);
            if (delta == null) {
                delta = New.hashSet();
            }
            boolean wasAdded = delta.remove(row);
            if (!wasAdded) {
                delta.add(row);
            }
            incrementRowCount(session.getId(), -1);
        }
        rowCount--;
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        return new ColumnarCursor(session, this, database.isMultiVersion());
    }

    @Override
    public double getCost(Session session, int[] masks,
			  TableFilter[] filters, int filter, SortOrder sortOrder,
			  HashSet<Column> allColumnsSet) {
        return tableData.getRowCountApproximation() + Constants.COST_ROW_OFFSET;
    }

    @Override
    public long getRowCount(Session session) {
        if (database.isMultiVersion()) {
            Integer i = sessionRowCount.get(session.getId());
            long count = i == null ? 0 : i.intValue();
            count += rowCount;
            count -= rowCountDiff;
            return count;
        }
        return rowCount;
    }

    /**
     * Get the next row that is stored after the provided row.
     *
     * @param row the current row or null to start the scan
     * @return the next row or null if there are no more rows
     */
    Row getNextRow(Row row) {
	/*
	long key = row == null ? -1 : row.getKey();
	log.info("getNextRow(" + row + ") - key: " + key);
	return getRow(null, key + 1);
	*/	

        long key = row == null ? -1 : row.getKey();
        while (true) {
            key++;
            row = getRow(null, key);
	    if (row == null) { return row; }
            if (!row.isEmpty() && row != TOMBSTONE) {
                return row;
            }
        }
    }

    @Override
    public int getColumnIndex(Column col) {
        // the scan index cannot use any columns
        return -1;
    }

    @Override
    public boolean isFirstColumn(Column column) {
        return false;
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("SCAN");
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public boolean canGetFirstOrLast() {
        return false;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        throw DbException.getUnsupportedException("SCAN");
    }

    Iterator<Row> getDelta() {
        if (delta == null) {
            List<Row> e = Collections.emptyList();
            return e.iterator();
        }
        return delta.iterator();
    }

    @Override
    public long getRowCountApproximation() {
        return rowCount;  // exact count
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;  // this implementation supports in-memory usage only
    }

    @Override
    public String getPlanSQL() {
        return table.getSQL() + ".tableScan";
    }

    @Override
    public void close(Session session) {
        // nothing to do
    }
    
}
