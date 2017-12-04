package org.h2.column;

import java.util.HashSet;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.RegularTable;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.index.Cursor;
import org.h2.index.SingleRowCursor;
import org.h2.index.BaseIndex;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.IndexCondition;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Hash index implementation for a table stored in columnar format. Uses a simple
 * extensible hashing algorithm, based on Java Collections containers.
 *
 */
public class ColumnarHashIndex extends BaseIndex {
    private final ColumnarTable table;
    private final int idxColId; // array index of column on which this index is defined

    private long rowCount = 0;
    private Map<Value,List<Long>> hashTable = new HashMap<Value,List<Long>>();
    
    public ColumnarHashIndex(ColumnarTable table, int id, String indexName,
		     IndexColumn[] columns, IndexType indexType) {
	this.table = table;
	this.idxColId = columns[0].column.getColumnId();
	clear();
    }

    private void clear() {
	hashTable.clear();
	rowCount = 0;
    }

    @Override
    public void truncate(Session session) {
	clear();
    }

    @Override
    public void add(Session session, Row row) {
	Value v = row.getValue(idxColId);
	List<Long> hashBin = null;
	if (hashTable.containsKey(v)) {
	    hashBin = hashTable.get(v);
	} else {
	    hashBin = new ArrayList<Long>();
	    hashTable.put(v, hashBin);
	}
	hashBin.add(row.getKey());
	rowCount++;
    }

    @Override
    public void remove(Session session, Row row) {
	Value v = row.getValue(idxColId);
	if (hashTable.containsKey(v)) {
	    List<Long> hashBin = hashTable.get(v);
	    if (hashBin.contains(row.getKey())) {
		hashBin.remove(row.getKey());
		rowCount--;
	    }
	    if (hashBin.size() == 0) {
		hashTable.remove(v);
	    }
	}
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
	if (first == null || last == null) {
	    throw DbException.throwInternalError(first + " " + last);
	}
	
	Value v = first.getValue(idxColId);
	v = v.convertTo(table.getColumn(idxColId).getType());  // convert int->long if needed
	Row result;
	Long rowKey = hashTable.get(v).get(0);
	if (rowKey == null) {
	    result = null;
	} else {
	    result = table.getRow(session, rowKey.intValue());
	}
	return new SingleRowCursor(result);
    }
    
    @Override
    public long getRowCount(Session session) {
	return getRowCountApproximation();
    }

    @Override
    public long getRowCountApproximation() {
	return rowCount;
    }

    @Override
    public long getDiskSpaceUsed() {
	return 0;  // in-memory
    }

    @Override
    public void close(Session session) {
	clear();
    }
    
    @Override
    public void remove(Session session) {
	clear();
    }
    
    @Override
    public double getCost(Session session, int[] masks,
			  TableFilter[] filters, int filter, SortOrder sortOrder,
			  HashSet<Column> allColumnsSet) {
	for (Column column : columns) {
	    int index = column.getColumnId();
	    int mask = masks[index];
	    if ((mask & IndexCondition.EQUALITY) != IndexCondition.EQUALITY) {
		return Long.MAX_VALUE;
	    }
	}
	return 2;
    }

    @Override
    public void checkRename() {
	// always permitted
    }

    @Override
    public boolean needRebuild() {
	return true;
    }
    
    @Override
    public boolean canGetFirstOrLast() {
	return false;  // not an ordered index
    }
    
    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
	throw DbException.getUnsupportedException("ColumnarHash");
    }
    
    @Override
    public boolean canScan() {
	return false;
    }
    
}
