package org.h2.column;

import org.h2.engine.Session;
import org.h2.command.ddl.CreateTableData;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.table.IndexColumn;
import org.h2.table.RegularTable;
import org.h2.index.Index;
import org.h2.result.Row;
import java.util.logging.Logger;

/**
 * A in-memory table that store data in column-oriented format, rather than the H2 default
 * tuple-oriented storage. Actual data storage takes place in the ColumnarIndex class,
 * this is simply a wrapper class.
 * 
 */
public class ColumnarTable extends RegularTable {
    Logger log = Logger.getLogger(ColumnarTable.class.getName());

    private final String tableName;
    private boolean debugOn = false;
    
    public ColumnarTable(CreateTableData data) {
        super(data);

	tableName = data.tableName;

	// enable 560-specific debug logging, if environment variable is set
	if ("on".equalsIgnoreCase("H2_560_DEBUG")) {
	    debugOn = true;
	}
	
	log.info("ColumnarTable() - table name: " + tableName + ", database: " + database);

	// activate columnar index as the "scan index" for this table
	Index scanIndex = new ColumnarIndex(this, data.id,
					    IndexColumn.wrap(getColumns()), IndexType.createScan(data.persistData));
	super.setScanIndex(scanIndex);
    }

    @Override
    public Index addIndex(Session session, String indexName, int indexId,
			  IndexColumn[] cols, IndexType indexType, boolean create,
			  String indexComment) {
	log.info(tableName + "::addIndex() - index name: " + indexName + ", isPrimaryKey? " + indexType.isPrimaryKey());
	return super.addIndex(session, indexName, indexId, cols, indexType, create, indexComment);
    }

    protected String getTableName() {
	return this.tableName;
    }

    @Override
    public void addRow(Session session, Row row) {
	if (debugOn) {
	    log.info(tableName + "::addRow() - " + row);
	}
	super.addRow(session, row);
    }

    @Override
    public void removeRow(Session session, Row row) {
	if (debugOn) {
	    log.info(tableName + "::removeRow() - " + row);
	}
	super.removeRow(session, row);
    }
    
}
