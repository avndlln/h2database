package org.h2.column.mvstore;

import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.command.ddl.CreateTableData;

import org.h2.mvstore.db.MVTable;
import org.h2.mvstore.db.MVTableEngine;

import java.util.logging.Logger;

public class ColumnarTable extends MVTable {

    Logger log = Logger.getLogger(ColumnarTable.class.getName());

    public ColumnarTable(CreateTableData data, MVTableEngine.Store store) {
	super(data, store);
	log.info("ColumnarTable::constructor()");
    }

    
    /*
	// TODO: create a custom "ColumnarIndex" object here...
	primaryIndex = new MVPrimaryIndex(session.getDatabase(), this, getId(),
					  IndexColumn.wrap(getColumns()), IndexType.createScan(true));
	indexes.add(primaryIndex);
	*/

    
    /*
    @Override
    public void addRow(Session session, Row row) {
	log.info("addRow - " + session + ", " + row + ") - begin");
	System.err.println("[err] addRow - " + session + ", " + row + ") - begin");
	super.addRow(session, row);
    }
    */
    
}
