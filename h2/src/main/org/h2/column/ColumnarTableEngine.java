package org.h2.column;

import org.h2.engine.Database;
import org.h2.api.TableEngine;
import org.h2.table.TableBase;
import org.h2.command.ddl.CreateTableData;

import java.util.logging.Logger;

public class ColumnarTableEngine implements TableEngine {

    private Logger log = Logger.getLogger(ColumnarTableEngine.class.getName());

        @Override
	public TableBase createTable(CreateTableData data) {
	    log.info("createTable(" + data + ")");
	    return new ColumnarTable(data);
	}
    
}
