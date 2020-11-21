package org.oclc.hbase.nowaltool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Puts a row to a table with durability turned-off!
 */
public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException {
        //args:  table row fam:col value
        if (args.length < 4) {
            throw new IllegalArgumentException("args: table row fam:col value");
        }
        LOG.info("sending SKIP_WAL put to {}, row={}", args[0], args[1]);
        String lastValue = HBaseTool.putToTable(args[0], args[1], args[2], args[3]);
        LOG.info("cell value after put={}", lastValue);

    }

}
