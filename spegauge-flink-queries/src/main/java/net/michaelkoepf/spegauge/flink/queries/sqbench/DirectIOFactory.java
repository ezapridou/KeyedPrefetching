package net.michaelkoepf.spegauge.flink.queries.sqbench;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.state.rocksdb.ConfigurableRocksDBOptionsFactory;
import org.apache.flink.state.rocksdb.RocksDBOptionsFactory;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

import java.util.Collection;

public class DirectIOFactory implements ConfigurableRocksDBOptionsFactory {

    @Override
    public DBOptions createDBOptions(DBOptions opts, Collection<AutoCloseable> handlesToClose) {
        return opts
                .setUseDirectReads(true)
                .setUseDirectIoForFlushAndCompaction(true)
                .setAllowMmapReads(false)
                .setAllowMmapWrites(false);
                // Good practice with Direct I/O: give compaction its own readahead.
                //.setCompactionReadaheadSize(2 * 1024 * 1024)
    }

    @Override
    public ColumnFamilyOptions createColumnOptions(
            ColumnFamilyOptions current, Collection<AutoCloseable> handlesToClose) {

        // Reuse existing table config if it’s block-based; otherwise create one
        //BlockBasedTableConfig tbl;
        //if (current.tableFormatConfig() instanceof BlockBasedTableConfig) {
        //    tbl = (BlockBasedTableConfig) current.tableFormatConfig();
        //} else {
            //tbl = new BlockBasedTableConfig()

        //}

        //.setNoBlockCache(true)   // disable data-block cache entirely
        //.setBlockCache(null)
        //.setCacheIndexAndFilterBlocks(false)
        //.setPinL0FilterAndIndexBlocksInCache(false)
        //.setPinTopLevelIndexAndFilter(false);
        //.setCacheIndexAndFilterBlocksWithHighPriority(false);

        //current.setTableFormatConfig(tbl);
        return current;
    }

    @Override
    public RocksDBOptionsFactory configure(ReadableConfig configuration) {
        return this;
    }
}

