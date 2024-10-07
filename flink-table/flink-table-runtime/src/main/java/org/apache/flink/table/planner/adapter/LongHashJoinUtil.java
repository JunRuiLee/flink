package org.apache.flink.table.planner.adapter;

import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.operators.join.HashJoinType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import java.util.Arrays;

public class LongHashJoinUtil {

    public static boolean support(HashJoinType joinType, RowType keyType, boolean[] filterNulls) {
        boolean isJoinTypeSupported = joinType == HashJoinType.INNER ||
                joinType == HashJoinType.SEMI ||
                joinType == HashJoinType.ANTI ||
                joinType == HashJoinType.PROBE_OUTER;
        boolean areAllFilterNullsTrue = true;
        for (int i = 0; i < filterNulls.length; ++i) {
            if (!filterNulls[i]) {
                areAllFilterNullsTrue = false;
            }
        }
        boolean isSingleKeyField = keyType.getFieldCount() == 1;

        if (!isJoinTypeSupported || !areAllFilterNullsTrue || !isSingleKeyField) {
            return false;
        }

        switch (keyType.getTypeAt(0).getTypeRoot()) {
            case BIGINT:
            case INTEGER:
            case SMALLINT:
            case TINYINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return true;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) keyType.getTypeAt(0);
                return TimestampData.isCompact(timestampType.getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType lzTs = (LocalZonedTimestampType) keyType.getTypeAt(0);
                return TimestampData.isCompact(lzTs.getPrecision());
            default:
                return false;
        }

    }
}
