BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE {Schema}.UTIL_SYS_ETL_BATCH_STATUS_HIST';
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

-- Create table
CREATE TABLE {Schema}.UTIL_SYS_ETL_BATCH_STATUS_HIST
(
    BATCH_ID            NUMBER(10)      NOT NULL,
    BATCH_COMMENT       VARCHAR2(4000),
    SCHEDULED_TIME      DATE,
    BATCH_STATUS        VARCHAR2(30)    NOT NULL,
    TASKS_COUNT         NUMBER(5)       DEFAULT 0           NOT NULL,
    SERIES_COUNT        NUMBER(5)       DEFAULT 0           NOT NULL,
    ENTRY_TIME          DATE            NOT NULL,
    TRIGGERED_TIME      DATE,
    COMPLETED_TIME      DATE,

    CONSTRAINT PK_UTIL_SYS_ETL_BATCH_STATUS_H PRIMARY KEY (BATCH_ID)
)
ROW STORE COMPRESS ADVANCED NOLOGGING;

COMMENT ON TABLE {Schema}.UTIL_SYS_ETL_BATCH_STATUS_HIST IS 'Simple ETL Spooler batch status archived history';
