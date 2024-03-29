BEGIN
	EXECUTE IMMEDIATE 'DROP TABLE {Schema}.UTIL_SYS_ETL_TASK_QUEUE_HIST';
EXCEPTION
	WHEN OTHERS THEN
		NULL;
END;
/

-- Create table
CREATE TABLE {Schema}.UTIL_SYS_ETL_TASK_QUEUE_HIST
(
    TASK_ID             NUMBER(10)      NOT NULL,
    BATCH_ID            NUMBER(10)      NOT NULL,
    STEP_PLAN           VARCHAR2(2000)  NOT NULL,

    EXTRACT_TYPE        VARCHAR2(32)    NOT NULL,
    EXTRACT_SOURCE      VARCHAR2(1000)  NOT NULL,
    EXTRACT_COMMAND     VARCHAR2(4000)  NOT NULL,
    EXTRACT_PARAMS      VARCHAR2(4000)  NOT NULL,
    EXTRACT_TIMEOUT_SEC NUMBER(5)       NOT NULL,

    RESULT_SET          NUMBER(2)       NOT NULL,
    NAMING_CONVENTION   CHAR(1)         NOT NULL,

    LOAD_TYPE           VARCHAR2(32)    NOT NULL,
    FIELD_MAPPING       VARCHAR2(4000)  NOT NULL,
    MERGE_PARAMS        VARCHAR2(4000)  NOT NULL,
    LOAD_DESTINATION    VARCHAR2(1000)  NOT NULL,
    LOAD_COMMAND        VARCHAR2(4000)  NOT NULL,
    LOAD_TIMEOUT_SEC    NUMBER(5)       NOT NULL,

    TASK_COMMENT        VARCHAR2(4000),
    CLIENT_ACCOUNT      VARCHAR2(32)    NOT NULL,
    ENTRY_TIME          DATE            NOT NULL,
    COMPLETED_TIME      DATE            NOT NULL,
    RUNTIME_ERROR       VARCHAR2(4000),

    CONSTRAINT PK_UTIL_SYS_ETL_TASK_QUEUE_HIS PRIMARY KEY (TASK_ID),
    CONSTRAINT CK_UTIL_SYS_ETL_TASK_HIST_EP CHECK (EXTRACT_PARAMS IS JSON),
    CONSTRAINT CK_UTIL_SYS_ETL_TASK_HIST_FM CHECK (FIELD_MAPPING IS JSON),
    CONSTRAINT CK_UTIL_SYS_ETL_TASK_HIST_MP CHECK (MERGE_PARAMS IS JSON)
)
ROW STORE COMPRESS ADVANCED NOLOGGING;

COMMENT ON TABLE {Schema}.UTIL_SYS_ETL_TASK_QUEUE_HIST IS 'Simple ETL Spooler task processing queue archived history';

CREATE INDEX {Schema}.IX_UTIL_SYS_ETL_TASK_QUEUE_HIS ON {Schema}.UTIL_SYS_ETL_TASK_QUEUE_HIST (BATCH_ID, STEP_PLAN, TASK_ID);
