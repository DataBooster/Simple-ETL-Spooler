BEGIN
	EXECUTE IMMEDIATE 'DROP TABLE {Schema}.UTIL_SYS_ETL_TASK_QUEUE';
EXCEPTION
	WHEN OTHERS THEN
		NULL;
END;
/

-- Create table
CREATE TABLE {Schema}.UTIL_SYS_ETL_TASK_QUEUE
(
    TASK_ID             NUMBER(10)      NOT NULL,
    BATCH_ID            NUMBER(10)      NOT NULL,
    STEP_PLAN           VARCHAR2(2000)  DEFAULT ':'     NOT NULL,

    EXTRACT_TYPE        VARCHAR2(32)    NOT NULL,
    EXTRACT_SOURCE      VARCHAR2(1000)  NOT NULL,
    EXTRACT_COMMAND     VARCHAR2(4000)  NOT NULL,
    EXTRACT_PARAMS      VARCHAR2(4000)  DEFAULT '{}'    NOT NULL,
    EXTRACT_TIMEOUT_SEC NUMBER(5)       DEFAULT 0       NOT NULL,

    RESULT_SET          NUMBER(2)       DEFAULT 0       NOT NULL,
    NAMING_CONVENTION   CHAR(1)         DEFAULT 'N'     NOT NULL,

    LOAD_TYPE           VARCHAR2(32)    NOT NULL,
    FIELD_MAPPING       VARCHAR2(4000)  DEFAULT '{}'    NOT NULL,
    MERGE_PARAMS        VARCHAR2(4000)  DEFAULT '{}'    NOT NULL,
    LOAD_DESTINATION    VARCHAR2(1000)  NOT NULL,
    LOAD_COMMAND        VARCHAR2(4000)  NOT NULL,
    LOAD_TIMEOUT_SEC    NUMBER(5)       DEFAULT 0       NOT NULL,

    TASK_COMMENT        VARCHAR2(4000),
    CLIENT_ACCOUNT      VARCHAR2(32)    DEFAULT SYS_CONTEXT('USERENV', 'OS_USER')   NOT NULL,
    ENTRY_TIME          DATE            DEFAULT SYSDATE NOT NULL,
    COMPLETED_TIME      DATE,
    RUNTIME_ERROR       VARCHAR2(4000),

    CONSTRAINT PK_UTIL_SYS_ETL_TASK_QUEUE PRIMARY KEY (TASK_ID),
    CONSTRAINT CK_UTIL_SYS_ETL_TASK_QUEUE_ET CHECK (EXTRACT_TYPE IN ('SP', 'SQL', 'MDX', 'REST')),
    CONSTRAINT CK_UTIL_SYS_ETL_TASK_QUEUE_NC CHECK (NAMING_CONVENTION IN ('N', 'P', 'C')),
    CONSTRAINT CK_UTIL_SYS_ETL_TASK_QUEUE_LT CHECK (LOAD_TYPE IN ('SP')),
    CONSTRAINT CK_UTIL_SYS_ETL_TASK_ETIMEOUT CHECK (EXTRACT_TIMEOUT_SEC >= 60 OR EXTRACT_TIMEOUT_SEC <= 0),
    CONSTRAINT CK_UTIL_SYS_ETL_TASK_LTIMEOUT CHECK (LOAD_TIMEOUT_SEC >= 60 OR LOAD_TIMEOUT_SEC <= 0)
)
NOLOGGING;

COMMENT ON TABLE {Schema}.UTIL_SYS_ETL_TASK_QUEUE IS 'Simple ETL Spooler task processing queue';

CREATE INDEX {Schema}.IX_UTIL_SYS_ETL_TASK_QUEUE ON {Schema}.UTIL_SYS_ETL_TASK_QUEUE (BATCH_ID, STEP_PLAN, TASK_ID);

comment on column {Schema}.UTIL_SYS_ETL_TASK_QUEUE.batch_id
  is 'Multiple tasks may belong to the same batch.';
comment on column {Schema}.UTIL_SYS_ETL_TASK_QUEUE.step_plan
  is 'Will be used to indicate how to organize tasks in serial-parallel order throughout the batch';
comment on column {Schema}.UTIL_SYS_ETL_TASK_QUEUE.extract_type
  is 'The type of extraction source, currently supported: (''SP'', ''SQL'', ''MDX'', ''REST'')';
comment on column {Schema}.UTIL_SYS_ETL_TASK_QUEUE.extract_source
  is 'The database server name of ''SP''/''SQL'', or the full connection string of ''MDX'', or the URL of ''REST''.';
comment on column {Schema}.UTIL_SYS_ETL_TASK_QUEUE.extract_command
  is 'For ''SP'': The fully qualified name of the stored procedure; For ''SQL''/''MDX'': A complete dynamic query statement.';
comment on column {Schema}.UTIL_SYS_ETL_TASK_QUEUE.extract_params
  is 'Only for ''SP'': A valid JSON string containing all input parameters to be passed to the stored procedure.';
comment on column {Schema}.UTIL_SYS_ETL_TASK_QUEUE.RESULT_SET
  is 'Indicates which result-set (zero-based index) from the source needs to be loaded into the destination.';
comment on column {Schema}.UTIL_SYS_ETL_TASK_QUEUE.naming_convention
  is 'Naming convention for all columns of result-set. ''N'': None - as it was in source; ''p'': PascalCase; ''C'': CamelCase.';
comment on column {Schema}.UTIL_SYS_ETL_TASK_QUEUE.load_type
  is 'The type of load destination, currently supported: ''SP'' (Oracle only for now)';
comment on column {Schema}.UTIL_SYS_ETL_TASK_QUEUE.field_mapping
  is 'A valid JSON string specifying some special name mapping between source columns and destination input paramet{Schema}.';
comment on column {Schema}.UTIL_SYS_ETL_TASK_QUEUE.merge_params
  is 'Any additional input parameters required by the destination SP can be entered in this JSON.';
comment on column {Schema}.UTIL_SYS_ETL_TASK_QUEUE.load_destination
  is 'The destination SP to store/process the result-set of source.';
comment on column {Schema}.UTIL_SYS_ETL_TASK_QUEUE.load_command
  is 'The fully qualified name of the stored procedure for loading.';
