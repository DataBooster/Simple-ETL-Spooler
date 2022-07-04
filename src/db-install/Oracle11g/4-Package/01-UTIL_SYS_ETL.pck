CREATE OR REPLACE PACKAGE {Schema}.UTIL_SYS_ETL IS


  -- Simple ETL Spooler task processing
  -- Original Author     : Abel Cheng
  -- Original Repository : https://github.com/DataBooster/Simple-ETL-Spooler
  -- Created : 2022-04-10 15:51:56
  -- Notes   : This package requires to be customized.
  --           Please replace all "{Schema}" with the actual schema name you are going to install into,
  --           and replace the name of the parameter "inTheUserName" with the actual value of the UserNameReservedParameter
  --           setting defined in your DbWebApi (https://github.com/DataBooster/DbWebApi#username) if you have.


FUNCTION CREATE_BATCH
(
    inComment           VARCHAR2    := NULL,
    inScheduled_Time    DATE        := SYSDATE
)   RETURN SIMPLE_INTEGER;


FUNCTION ADD_TASK
(
    inBatch_ID          SIMPLE_INTEGER,
    inSerially          BOOLEAN,
    inExtract_Type      VARCHAR2,
    inExtract_Source    VARCHAR2,
    inExtract_Command   VARCHAR2,
    inExtract_Params    VARCHAR2    := '{}',
    inExtract_Timeout   SIMPLE_INTEGER  := 1800,
    inLoad_Type         VARCHAR2    := 'SP',
    inField_Mapping     VARCHAR2    := '{}',
    inMerge_Params      VARCHAR2    := '{}',
    inLoad_Destination  VARCHAR2,
    inLoad_Command      VARCHAR2,
    inLoad_Timeout      SIMPLE_INTEGER  := 1800,
    inTask_Comment      VARCHAR2,
    inStep_Plan         VARCHAR2    := NULL,
    inTheUserName 	    VARCHAR2	:= SYS_CONTEXT('USERENV', 'OS_USER')
)	RETURN SIMPLE_INTEGER;


PROCEDURE START_BATCH
(
    inBatch_ID          SIMPLE_INTEGER
);


FUNCTION START_SINGLE_TASK
(
    inEXTRACT_TYPE      VARCHAR2,
    inEXTRACT_SOURCE    VARCHAR2,
    inEXTRACT_COMMAND   VARCHAR2,
    inEXTRACT_PARAMS    VARCHAR2    := '{}',
    inExtract_Timeout   SIMPLE_INTEGER  := 1800,
    inLOAD_TYPE         VARCHAR2    := 'SP',
    inFIELD_MAPPING     VARCHAR2    := '{}',
    inMERGE_PARAMS      VARCHAR2    := '{}',
    inLOAD_DESTINATION  VARCHAR2,
    inLOAD_COMMAND      VARCHAR2,
    inLoad_Timeout      SIMPLE_INTEGER  := 1800,
    inComment           VARCHAR2    := NULL,
    inScheduled_Time    DATE        := SYSDATE,
    inTheUserName 	    VARCHAR2	:= SYS_CONTEXT('USERENV', 'OS_USER')
)   RETURN SIMPLE_INTEGER;


PROCEDURE CANCEL_BATCH
(
    inBatch_ID          SIMPLE_INTEGER
);


PROCEDURE POLL_TASK_QUEUE
(
    RC1     OUT SYS_REFCURSOR
);


PROCEDURE END_TASK
(
    inTask_ID           SIMPLE_INTEGER,
    inRuntime_Error     VARCHAR2
);


PROCEDURE END_BATCH
(
    inBatch_ID  SIMPLE_INTEGER
);


FUNCTION RERUN_BATCH
(
    inBatch_ID  SIMPLE_INTEGER
)   RETURN SIMPLE_INTEGER;


PROCEDURE CLEAN_UP
(
    inExpiry_Days   SIMPLE_INTEGER  := 7
);


END UTIL_SYS_ETL;
/
CREATE OR REPLACE PACKAGE BODY {Schema}.UTIL_SYS_ETL IS


  -- Simple ETL Spooler task processing
  -- Original Author     : Abel Cheng
  -- Original Repository : https://github.com/DataBooster/Simple-ETL-Spooler
  -- Created : 2022-04-10 15:51:56
  -- Notes   : This package requires to be customized.
  --           Please replace all "{Schema}" with the actual schema name you are going to install into,
  --           and replace the name of the parameter "inTheUserName" with the actual value of the UserNameReservedParameter
  --           setting defined in your DbWebApi (https://github.com/DataBooster/DbWebApi#username) if you have.


cApp_Key_Etl  CONSTANT VARCHAR2(30)    := 'POLL_ETL_TASK_QUEUE';


FUNCTION CREATE_BATCH
(
    inComment           VARCHAR2    := NULL,
    inScheduled_Time    DATE        := SYSDATE
)   RETURN SIMPLE_INTEGER
IS
tBatch_ID SIMPLE_INTEGER  := {Schema}.UTIL_LOG_BATCH_ID_SEQ.NEXTVAL;
BEGIN
    INSERT INTO {Schema}.UTIL_SYS_ETL_BATCH_STATUS (BATCH_ID, BATCH_COMMENT, SCHEDULED_TIME, BATCH_STATUS, TASKS_COUNT, SERIES_COUNT, ENTRY_TIME)
    VALUES (tBatch_ID, inComment, NVL(inScheduled_Time, SYSDATE), 'DRAFTING', 0, 0, SYSDATE);
    COMMIT;
    RETURN tBatch_ID;
END CREATE_BATCH;


FUNCTION ADD_TASK
(
    inBatch_ID          SIMPLE_INTEGER,
    inSerially          BOOLEAN,
    inExtract_Type      VARCHAR2,
    inExtract_Source    VARCHAR2,
    inExtract_Command   VARCHAR2,
    inExtract_Params    VARCHAR2    := '{}',
    inExtract_Timeout   SIMPLE_INTEGER  := 1800,
    inLoad_Type         VARCHAR2    := 'SP',
    inField_Mapping     VARCHAR2    := '{}',
    inMerge_Params      VARCHAR2    := '{}',
    inLoad_Destination  VARCHAR2,
    inLoad_Command      VARCHAR2,
    inLoad_Timeout      SIMPLE_INTEGER  := 1800,
    inTask_Comment      VARCHAR2,
    inStep_Plan         VARCHAR2    := NULL,
    inTheUserName 	    VARCHAR2	:= SYS_CONTEXT('USERENV', 'OS_USER')
)	RETURN SIMPLE_INTEGER
IS
tExtract_Type   VARCHAR2(32)    := UPPER(TRIM(inEXTRACT_TYPE));
tLoad_Type      VARCHAR2(32)    := UPPER(TRIM(inLOAD_TYPE));
tStep_Plan      VARCHAR2(2000)  := inSTEP_PLAN;
tSeries_Count   PLS_INTEGER;
tTask_ID        SIMPLE_INTEGER  := {Schema}.UTIL_SYS_ETL_TASK_ID_SEQ.NEXTVAL;
BEGIN
    IF tExtract_Type NOT IN ('SP', 'SQL', 'MDX', 'REST') THEN
		RAISE_APPLICATION_ERROR(-20081, 'inEXTRACT_TYPE: "' || inEXTRACT_TYPE || '" is not a supported extraction type, currently only "SP","SQL","MDX","REST" are supported.');
    END IF;
    IF tLoad_Type NOT IN ('SP') THEN
		RAISE_APPLICATION_ERROR(-20082, 'inLOAD_TYPE: "' || inLOAD_TYPE || '" is not a supported load type, currently only "SP" is supported.');
    END IF;

    SELECT  b.SERIES_COUNT  INTO tSeries_Count
    FROM    {Schema}.UTIL_SYS_ETL_BATCH_STATUS b
    WHERE   b.BATCH_ID  = inBatch_ID AND b.BATCH_STATUS = 'DRAFTING';

    IF inSerially OR tSeries_Count = 0 THEN
        tSeries_Count   := tSeries_Count + 1;
    END IF;

    IF tSTEP_PLAN IS NULL OR tSTEP_PLAN = ':' THEN
        tStep_Plan  := ':' || LPAD(TO_CHAR(tSeries_Count), 5, '0');
    END IF;

    INSERT INTO {Schema}.UTIL_SYS_ETL_TASK_QUEUE (
        TASK_ID,
        BATCH_ID,
        STEP_PLAN,
        EXTRACT_TYPE,
        EXTRACT_SOURCE,
        EXTRACT_COMMAND,
        EXTRACT_PARAMS,
        EXTRACT_TIMEOUT_SEC,
        RESULT_SET,
        NAMING_CONVENTION,
        LOAD_TYPE,
        FIELD_MAPPING,
        MERGE_PARAMS,
        LOAD_DESTINATION,
        LOAD_COMMAND,
        LOAD_TIMEOUT_SEC,
        TASK_COMMENT,
        CLIENT_ACCOUNT,
        ENTRY_TIME
    )
    VALUES (
        tTask_ID,
        inBatch_ID,
        tSTEP_PLAN,
        tExtract_Type,
        inEXTRACT_SOURCE,
        inEXTRACT_COMMAND,
        inEXTRACT_PARAMS,
        inExtract_Timeout,
        0,
        'N',
        tLoad_Type,
        inFIELD_MAPPING,
        inMERGE_PARAMS,
        inLOAD_DESTINATION,
        inLOAD_COMMAND,
        inLoad_Timeout,
        inTASK_COMMENT, 
        inTheUserName ,
        SYSDATE
    );

    UPDATE  {Schema}.UTIL_SYS_ETL_BATCH_STATUS b
    SET     b.SERIES_COUNT  = tSeries_Count,
            b.TASKS_COUNT   = b.TASKS_COUNT + 1
    WHERE   b.BATCH_ID  = inBatch_ID AND b.BATCH_STATUS = 'DRAFTING';

    COMMIT;
    RETURN tTask_ID;
END ADD_TASK;


PROCEDURE START_BATCH
(
    inBatch_ID          SIMPLE_INTEGER
)   IS
tTasks_Count    PLS_INTEGER;
BEGIN
    UPDATE  {Schema}.UTIL_SYS_ETL_BATCH_STATUS b
    SET     b.BATCH_STATUS  = 'READY_TO_RUN'
    WHERE   b.BATCH_ID  = inBatch_ID AND b.BATCH_STATUS = 'DRAFTING'
    RETURNING b.TASKS_COUNT INTO tTasks_Count;

    IF SQL%NOTFOUND THEN
		RAISE_APPLICATION_ERROR(-20084, TO_CHAR(inBatch_ID) || ' is not a valid Batch_ID ready to run.');
    END IF;

    IF tTasks_Count = 0 THEN
        INSERT INTO {Schema}.UTIL_SYS_ETL_BATCH_STATUS_HIST (
            BATCH_ID,
            BATCH_COMMENT,
            SCHEDULED_TIME,
            BATCH_STATUS,
            TASKS_COUNT,
            SERIES_COUNT,
            ENTRY_TIME,
            TRIGGERED_TIME,
            COMPLETED_TIME
        )
        SELECT
            b.BATCH_ID,
            b.BATCH_COMMENT,
            b.SCHEDULED_TIME,
            'VOIDED'            AS BATCH_STATUS,
            b.TASKS_COUNT,
            b.SERIES_COUNT,
            b.ENTRY_TIME,
            b.TRIGGERED_TIME,
            SYSDATE
        FROM    {Schema}.UTIL_SYS_ETL_BATCH_STATUS   b
        WHERE   b.BATCH_ID  = inBatch_ID;

        DELETE  FROM {Schema}.UTIL_SYS_ETL_BATCH_STATUS   b
        WHERE   b.BATCH_ID  = inBatch_ID;
    END IF;

    COMMIT;
END START_BATCH;


FUNCTION START_SINGLE_TASK
(
    inEXTRACT_TYPE      VARCHAR2,
    inEXTRACT_SOURCE    VARCHAR2,
    inEXTRACT_COMMAND   VARCHAR2,
    inEXTRACT_PARAMS    VARCHAR2    := '{}',
    inExtract_Timeout   SIMPLE_INTEGER  := 1800,
    inLOAD_TYPE         VARCHAR2    := 'SP',
    inFIELD_MAPPING     VARCHAR2    := '{}',
    inMERGE_PARAMS      VARCHAR2    := '{}',
    inLOAD_DESTINATION  VARCHAR2,
    inLOAD_COMMAND      VARCHAR2,
    inLoad_Timeout      SIMPLE_INTEGER  := 1800,
    inComment           VARCHAR2    := NULL,
    inScheduled_Time    DATE        := SYSDATE,
    inTheUserName 	    VARCHAR2	:= SYS_CONTEXT('USERENV', 'OS_USER')
)   RETURN SIMPLE_INTEGER
IS
tBatch_ID   PLS_INTEGER;
tTask_ID    PLS_INTEGER;
BEGIN
    tBatch_ID   := CREATE_BATCH(inComment, inScheduled_Time);
    tTask_ID    := ADD_TASK(tBatch_ID, FALSE,
                    inEXTRACT_TYPE, 
                    inEXTRACT_SOURCE,
                    inEXTRACT_COMMAND,
                    inEXTRACT_PARAMS,
                    inExtract_Timeout,
                    inLOAD_TYPE,
                    inFIELD_MAPPING,
                    inMERGE_PARAMS,
                    inLOAD_DESTINATION,
                    inLOAD_COMMAND,
                    inLoad_Timeout,
                    inComment,
                    '.',
                    inTheUserName );
    START_BATCH(tBatch_ID);
    RETURN tBatch_ID;
END START_SINGLE_TASK;


PROCEDURE CANCEL_BATCH
(
    inBatch_ID          SIMPLE_INTEGER
)   IS
BEGIN
    INSERT INTO {Schema}.UTIL_SYS_ETL_BATCH_STATUS_HIST (
        BATCH_ID,
        BATCH_COMMENT,
        SCHEDULED_TIME,
        BATCH_STATUS,
        TASKS_COUNT,
        SERIES_COUNT,
        ENTRY_TIME,
        TRIGGERED_TIME,
        COMPLETED_TIME
    )
    SELECT
        b.BATCH_ID,
        b.BATCH_COMMENT,
        b.SCHEDULED_TIME,
        'CANCELLED'         AS BATCH_STATUS,
        b.TASKS_COUNT,
        b.SERIES_COUNT,
        b.ENTRY_TIME,
        b.TRIGGERED_TIME,
        SYSDATE
    FROM
        {Schema}.UTIL_SYS_ETL_BATCH_STATUS   b
    WHERE
        b.BATCH_ID  = inBatch_ID
        AND b.BATCH_STATUS  IN ('DRAFTING', 'READY_TO_RUN');

    IF SQL%NOTFOUND THEN
        RETURN;
    END IF;

    INSERT INTO {Schema}.UTIL_SYS_ETL_TASK_QUEUE_HIST (
        TASK_ID,
        BATCH_ID,
        STEP_PLAN,
        EXTRACT_TYPE,
        EXTRACT_SOURCE,
        EXTRACT_COMMAND,
        EXTRACT_PARAMS,
        EXTRACT_TIMEOUT_SEC,
        RESULT_SET,
        NAMING_CONVENTION,
        LOAD_TYPE,
        FIELD_MAPPING,
        MERGE_PARAMS,
        LOAD_DESTINATION,
        LOAD_COMMAND,
        LOAD_TIMEOUT_SEC,
        TASK_COMMENT,
        CLIENT_ACCOUNT,
        ENTRY_TIME,
        COMPLETED_TIME,
        RUNTIME_ERROR
    )
    SELECT
        t.TASK_ID,
        t.BATCH_ID,
        t.STEP_PLAN,
        t.EXTRACT_TYPE,
        t.EXTRACT_SOURCE,
        t.EXTRACT_COMMAND,
        t.EXTRACT_PARAMS,
        t.EXTRACT_TIMEOUT_SEC,
        t.RESULT_SET,
        t.NAMING_CONVENTION,
        t.LOAD_TYPE,
        t.FIELD_MAPPING,
        t.MERGE_PARAMS,
        t.LOAD_DESTINATION,
        t.LOAD_COMMAND,
        t.LOAD_TIMEOUT_SEC,
        t.TASK_COMMENT,
        t.CLIENT_ACCOUNT,
        t.ENTRY_TIME,
        NVL(t.COMPLETED_TIME, SYSDATE)                      AS COMPLETED_TIME,
        NVL(t.RUNTIME_ERROR, 'Cancelled before running')    AS RUNTIME_ERROR
    FROM    {Schema}.UTIL_SYS_ETL_TASK_QUEUE     t
    WHERE   t.BATCH_ID  = inBatch_ID;

    IF SQL%FOUND THEN
        DELETE FROM {Schema}.UTIL_SYS_ETL_TASK_QUEUE     t
        WHERE   t.BATCH_ID  = inBatch_ID;
    END IF;

    DELETE  FROM {Schema}.UTIL_SYS_ETL_BATCH_STATUS   b
    WHERE   b.BATCH_ID  = inBatch_ID;

    COMMIT;
END CANCEL_BATCH;


PROCEDURE POLL_TASK_QUEUE
(
    RC1     OUT SYS_REFCURSOR
)   IS
BEGIN
    UPDATE  {Schema}.UTIL_SYS_LAST_ACTIVE_STATUS a
    SET     a.LAST_TIMESTAMP    = SYSTIMESTAMP
    WHERE   a.APP_KEY           = cApp_Key_Etl;

    DELETE FROM {Schema}.TEMP_NUM_ID;

    INSERT INTO {Schema}.TEMP_NUM_ID (ID_)
    SELECT  b.BATCH_ID
    FROM    {Schema}.UTIL_SYS_ETL_BATCH_STATUS    b
    WHERE   b.BATCH_STATUS      = 'READY_TO_RUN'
        AND b.SCHEDULED_TIME    <= SYSDATE;

    UPDATE  {Schema}.UTIL_SYS_ETL_BATCH_STATUS    b
    SET     b.BATCH_STATUS      = 'RUNNING',
            b.TRIGGERED_TIME    = SYSDATE
    WHERE   b.BATCH_ID IN (SELECT ID_ FROM {Schema}.TEMP_NUM_ID);

    COMMIT;

    OPEN RC1 FOR
    SELECT
        q.BATCH_ID,
        q.STEP_PLAN,
        q.TASK_ID,
        q.EXTRACT_TYPE,
        q.EXTRACT_SOURCE,
        q.EXTRACT_COMMAND,
        q.EXTRACT_PARAMS,
        q.EXTRACT_TIMEOUT_SEC,
        q.RESULT_SET,
        q.NAMING_CONVENTION,
        q.LOAD_TYPE,
        q.FIELD_MAPPING,
        q.MERGE_PARAMS,
        q.LOAD_DESTINATION,
        q.LOAD_COMMAND,
        q.LOAD_TIMEOUT_SEC
    FROM
        {Schema}.VIEW_UTIL_SYS_ETL_TASK_QUEUE    q
        JOIN
        {Schema}.TEMP_NUM_ID                     b
        ON q.BATCH_ID = b.ID_
    ORDER BY
        q.SCHEDULED_TIME,
        q.BATCH_ID,
        q.STEP_PLAN,
        q.TASK_ID;

END POLL_TASK_QUEUE;


PROCEDURE END_TASK
(
    inTask_ID           SIMPLE_INTEGER,
    inRuntime_Error     VARCHAR2
)   IS
BEGIN
    UPDATE  {Schema}.UTIL_SYS_ETL_TASK_QUEUE t
    SET     t.COMPLETED_TIME    = SYSDATE,
            t.RUNTIME_ERROR     = inRuntime_Error
    WHERE   t.TASK_ID           = inTask_ID;

    COMMIT;
END END_TASK;


PROCEDURE END_BATCH
(
    inBatch_ID  SIMPLE_INTEGER
)   IS
tError_Cnt  PLS_INTEGER;
BEGIN
    SELECT  COUNT(*)    INTO tError_Cnt
    FROM    {Schema}.UTIL_SYS_ETL_TASK_QUEUE    t
    WHERE   t.BATCH_ID = inBatch_ID AND t.RUNTIME_ERROR IS NOT NULL;

    INSERT INTO {Schema}.UTIL_SYS_ETL_BATCH_STATUS_HIST (
        BATCH_ID,
        BATCH_COMMENT,
        SCHEDULED_TIME,
        BATCH_STATUS,
        TASKS_COUNT,
        SERIES_COUNT,
        ENTRY_TIME,
        TRIGGERED_TIME,
        COMPLETED_TIME
    )
    SELECT
        b.BATCH_ID,
        b.BATCH_COMMENT,
        b.SCHEDULED_TIME,
        DECODE(tError_Cnt, 0, 'COMPLETED', 'COMPLETED_WITH_ERROR') AS BATCH_STATUS,
        b.TASKS_COUNT,
        b.SERIES_COUNT,
        b.ENTRY_TIME,
        b.TRIGGERED_TIME,
        SYSDATE
    FROM
        {Schema}.UTIL_SYS_ETL_BATCH_STATUS   b
    WHERE
        b.BATCH_ID  = inBatch_ID
        AND b.BATCH_STATUS  = 'RUNNING';

    IF SQL%NOTFOUND THEN
        RETURN;
    END IF;

    INSERT INTO {Schema}.UTIL_SYS_ETL_TASK_QUEUE_HIST (
        TASK_ID,
        BATCH_ID,
        STEP_PLAN,
        EXTRACT_TYPE,
        EXTRACT_SOURCE,
        EXTRACT_COMMAND,
        EXTRACT_PARAMS,
        EXTRACT_TIMEOUT_SEC,
        RESULT_SET,
        NAMING_CONVENTION,
        LOAD_TYPE,
        FIELD_MAPPING,
        MERGE_PARAMS,
        LOAD_DESTINATION,
        LOAD_COMMAND,
        LOAD_TIMEOUT_SEC,
        TASK_COMMENT,
        CLIENT_ACCOUNT,
        ENTRY_TIME,
        COMPLETED_TIME,
        RUNTIME_ERROR
    )
    SELECT
        t.TASK_ID,
        t.BATCH_ID,
        t.STEP_PLAN,
        t.EXTRACT_TYPE,
        t.EXTRACT_SOURCE,
        t.EXTRACT_COMMAND,
        t.EXTRACT_PARAMS,
        t.EXTRACT_TIMEOUT_SEC,
        t.RESULT_SET,
        t.NAMING_CONVENTION,
        t.LOAD_TYPE,
        t.FIELD_MAPPING,
        t.MERGE_PARAMS,
        t.LOAD_DESTINATION,
        t.LOAD_COMMAND,
        t.LOAD_TIMEOUT_SEC,
        t.TASK_COMMENT,
        t.CLIENT_ACCOUNT,
        t.ENTRY_TIME,
        t.COMPLETED_TIME,
        t.RUNTIME_ERROR
    FROM
        {Schema}.UTIL_SYS_ETL_TASK_QUEUE     t
    WHERE   t.BATCH_ID  = inBatch_ID
        AND t.COMPLETED_TIME    IS NOT NULL;

    IF SQL%FOUND THEN
        DELETE FROM {Schema}.UTIL_SYS_ETL_TASK_QUEUE     t
        WHERE   t.BATCH_ID  = inBatch_ID
            AND t.COMPLETED_TIME    IS NOT NULL;
    END IF;

    DELETE  FROM {Schema}.UTIL_SYS_ETL_BATCH_STATUS   b
    WHERE   b.BATCH_ID  = inBatch_ID
        AND b.BATCH_STATUS  = 'RUNNING';

    COMMIT;
END END_BATCH;


FUNCTION RERUN_BATCH
(
    inBatch_ID  SIMPLE_INTEGER
)   RETURN SIMPLE_INTEGER
IS
tBatch_ID SIMPLE_INTEGER  := {Schema}.UTIL_LOG_BATCH_ID_SEQ.NEXTVAL;
BEGIN
    INSERT INTO {Schema}.UTIL_SYS_ETL_BATCH_STATUS (
        BATCH_ID,
        BATCH_COMMENT,
        SCHEDULED_TIME,
        BATCH_STATUS,
        TASKS_COUNT,
        SERIES_COUNT,
        ENTRY_TIME
    )
    SELECT
        tBatch_ID,
        h.BATCH_COMMENT,
        SYSDATE         AS SCHEDULED_TIME,
        'DRAFTING'      AS BATCH_STATUS,
        TASKS_COUNT,
        SERIES_COUNT,
        SYSDATE         AS ENTRY_TIME
    FROM
        {Schema}.UTIL_SYS_ETL_BATCH_STATUS_HIST    h
    WHERE
        h.BATCH_ID   = inBatch_ID;

    INSERT INTO {Schema}.UTIL_SYS_ETL_TASK_QUEUE (
        TASK_ID,
        BATCH_ID,
        STEP_PLAN,
        EXTRACT_TYPE,
        EXTRACT_SOURCE,
        EXTRACT_COMMAND,
        EXTRACT_PARAMS,
        EXTRACT_TIMEOUT_SEC,
        RESULT_SET,
        NAMING_CONVENTION,
        LOAD_TYPE,
        FIELD_MAPPING,
        MERGE_PARAMS,
        LOAD_DESTINATION,
        LOAD_COMMAND,
        LOAD_TIMEOUT_SEC,
        TASK_COMMENT,
        CLIENT_ACCOUNT
    )
    SELECT
        {Schema}.UTIL_SYS_ETL_TASK_ID_SEQ.NEXTVAL    AS TASK_ID,
        tBatch_ID                               AS BATCH_ID,
        h.STEP_PLAN,
        h.EXTRACT_TYPE,
        h.EXTRACT_SOURCE,
        h.EXTRACT_COMMAND,
        h.EXTRACT_PARAMS,
        h.EXTRACT_TIMEOUT_SEC,
        h.RESULT_SET,
        h.NAMING_CONVENTION,
        h.LOAD_TYPE,
        h.FIELD_MAPPING,
        h.MERGE_PARAMS,
        h.LOAD_DESTINATION,
        h.LOAD_COMMAND,
        h.LOAD_TIMEOUT_SEC,
        h.TASK_COMMENT,
        SYS_CONTEXT('USERENV', 'OS_USER')       AS CLIENT_ACCOUNT
    FROM
        {Schema}.UTIL_SYS_ETL_TASK_QUEUE_HIST    h
    WHERE
        h.BATCH_ID   = inBatch_ID;

    START_BATCH(tBatch_ID);
    RETURN tBatch_ID;
END RERUN_BATCH;


PROCEDURE CLEAN_UP
(
    inExpiry_Days   SIMPLE_INTEGER  := 7
)
IS
tExpiry_Date    DATE    := SYSDATE - GREATEST(inExpiry_Days, 1);
BEGIN
    DELETE FROM {Schema}.TEMP_NUM_ID;

    FOR bat IN (
        SELECT b.BATCH_ID, b.BATCH_STATUS FROM {Schema}.UTIL_SYS_ETL_BATCH_STATUS b
        WHERE b.ENTRY_TIME < tExpiry_Date AND b.SCHEDULED_TIME < SYSDATE - 1
    ) LOOP
        IF bat.BATCH_STATUS  IN ('DRAFTING', 'READY_TO_RUN') THEN
            CANCEL_BATCH(bat.BATCH_ID);
        ELSE
            INSERT INTO {Schema}.TEMP_NUM_ID (ID_) VALUES (bat.BATCH_ID);
        END IF;
    END LOOP;

    INSERT INTO {Schema}.UTIL_SYS_ETL_BATCH_STATUS_HIST (
        BATCH_ID,
        BATCH_COMMENT,
        SCHEDULED_TIME,
        BATCH_STATUS,
        TASKS_COUNT,
        SERIES_COUNT,
        ENTRY_TIME,
        TRIGGERED_TIME,
        COMPLETED_TIME
    )
    SELECT
        b.BATCH_ID,
        b.BATCH_COMMENT,
        b.SCHEDULED_TIME,
        'ZOMBIE'            AS BATCH_STATUS,
        b.TASKS_COUNT,
        b.SERIES_COUNT,
        b.ENTRY_TIME,
        b.TRIGGERED_TIME,
        SYSDATE
    FROM
        {Schema}.UTIL_SYS_ETL_BATCH_STATUS   b
        JOIN
        {Schema}.TEMP_NUM_ID                 t
        ON  b.BATCH_ID  = t.ID_;

    IF SQL%NOTFOUND THEN
        RETURN;
    END IF;

    INSERT INTO {Schema}.UTIL_SYS_ETL_TASK_QUEUE_HIST (
        TASK_ID,
        BATCH_ID,
        STEP_PLAN,
        EXTRACT_TYPE,
        EXTRACT_SOURCE,
        EXTRACT_COMMAND,
        EXTRACT_PARAMS,
        EXTRACT_TIMEOUT_SEC,
        RESULT_SET,
        NAMING_CONVENTION,
        LOAD_TYPE,
        FIELD_MAPPING,
        MERGE_PARAMS,
        LOAD_DESTINATION,
        LOAD_COMMAND,
        LOAD_TIMEOUT_SEC,
        TASK_COMMENT,
        CLIENT_ACCOUNT,
        ENTRY_TIME,
        COMPLETED_TIME,
        RUNTIME_ERROR
    )
    SELECT
        t.TASK_ID,
        t.BATCH_ID,
        t.STEP_PLAN,
        t.EXTRACT_TYPE,
        t.EXTRACT_SOURCE,
        t.EXTRACT_COMMAND,
        t.EXTRACT_PARAMS,
        t.EXTRACT_TIMEOUT_SEC,
        t.RESULT_SET,
        t.NAMING_CONVENTION,
        t.LOAD_TYPE,
        t.FIELD_MAPPING,
        t.MERGE_PARAMS,
        t.LOAD_DESTINATION,
        t.LOAD_COMMAND,
        t.LOAD_TIMEOUT_SEC,
        t.TASK_COMMENT,
        t.CLIENT_ACCOUNT,
        t.ENTRY_TIME,
        t.COMPLETED_TIME,
        NVL(t.RUNTIME_ERROR, 'Zombie')  AS RUNTIME_ERROR
    FROM
        {Schema}.UTIL_SYS_ETL_TASK_QUEUE     t
        JOIN
        {Schema}.TEMP_NUM_ID                 z
        ON t.BATCH_ID  = z.ID_;

    IF SQL%FOUND THEN
        DELETE  FROM {Schema}.UTIL_SYS_ETL_TASK_QUEUE     t
        WHERE   t.BATCH_ID  IN (SELECT ID_ FROM {Schema}.TEMP_NUM_ID);
    END IF;

    DELETE  FROM {Schema}.UTIL_SYS_ETL_BATCH_STATUS   b
    WHERE   b.BATCH_ID  IN (SELECT ID_ FROM {Schema}.TEMP_NUM_ID);

    COMMIT;
END CLEAN_UP;


END UTIL_SYS_ETL;
/
