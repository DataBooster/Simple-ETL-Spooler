CREATE OR REPLACE PACKAGE EDS.DAILY_RFSH_MAIN IS


FUNCTION REFRESH_FROM_SF_TO_EDB(
    inDate            DATE,
    inSubject_Area    VARCHAR2    := 'DEFAULT',
    inParent_Batch_ID PLS_INTEGER := NULL
)
RETURN SIMPLE_INTEGER;


FUNCTION REFRESH_FROM_EDB_TO_SF(
    inDate            DATE,
    inSubject_Area    VARCHAR2    := 'DEFAULT',
    inParent_Batch_ID PLS_INTEGER := NULL
)
RETURN SIMPLE_INTEGER;


PROCEDURE REFRESH_ROUND_TRIP
(
    inDate            DATE,
    inSubject_Area    VARCHAR2    := 'DEFAULT',
    inParent_Batch_ID PLS_INTEGER := NULL,
    inWait            VARCHAR2    := 'N'
);


END DAILY_RFSH_MAIN;
/
CREATE OR REPLACE PACKAGE BODY EDS.DAILY_RFSH_MAIN IS

cSnowflake_RISK_DB_Conf CONSTANT VARCHAR2(30) := 'SNOWFLAKE.RISK_DB';
cEDB_Server             CONSTANT VARCHAR2(30) := ERS.UTIL_CONF_RENDERER.g_Cur_Edb;


FUNCTION COMPLETE_QUERY_DATE_COND(
    inQuery    VARCHAR2,
    inDate_Col VARCHAR2
)
RETURN VARCHAR2
IS
  tQuery      VARCHAR2(4000);
  tPrefix     VARCHAR2(4000);
  tMain_Query VARCHAR2(4000);
  tHead       VARCHAR2(4000);
  tTail       VARCHAR2(4000);
  tCond_Pred  VARCHAR2(200);
  tMain_Pos   PLS_INTEGER := 0;
  tClause_Pos PLS_INTEGER := 0;

  FUNCTION LAST_REGEX_INSTR(inText VARCHAR2, inPattern VARCHAR2)
  RETURN PLS_INTEGER
  IS
    tOcc PLS_INTEGER;
  BEGIN
    tOcc := REGEXP_COUNT(inText, inPattern, 1, 'in');
    IF tOcc > 0 THEN
      RETURN REGEXP_INSTR(inText, inPattern, 1, tOcc, 0, 'in');
    ELSE
      RETURN 0;
    END IF;
  END LAST_REGEX_INSTR;
BEGIN
  IF inQuery IS NULL OR inDate_Col IS NULL THEN
    RETURN inQuery;
  END IF;

  tQuery := RTRIM(inQuery);
  tCond_Pred := inDate_Col || ' = :' || inDate_Col;

  IF REGEXP_LIKE(tQuery, '^\s*WITH\W', 'in') THEN
    tMain_Pos := LAST_REGEX_INSTR(tQuery, '\)\s*SELECT\W');
    IF tMain_Pos > 0 THEN
      tMain_Pos := REGEXP_INSTR(tQuery, 'SELECT\W', tMain_Pos, 1, 0, 'in');
    END IF;
  END IF;

  IF tMain_Pos <= 0 THEN
    tMain_Pos := 1;
  END IF;

  tPrefix := SUBSTR(tQuery, 1, tMain_Pos - 1);
  tMain_Query := SUBSTR(tQuery, tMain_Pos);

  tMain_Pos := LAST_REGEX_INSTR(tMain_Query, '\s+(UNION(\s+ALL)?|INTERSECT|MINUS)\s+SELECT\W');
  IF tMain_Pos > 0 THEN
    tMain_Pos := REGEXP_INSTR(tMain_Query, 'SELECT\W', tMain_Pos, 1, 0, 'in');
    tPrefix := tPrefix || SUBSTR(tMain_Query, 1, tMain_Pos - 1);
    tMain_Query := SUBSTR(tMain_Query, tMain_Pos);
  END IF;

  tClause_Pos := GREATEST(
      LAST_REGEX_INSTR(tMain_Query, '\s+ORDER\s+BY\W'),
      LAST_REGEX_INSTR(tMain_Query, '\s+GROUP\s+BY\W'),
      LAST_REGEX_INSTR(tMain_Query, '\s+HAVING\W'));

  IF tClause_Pos > 0 THEN
    tHead := RTRIM(SUBSTR(tMain_Query, 1, tClause_Pos - 1));
    tTail := SUBSTR(tMain_Query, tClause_Pos);
  ELSE
    tHead := tMain_Query;
    tTail := NULL;
  END IF;

  IF REGEXP_LIKE(tHead, '(^|\W)WHERE\W.*:' || inDate_Col || '(\W|$)', 'in') THEN
    RETURN tQuery;
  END IF;

  IF NOT REGEXP_LIKE(tHead, '(^|\W)WHERE(\W|$)', 'in') THEN
    tCond_Pred := ' WHERE ' || tCond_Pred;
  ELSIF REGEXP_LIKE(tHead, '(^|\W)WHERE$', 'in') THEN
    tCond_Pred := ' ' || tCond_Pred;
  ELSE
    tCond_Pred := ' AND ' || tCond_Pred;
  END IF;

  RETURN tPrefix || tHead || tCond_Pred || tTail;
END COMPLETE_QUERY_DATE_COND;


FUNCTION REFRESH_FROM_SF_TO_EDB(
    inDate            DATE,
    inSubject_Area    VARCHAR2    := 'DEFAULT',
    inParent_Batch_ID PLS_INTEGER := NULL
)
RETURN SIMPLE_INTEGER
IS
  tBatch_ID   PLS_INTEGER;
  tTask_ID    PLS_INTEGER;
  tLast_Order VARCHAR2(3);
  tSrc_Query  VARCHAR2(4000);
  tRefresh_By VARCHAR2(2000);
BEGIN
  tBatch_ID := ERS.UTIL_SYS_ETL.CREATE_BATCH(
                 inComment         => 'Daily refresh(Snowflake-->Oracle) for ' || to_char(inDate, 'YYYY-MM-DD'),
                 inParent_Batch_ID => inParent_Batch_ID);
  FOR t IN (
      SELECT t.REFRESH_ORDER,
          t.TARGET_TABLE,
          t.SOURCE_QUERY,
          t.DATE_COLUMN
      FROM EDS.DAILY_RFSH_FROM_SF_TASK_CONF t
      WHERE t.IS_ENABLED = 'Y'
         AND t.SUBJECT_AREA = NVL(UPPER(TRIM(inSubject_Area)), 'DEFAULT')
      ORDER BY t.REFRESH_ORDER
  )	LOOP
      tSrc_Query  := COMPLETE_QUERY_DATE_COND(t.SOURCE_QUERY, t.DATE_COLUMN);
      tRefresh_By := CASE WHEN t.DATE_COLUMN IS NULL THEN '{}' ELSE JSON_OBJECT(t.DATE_COLUMN VALUE inDate) END;

      tTask_ID  := EDS.UTIL_SYS_REFRESH_TABLE_MAIN.ADD_REFRESH_SF_TO_EDB_TASK(
          inBatch_ID  => tBatch_ID,
          inSerially  => (t.REFRESH_ORDER <> tLast_Order),
          inSrc_SF_Conf_Key => cSnowflake_RISK_DB_Conf,
          inSrc_SF_Query    => tSrc_Query,
          inRefresh_By_Dict => tRefresh_By,
          inExtract_Timeout => 1800,
          inDst_EDB_Server  => cEDB_Server,
          inDst_EDB_Table   => t.TARGET_TABLE,
          inLoad_Timeout    => 1800,
          inTask_Comment    => 'Daily refresh(Snowflake->Oracle) of ' || t.TARGET_TABLE
        );

      tLast_Order := t.REFRESH_ORDER;
	END LOOP;

  ERS.UTIL_SYS_ETL.START_BATCH(tBatch_ID);
  RETURN tBatch_ID;

EXCEPTION
    WHEN OTHERS THEN
        ERS.UTIL_SYS_ETL.CANCEL_BATCH(tBatch_ID);
        RAISE;
END REFRESH_FROM_SF_TO_EDB;


FUNCTION REFRESH_FROM_EDB_TO_SF(
    inDate            DATE,
    inSubject_Area    VARCHAR2    := 'DEFAULT',
    inParent_Batch_ID PLS_INTEGER := NULL
)
RETURN SIMPLE_INTEGER
IS
  tBatch_ID   PLS_INTEGER;
  tTask_ID    PLS_INTEGER;
  tLast_Order VARCHAR2(3);
  tSrc_Query  VARCHAR2(4000);
  tRefresh_By VARCHAR2(2000);
BEGIN
  tBatch_ID := ERS.UTIL_SYS_ETL.CREATE_BATCH(
                 inComment         => 'Daily refresh(Oracle-->Snowflake) for ' || to_char(inDate, 'YYYY-MM-DD'),
                 inParent_Batch_ID => inParent_Batch_ID);
  FOR t IN (
      SELECT t.REFRESH_ORDER,
          t.TARGET_TABLE,
          t.SOURCE_QUERY,
          t.DATE_COLUMN
      FROM EDS.DAILY_RFSH_TO_SF_TASK_CONF t
      WHERE t.IS_ENABLED = 'Y'
         AND t.SUBJECT_AREA = NVL(UPPER(TRIM(inSubject_Area)), 'DEFAULT')
      ORDER BY t.REFRESH_ORDER
  )	LOOP
      tSrc_Query  := COMPLETE_QUERY_DATE_COND(t.SOURCE_QUERY, t.DATE_COLUMN);
      tRefresh_By := CASE WHEN t.DATE_COLUMN IS NULL THEN '{}' ELSE JSON_OBJECT(t.DATE_COLUMN VALUE inDate) END;

      tTask_ID  := EDS.UTIL_SYS_REFRESH_TABLE_MAIN.ADD_REFRESH_EDB_TO_SF_TASK(
          inBatch_ID  => tBatch_ID,
          inSerially  => (t.REFRESH_ORDER <> tLast_Order),
          inSrc_EDB_Server  => cEDB_Server,
          inSrc_EDB_Query   => tSrc_Query,
          inRefresh_By_Dict => tRefresh_By,
          inExtract_Timeout => 1800,
          inDst_SF_Conf_Key => cSnowflake_RISK_DB_Conf,
          inDst_SF_Table    => t.TARGET_TABLE,
          inLoad_Timeout    => 1800,
          inTask_Comment    => 'Daily refresh(Oracle->Snowflake) of ' || t.TARGET_TABLE
        );

      tLast_Order := t.REFRESH_ORDER;
	END LOOP;

  ERS.UTIL_SYS_ETL.START_BATCH(tBatch_ID);
  RETURN tBatch_ID;

EXCEPTION
    WHEN OTHERS THEN
        ERS.UTIL_SYS_ETL.CANCEL_BATCH(tBatch_ID);
        RAISE;
END REFRESH_FROM_EDB_TO_SF;


PROCEDURE TERP_BATCH_STATUS
(
    inStatus    VARCHAR2,
    inCur_Stg   VARCHAR2,
    inBatch_ID  PLS_INTEGER
)
IS
  tBatch_ID VARCHAR2(12)  := TO_CHAR(inBatch_ID);
  tErr_Msg  VARCHAR2(500);
BEGIN
    CASE inStatus
        WHEN 'COMPLETED' THEN RETURN;
        WHEN 'CANCELLED' THEN RETURN;
        WHEN 'VOIDED' THEN RETURN;
        WHEN 'DRAFTING' THEN tErr_Msg := 'Batch not started.';
        WHEN 'READY_TO_RUN' THEN tErr_Msg := 'Batch not picked up. Check the Windows service.';
        WHEN 'RUNNING' THEN tErr_Msg := 'Batch did not finish in time.';
        ELSE tErr_Msg := 'Batch failed. Check: SELECT * FROM ERS.VIEW_UTIL_SYS_ETL_TASK_STATUS WHERE BATCH_ID = ' || tBatch_ID;
    END CASE;

    RAISE_APPLICATION_ERROR(-20094, 'Batch_ID#' || tBatch_ID || '(' || inCur_Stg || '): ' || tErr_Msg);
END TERP_BATCH_STATUS;


PROCEDURE REFRESH_ROUND_TRIP
(
    inDate            DATE,
    inSubject_Area    VARCHAR2    := 'DEFAULT',
    inParent_Batch_ID PLS_INTEGER := NULL,
    inWait            VARCHAR2    := 'N'
) IS
  tBatch_ID_SF_TO_EDB PLS_INTEGER;
  tBatch_ID_EDB_TO_SF PLS_INTEGER;
  tBatch_Status       VARCHAR2(30);
BEGIN
    tBatch_ID_SF_TO_EDB := REFRESH_FROM_SF_TO_EDB(inDate => inDate, inSubject_Area => inSubject_Area, inParent_Batch_ID => inParent_Batch_ID);
    tBatch_ID_EDB_TO_SF := REFRESH_FROM_EDB_TO_SF(inDate => inDate, inSubject_Area => inSubject_Area, inParent_Batch_ID => tBatch_ID_SF_TO_EDB);

    IF UPPER(inWait) IN ('Y', 'YES', 'TRUE', 'T') THEN
        tBatch_Status := ERS.UTIL_SYS_ETL.WAIT_BATCH(inBatch_ID => tBatch_ID_SF_TO_EDB, inTimeout_Minutes => 240, inTimeslice_Seconds => 10);
        TERP_BATCH_STATUS(tBatch_Status, 'Snowflake->Oracle', tBatch_ID_SF_TO_EDB);
        tBatch_Status := ERS.UTIL_SYS_ETL.WAIT_BATCH(inBatch_ID => tBatch_ID_EDB_TO_SF, inTimeout_Minutes => 240, inTimeslice_Seconds => 10);
        TERP_BATCH_STATUS(tBatch_Status, 'Oracle->Snowflake', tBatch_ID_SF_TO_EDB);
    END IF;
END REFRESH_ROUND_TRIP;


END DAILY_RFSH_MAIN;
/
