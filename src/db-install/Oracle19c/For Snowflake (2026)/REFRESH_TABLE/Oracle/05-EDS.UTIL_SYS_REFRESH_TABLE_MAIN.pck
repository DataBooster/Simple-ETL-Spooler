CREATE OR REPLACE PACKAGE EDS.UTIL_SYS_REFRESH_TABLE_MAIN IS

-- Author  : Abel Cheng
-- Created : 2026-06-17 1:15:38 PM
-- Purpose : A generic SP receives refreshed data from external data sources in JSON format

FUNCTION GET_SF_DB_CONF(inTpl_Key VARCHAR2)
RETURN VARCHAR2;

FUNCTION VALIDATE_TABLE_EXIST(p_Table_Name IN VARCHAR2)
RETURN BOOLEAN;

FUNCTION ADD_REFRESH_SF_TO_EDB_TASK
(
    inBatch_ID          SIMPLE_INTEGER,
    inSerially          BOOLEAN,
    inSrc_SF_Conf_Key   VARCHAR2,
    inSrc_SF_Query      VARCHAR2,
    inRefresh_By_Dict   VARCHAR2,
    inExtract_Timeout   SIMPLE_INTEGER  := 1800,
    inDst_EDB_Server    VARCHAR2        := ERS.UTIL_CONF_RENDERER.g_Cur_Edb,
    inDst_EDB_Table     VARCHAR2,
    inLoad_Timeout      SIMPLE_INTEGER  := 1800,
    inTask_Comment      VARCHAR2,
    inDry_Run           BOOLEAN         := FALSE,
    inStep_Plan         VARCHAR2        := '.',
    inOtppUserName      VARCHAR2        := SYS_CONTEXT('USERENV', 'OS_USER')
) RETURN SIMPLE_INTEGER;

FUNCTION START_1_TABLE_RFSH_SF_TO_EDB
(
    inSrc_SF_Conf_Key   VARCHAR2,
    inSrc_SF_Query      VARCHAR2,
    inRefresh_By_Dict   VARCHAR2,
    inExtract_Timeout   SIMPLE_INTEGER  := 1800,
    inDst_EDB_Server    VARCHAR2        := ERS.UTIL_CONF_RENDERER.g_Cur_Edb,
    inDst_EDB_Table     VARCHAR2,
    inLoad_Timeout      SIMPLE_INTEGER  := 1800,
    inComment           VARCHAR2,
    inDry_Run           BOOLEAN         := FALSE,
    inScheduled_Time    DATE            := SYSDATE,
    inParent_Batch_ID   PLS_INTEGER     := NULL,
    inOtppUserName      VARCHAR2        := SYS_CONTEXT('USERENV', 'OS_USER')
) RETURN SIMPLE_INTEGER;

FUNCTION ADD_REFRESH_EDB_TO_SF_TASK
(
    inBatch_ID          SIMPLE_INTEGER,
    inSerially          BOOLEAN,
    inSrc_EDB_Server    VARCHAR2        := ERS.UTIL_CONF_RENDERER.g_Cur_Edb,
    inSrc_EDB_Query     VARCHAR2,
    inRefresh_By_Dict   VARCHAR2,
    inExtract_Timeout   SIMPLE_INTEGER  := 1800,
    inDst_SF_Conf_Key   VARCHAR2,
    inDst_SF_Table      VARCHAR2,
    inLoad_Timeout      SIMPLE_INTEGER  := 1800,
    inTask_Comment      VARCHAR2,
    inDry_Run           BOOLEAN         := FALSE,
    inStep_Plan         VARCHAR2        := '.',
    inOtppUserName      VARCHAR2        := SYS_CONTEXT('USERENV', 'OS_USER')
) RETURN SIMPLE_INTEGER;

FUNCTION START_1_TABLE_RFSH_EDB_TO_SF
(
    inSrc_EDB_Server    VARCHAR2        := ERS.UTIL_CONF_RENDERER.g_Cur_Edb,
    inSrc_EDB_Query     VARCHAR2,
    inRefresh_By_Dict   VARCHAR2,
    inExtract_Timeout   SIMPLE_INTEGER  := 1800,
    inDst_SF_Conf_Key   VARCHAR2,
    inDst_SF_Table      VARCHAR2,
    inLoad_Timeout      SIMPLE_INTEGER  := 1800,
    inComment           VARCHAR2,
    inDry_Run           BOOLEAN         := FALSE,
    inScheduled_Time    DATE            := SYSDATE,
    inParent_Batch_ID   PLS_INTEGER     := NULL,
    inOtppUserName      VARCHAR2        := SYS_CONTEXT('USERENV', 'OS_USER')
) RETURN SIMPLE_INTEGER;

PROCEDURE RECEIVE_TABLE(json_packet CLOB);

PROCEDURE ARCHIVE_REFRESH_LOG;

END UTIL_SYS_REFRESH_TABLE_MAIN;
/
CREATE OR REPLACE PACKAGE BODY EDS.UTIL_SYS_REFRESH_TABLE_MAIN IS

-- Shared infrastructure layout definitions
cPackage_Full_Name  CONSTANT VARCHAR2(50) := 'EDS.UTIL_SYS_REFRESH_TABLE_MAIN';
cLF                 CONSTANT VARCHAR2(1)  := CHR(10);

-- Package-Body Global Reserved Words Constants Setup
cTargetTableKey   CONSTANT VARCHAR2(30) := '_TARGET_TABLE_';
cRefreshByJsonKey CONSTANT VARCHAR2(30) := '_REFRESH_BY_JSON_';
cDryRunKey        CONSTANT VARCHAR2(30) := '_DRY_RUN_';

TYPE Column_Map_Type IS TABLE OF VARCHAR2(100) INDEX BY VARCHAR2(100);

FUNCTION GET_SF_DB_CONF(inTpl_Key VARCHAR2)
RETURN VARCHAR2
IS
BEGIN
  RETURN JSON_OBJECT('TplKey' VALUE inTpl_Key, 'OvrParams' VALUE '{}');
END GET_SF_DB_CONF;

FUNCTION ADD_REFRESH_SF_TO_EDB_TASK
(
    inBatch_ID          SIMPLE_INTEGER,
    inSerially          BOOLEAN,
    inSrc_SF_Conf_Key   VARCHAR2,
    inSrc_SF_Query      VARCHAR2,
    inRefresh_By_Dict   VARCHAR2,
    inExtract_Timeout   SIMPLE_INTEGER  := 1800,
    inDst_EDB_Server    VARCHAR2        := ERS.UTIL_CONF_RENDERER.g_Cur_Edb,
    inDst_EDB_Table     VARCHAR2,
    inLoad_Timeout      SIMPLE_INTEGER  := 1800,
    inTask_Comment      VARCHAR2,
    inDry_Run           BOOLEAN         := FALSE,
    inStep_Plan         VARCHAR2        := '.',
    inOtppUserName      VARCHAR2        := SYS_CONTEXT('USERENV', 'OS_USER')
) RETURN SIMPLE_INTEGER
IS
  tExtract_Source    VARCHAR2(500)  := GET_SF_DB_CONF(inTpl_Key => inSrc_SF_Conf_Key);
  tEnv_Related_Query VARCHAR2(4000) := ERS.UTIL_CONF_RENDERER.RENDER_ENV_PLACEHOLDER(inMarkup => inSrc_SF_Query);
  tMerge_ParamDict   JSON_OBJECT_T  := JSON_OBJECT_T();
BEGIN
  tMerge_ParamDict.put(cTargetTableKey, inDst_EDB_Table);
  tMerge_ParamDict.put(cRefreshByJsonKey, inRefresh_By_Dict);
  IF inDry_Run THEN
    tMerge_ParamDict.put(cDryRunKey, inDry_Run);
  ELSIF inDst_EDB_Server = ERS.UTIL_CONF_RENDERER.g_Cur_Edb AND
    NOT VALIDATE_TABLE_EXIST(inDst_EDB_Table) THEN
		RAISE_APPLICATION_ERROR(-20072, 'Destination table ' || inDst_EDB_Table || ' is not available in ' || inDst_EDB_Server || '.');
  END IF;

  RETURN ERS.UTIL_SYS_ETL.ADD_TASK(
    inBatch_ID          => inBatch_ID,
    inSerially          => inSerially,
    inExtract_Type      => 'SQL',
    inExtract_Source    => tExtract_Source,
    inExtract_Command   => tEnv_Related_Query,
    inExtract_Params    => inRefresh_By_Dict,
    inExtract_Timeout   => inExtract_Timeout,
    inLoad_Type         => 'SP',
    inField_Mapping     => JSON_OBJECT('*' VALUE 'json_packet'),
    inMerge_Params      => tMerge_ParamDict.TO_STRING,
    inLoad_Destination  => inDst_EDB_Server,
    inLoad_Command      => cPackage_Full_Name || '.RECEIVE_TABLE',
    inLoad_Timeout      => inLoad_Timeout,
    inTask_Comment      => inTask_Comment,
    inStep_Plan         => inStep_Plan,
    inOtppUserName      => inOtppUserName);
END ADD_REFRESH_SF_TO_EDB_TASK;

FUNCTION START_1_TABLE_RFSH_SF_TO_EDB
(
    inSrc_SF_Conf_Key   VARCHAR2,
    inSrc_SF_Query      VARCHAR2,
    inRefresh_By_Dict   VARCHAR2,
    inExtract_Timeout   SIMPLE_INTEGER  := 1800,
    inDst_EDB_Server    VARCHAR2        := ERS.UTIL_CONF_RENDERER.g_Cur_Edb,
    inDst_EDB_Table     VARCHAR2,
    inLoad_Timeout      SIMPLE_INTEGER  := 1800,
    inComment           VARCHAR2,
    inDry_Run           BOOLEAN         := FALSE,
    inScheduled_Time    DATE            := SYSDATE,
    inParent_Batch_ID   PLS_INTEGER     := NULL,
    inOtppUserName      VARCHAR2        := SYS_CONTEXT('USERENV', 'OS_USER')
) RETURN SIMPLE_INTEGER
IS
  tBatch_ID PLS_INTEGER;
  tTask_ID  PLS_INTEGER;
BEGIN
  tBatch_ID := ERS.UTIL_SYS_ETL.CREATE_BATCH(
                 inComment         => inComment,
                 inScheduled_Time  => inScheduled_Time,
                 inParent_Batch_ID => inParent_Batch_ID);
  tTask_ID := ADD_REFRESH_SF_TO_EDB_TASK(
                inBatch_ID        => tBatch_ID,
                inSerially        => FALSE,
                inSrc_SF_Conf_Key => inSrc_SF_Conf_Key,
                inSrc_SF_Query    => inSrc_SF_Query,
                inRefresh_By_Dict => inRefresh_By_Dict,
                inExtract_Timeout => inExtract_Timeout,
                inDst_EDB_Server  => inDst_EDB_Server,
                inDst_EDB_Table   => inDst_EDB_Table,
                inLoad_Timeout    => inLoad_Timeout,
                inTask_Comment    => inComment,
                inDry_Run         => inDry_Run,
                inOtppUserName    => inOtppUserName);
  ERS.UTIL_SYS_ETL.START_BATCH(tBatch_ID);
  RETURN tBatch_ID;
EXCEPTION
    WHEN OTHERS THEN
        ERS.UTIL_SYS_ETL.CANCEL_BATCH(tBatch_ID);
        RAISE;
END START_1_TABLE_RFSH_SF_TO_EDB;

FUNCTION ADD_REFRESH_EDB_TO_SF_TASK
(
    inBatch_ID          SIMPLE_INTEGER,
    inSerially          BOOLEAN,
    inSrc_EDB_Server    VARCHAR2        := ERS.UTIL_CONF_RENDERER.g_Cur_Edb,
    inSrc_EDB_Query     VARCHAR2,
    inRefresh_By_Dict   VARCHAR2,
    inExtract_Timeout   SIMPLE_INTEGER  := 1800,
    inDst_SF_Conf_Key   VARCHAR2,
    inDst_SF_Table      VARCHAR2,
    inLoad_Timeout      SIMPLE_INTEGER  := 1800,
    inTask_Comment      VARCHAR2,
    inDry_Run           BOOLEAN         := FALSE,
    inStep_Plan         VARCHAR2        := '.',
    inOtppUserName      VARCHAR2        := SYS_CONTEXT('USERENV', 'OS_USER')
) RETURN SIMPLE_INTEGER
IS
  tLoad_Destination   VARCHAR2(500)  := GET_SF_DB_CONF(inTpl_Key => inDst_SF_Conf_Key);
  tEnv_Related_Query  VARCHAR2(4000) := ERS.UTIL_CONF_RENDERER.RENDER_ENV_PLACEHOLDER(inMarkup => inSrc_EDB_Query);
  tMerge_ParamDict    JSON_OBJECT_T  := JSON_OBJECT_T();
BEGIN
  tMerge_ParamDict.put(cTargetTableKey, inDst_SF_Table);
  tMerge_ParamDict.put(cRefreshByJsonKey, inRefresh_By_Dict);
  IF inDry_Run THEN
    tMerge_ParamDict.put(cDryRunKey, inDry_Run);
  END IF;

  RETURN ERS.UTIL_SYS_ETL.ADD_TASK(
    inBatch_ID          => inBatch_ID,
    inSerially          => inSerially,
    inExtract_Type      => 'SQL',
    inExtract_Source    => inSrc_EDB_Server,
    inExtract_Command   => tEnv_Related_Query,
    inExtract_Params    => inRefresh_By_Dict,
    inExtract_Timeout   => inExtract_Timeout,
    inLoad_Type         => 'SP',
    inField_Mapping     => JSON_OBJECT('*' VALUE 'json_packet'),
    inMerge_Params      => tMerge_ParamDict.TO_STRING,
    inLoad_Destination  => tLoad_Destination,
    inLoad_Command      => 'CALL STG.UTIL_SYS_REFRESH_TABLE_RECEIVE(:json_packet)',
    inLoad_Timeout      => inLoad_Timeout,
    inTask_Comment      => inTask_Comment,
    inStep_Plan         => inStep_Plan,
    inOtppUserName      => inOtppUserName);
END ADD_REFRESH_EDB_TO_SF_TASK;

FUNCTION START_1_TABLE_RFSH_EDB_TO_SF
(
    inSrc_EDB_Server    VARCHAR2        := ERS.UTIL_CONF_RENDERER.g_Cur_Edb,
    inSrc_EDB_Query     VARCHAR2,
    inRefresh_By_Dict   VARCHAR2,
    inExtract_Timeout   SIMPLE_INTEGER  := 1800,
    inDst_SF_Conf_Key   VARCHAR2,
    inDst_SF_Table      VARCHAR2,
    inLoad_Timeout      SIMPLE_INTEGER  := 1800,
    inComment           VARCHAR2,
    inDry_Run           BOOLEAN         := FALSE,
    inScheduled_Time    DATE            := SYSDATE,
    inParent_Batch_ID   PLS_INTEGER     := NULL,
    inOtppUserName      VARCHAR2        := SYS_CONTEXT('USERENV', 'OS_USER')
) RETURN SIMPLE_INTEGER
IS
  tBatch_ID PLS_INTEGER;
  tTask_ID  PLS_INTEGER;
BEGIN
  tBatch_ID := ERS.UTIL_SYS_ETL.CREATE_BATCH(
                 inComment         => inComment,
                 inScheduled_Time  => inScheduled_Time,
                 inParent_Batch_ID => inParent_Batch_ID);
  tTask_ID := ADD_REFRESH_EDB_TO_SF_TASK(
                inBatch_ID        => tBatch_ID,
                inSerially        => FALSE,
                inSrc_EDB_Server  => inSrc_EDB_Server,
                inSrc_EDB_Query   => inSrc_EDB_Query,
                inRefresh_By_Dict => inRefresh_By_Dict,
                inExtract_Timeout => inExtract_Timeout,
                inDst_SF_Conf_Key => inDst_SF_Conf_Key,
                inDst_SF_Table    => inDst_SF_Table,
                inLoad_Timeout    => inLoad_Timeout,
                inTask_Comment    => inComment,
                inDry_Run         => inDry_Run,
                inOtppUserName    => inOtppUserName);
  ERS.UTIL_SYS_ETL.START_BATCH(tBatch_ID);
  RETURN tBatch_ID;
EXCEPTION
    WHEN OTHERS THEN
        ERS.UTIL_SYS_ETL.CANCEL_BATCH(tBatch_ID);
        RAISE;
END START_1_TABLE_RFSH_EDB_TO_SF;


  -- =========================================================================
  -- NESTED PUBLIC FUNCTION: VALIDATE_TABLE_EXIST
  -- Probes the database catalog for target table existence boundary.
  -- Utilizes boolean short-circuit evaluation via %ROWTYPE check blocks.
  -- =========================================================================
  FUNCTION VALIDATE_TABLE_EXIST(p_Table_Name IN VARCHAR2) RETURN BOOLEAN
  IS
    v_Block_Stream VARCHAR2(400);
  BEGIN
    IF p_Table_Name IS NULL OR NOT REGEXP_LIKE(p_Table_Name, '^[A-Z][A-Z0-9_$#.]*$') THEN
        RETURN FALSE;
    END IF;

    v_Block_Stream := 'DECLARE r ' || p_Table_Name || '%ROWTYPE; BEGIN NULL; END;';
    BEGIN
      EXECUTE IMMEDIATE v_Block_Stream;
      RETURN TRUE;
    EXCEPTION
      WHEN OTHERS THEN
        RETURN FALSE;
    END;
  END VALIDATE_TABLE_EXIST;

  -- =========================================================================
  -- NESTED PRIVATE FUNCTION: PROBE_TAB_COLUMN
  -- Performs runtime data type reflection for a standalone column name.
  -- Short-circuits reflection passes natively under active mock lifecycles.
  -- =========================================================================
  FUNCTION PROBE_TAB_COLUMN(
    p_Target_Table      IN VARCHAR2,
    p_Table_Mock        IN BOOLEAN,
    p_Col_Name          IN VARCHAR2,
    p_Type_Dict         IN OUT Column_Map_Type
  ) RETURN VARCHAR2
  IS
    v_Dynamic_Block     CLOB;
    v_System_Type       VARCHAR2(100);
    v_Error_Stack       VARCHAR2(4000);
    v_Assigned_Token    VARCHAR2(100);
  BEGIN
    IF p_Table_Mock THEN
        p_Type_Dict(p_Col_Name) := '_MOCK_';
        RETURN '_MOCK_';
    END IF;

    v_Dynamic_Block := 
        'DECLARE' || cLF ||
        '  v_Row ' || p_Target_Table || '%ROWTYPE;' || cLF ||
        '  FUNCTION PROBE(p NUMBER) RETURN VARCHAR2 IS BEGIN RETURN ''NUMBER''; END;' || cLF ||
        '  FUNCTION PROBE(p DATE) RETURN VARCHAR2 IS BEGIN RETURN ''DATE''; END;' || cLF ||
        '  FUNCTION PROBE(p TIMESTAMP) RETURN VARCHAR2 IS BEGIN RETURN ''TIMESTAMP''; END;' || cLF ||
        '  FUNCTION PROBE(p CLOB) RETURN VARCHAR2 IS BEGIN RETURN ''CLOB''; END;' || cLF ||
        '  FUNCTION PROBE(p BLOB) RETURN VARCHAR2 IS BEGIN RETURN ''BLOB''; END;' || cLF ||
        '  FUNCTION PROBE(p VARCHAR2) RETURN VARCHAR2 IS BEGIN RETURN ''VARCHAR2(4000)''; END;' || cLF ||
        'BEGIN' || cLF ||
        '  :1 := PROBE(v_Row.' || p_Col_Name || ');' || cLF ||
        'END;';
        
    BEGIN
        v_System_Type := NULL;
        EXECUTE IMMEDIATE v_Dynamic_Block USING OUT v_System_Type;
        
        IF v_System_Type IS NOT NULL THEN
            IF INSTR(v_System_Type, 'NUMBER') > 0 THEN
                v_Assigned_Token := 'NUMBER';
            ELSIF INSTR(v_System_Type, 'FLOAT') > 0 OR INSTR(v_System_Type, 'DOUBLE') > 0 THEN
                v_Assigned_Token := 'NUMBER';
            ELSIF INSTR(v_System_Type, 'CLOB') > 0 THEN
                v_Assigned_Token := 'CLOB';
            ELSIF INSTR(v_System_Type, 'BLOB') > 0 THEN
                v_Assigned_Token := 'BLOB';
            ELSIF INSTR(v_System_Type, 'DATE') > 0 THEN
                v_Assigned_Token := 'DATE';
            ELSIF INSTR(v_System_Type, 'TIMESTAMP') > 0 THEN
                v_Assigned_Token := 'TIMESTAMP';
            ELSE
                v_Assigned_Token := 'VARCHAR2(4000)';
            END IF;
        ELSE
            v_Assigned_Token := 'VARCHAR2(4000)';
        END IF;
        
        p_Type_Dict(p_Col_Name) := v_Assigned_Token;
        RETURN v_Assigned_Token;
        
    EXCEPTION
        WHEN OTHERS THEN
          v_Error_Stack := UPPER(DBMS_UTILITY.FORMAT_ERROR_STACK || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
          
          IF INSTR(v_Error_Stack, 'PLS-00302') = 0 THEN
              p_Type_Dict(p_Col_Name) := 'VARCHAR2(4000)';
              RETURN 'VARCHAR2(4000)';
          ELSE
              RETURN NULL;
          END IF;
    END;
  END PROBE_TAB_COLUMN;

  -- =========================================================================
  -- NESTED PRIVATE PROCEDURE: PREPROCESS_JSON_STREAM
  -- Extracts data arrays and control scalars maps under separate sequence layers.
  -- Enforces data priority by parsing matrices loops ahead of configuration inputs.
  -- =========================================================================
  PROCEDURE PREPROCESS_JSON_STREAM(
    p_Target_Table    IN VARCHAR2,
    p_Table_Mock      IN BOOLEAN,
    p_Refresh_By_Json IN VARCHAR2, 
    p_Json_Obj        IN OUT JSON_OBJECT_T,
    p_Type_Dict       IN OUT Column_Map_Type, 
    p_Ctrl_For_Del    OUT Column_Map_Type, 
    p_Ctrl_For_Ins    OUT Column_Map_Type, 
    p_Rows_Array      OUT JSON_ARRAY_T
  )
  IS
    v_Refresh_By_Obj  JSON_OBJECT_T; 
    v_Key_List        JSON_KEY_LIST; 
    v_Key_Name        VARCHAR2(100);
    v_Col_Name        VARCHAR2(100);
    v_DataType_Token  VARCHAR2(100);
    v_Check_Elem      JSON_ELEMENT_T;
    v_Is_New_Ctrl     BOOLEAN;
  BEGIN
    BEGIN
      v_Refresh_By_Obj := JSON_OBJECT_T.parse(p_Refresh_By_Json);
    EXCEPTION 
      WHEN OTHERS THEN
        RAISE_APPLICATION_ERROR(-20001, cRefreshByJsonKey || ' must be a valid JSON object string schema.');
    END;

    -- Layer 1: Processes untrusted data elements to lock down priority types
    v_Key_List := p_Json_Obj.get_keys; 

    FOR i IN REVERSE 1 .. v_Key_List.COUNT LOOP
      v_Key_Name := v_Key_List(i);
      v_Col_Name := UPPER(v_Key_Name);
      v_Check_Elem := p_Json_Obj.get(v_Key_Name);

      IF v_Check_Elem IS NULL OR NOT v_Check_Elem.is_array THEN
          v_DataType_Token := NULL;
      ELSE
          v_DataType_Token := PROBE_TAB_COLUMN(p_Target_Table, p_Table_Mock, v_Col_Name, p_Type_Dict);
      END IF;

      IF v_DataType_Token IS NULL THEN
          p_Json_Obj.remove(v_Key_Name); 
      ELSE
          IF p_Rows_Array IS NULL THEN
             p_Rows_Array := TREAT(v_Check_Elem AS JSON_ARRAY_T).clone;
          END IF;
      END IF;
    END LOOP;

    -- Layer 2: Registers remaining control keys safely avoiding data overrides
    v_Key_List := v_Refresh_By_Obj.get_keys; 

    FOR i IN 1 .. v_Key_List.COUNT LOOP
      v_Key_Name := v_Key_List(i);
      v_Col_Name := UPPER(TRIM(v_Key_Name));

      IF v_Col_Name IS NULL OR NOT REGEXP_LIKE(v_Col_Name, '^[A-Z][A-Z0-9_]*$') THEN
          RAISE_APPLICATION_ERROR(-20001, cRefreshByJsonKey || ' contains invalid column name.');
      END IF;

      v_Is_New_Ctrl := NOT p_Type_Dict.EXISTS(v_Col_Name);

      IF PROBE_TAB_COLUMN(p_Target_Table, p_Table_Mock, v_Col_Name, p_Type_Dict) IS NOT NULL THEN
          p_Ctrl_For_Del(v_Col_Name) := v_Key_Name;
          
          IF v_Is_New_Ctrl THEN
              p_Ctrl_For_Ins(v_Col_Name) := v_Key_Name;
          END IF;
      END IF;
    END LOOP;

  END PREPROCESS_JSON_STREAM;

  -- =========================================================================
  -- NESTED PRIVATE PROCEDURE: BUILD_DML_CLAUSES
  -- Generates fully parameterized PL/SQL execution wrappers dynamically.
  -- Maps delete bounds via full control matrices and insertion bounds via unique complementary offsets.
  -- =========================================================================
  PROCEDURE BUILD_DML_CLAUSES(
    p_Target_Table IN VARCHAR2,
    p_Ctrl_For_Del IN Column_Map_Type, 
    p_Ctrl_For_Ins IN Column_Map_Type, 
    p_Type_Dict    IN Column_Map_Type,
    p_Delete_SQL   OUT CLOB,
    p_Insert_SQL   OUT CLOB
  )
  IS
    v_Loop_Idx        VARCHAR2(100);
    v_Col_Name        VARCHAR2(100);
    v_Key_Name        VARCHAR2(100);
    v_DataType_Token  VARCHAR2(100);
    
    v_Del_Ctrl_Cols   CLOB;
    v_Delete_Where    CLOB;
    v_Insert_Cols     CLOB;
    v_Select_Cols     CLOB;
    v_Ins_Ctrl_Cols   CLOB;
    v_From_Data_Tab   CLOB;
  BEGIN
    -- Pass 1: Extract complete historical deletion constraints utilizing full p_Ctrl_For_Del
    v_Loop_Idx := p_Ctrl_For_Del.FIRST;
    WHILE v_Loop_Idx IS NOT NULL LOOP
        v_Col_Name := v_Loop_Idx;
        v_Key_Name := p_Ctrl_For_Del(v_Col_Name); 
        v_DataType_Token := p_Type_Dict(v_Col_Name);

        v_Del_Ctrl_Cols := v_Del_Ctrl_Cols || ',' || cLF || '    ' || v_Col_Name || ' ' || v_DataType_Token || ' PATH ''$.' || v_Key_Name || '''';
        v_Delete_Where := v_Delete_Where || '(t.' || v_Col_Name || ' = j.' || v_Col_Name || 
                           ' OR (t.' || v_Col_Name || ' IS NULL AND j.' || v_Col_Name || ' IS NULL))' || cLF || '    AND ';

        v_Loop_Idx := p_Ctrl_For_Del.NEXT(v_Loop_Idx);
    END LOOP;

    IF p_Target_Table IS NOT NULL THEN
        p_Delete_SQL := 'DECLARE' || cLF || 
                        '    v_c CLOB := :1;' || cLF || 
                        'BEGIN' || cLF || 
                        '    DELETE FROM ' || p_Target_Table || ' t';

        IF v_Delete_Where IS NOT NULL THEN
            v_Delete_Where := SUBSTR(v_Delete_Where, 1, LENGTH(v_Delete_Where) - 9); 
            p_Delete_SQL   := p_Delete_SQL || cLF ||
                              '    WHERE EXISTS (' || cLF ||
                              '        SELECT 1 ' || cLF ||
                              '        FROM JSON_TABLE(v_c, ''$''' || cLF ||
                              '            COLUMNS (' || cLF ||
                              '            ' || LTRIM(v_Del_Ctrl_Cols, ',' || cLF) || cLF ||
                              '            )) j' || cLF ||
                              '        WHERE ' || v_Delete_Where || cLF ||
                              '    )';
        END IF;

        p_Delete_SQL := p_Delete_SQL || ';' || cLF || 
                        '    :2 := SQL%ROWCOUNT;' || cLF || 
                        'END;';
    END IF;

    -- Pass 2: Map unique complementary configurations tokens securely into j1 proxies
    v_Loop_Idx := p_Ctrl_For_Ins.FIRST;
    WHILE v_Loop_Idx IS NOT NULL LOOP
        v_Col_Name := v_Loop_Idx;
        v_Key_Name := p_Ctrl_For_Ins(v_Col_Name);
        v_DataType_Token := p_Type_Dict(v_Col_Name);

        v_Insert_Cols := v_Insert_Cols || ', ' || v_Col_Name;
        v_Select_Cols := v_Select_Cols || ',' || cLF || '        j1.' || v_Col_Name;
        v_Ins_Ctrl_Cols := v_Ins_Ctrl_Cols || ',' || cLF || '    ' || v_Col_Name || ' ' || v_DataType_Token || ' PATH ''$.' || v_Key_Name || '''';

        v_Loop_Idx := p_Ctrl_For_Ins.NEXT(v_Loop_Idx);
    END LOOP;

    -- Pass 3: Process downstream pure array data vectors elements
    v_Loop_Idx := p_Type_Dict.FIRST;
    WHILE v_Loop_Idx IS NOT NULL LOOP
        v_Col_Name := v_Loop_Idx;
        IF NOT p_Ctrl_For_Ins.EXISTS(v_Col_Name) THEN
            v_DataType_Token := p_Type_Dict(v_Col_Name);
            
            v_Insert_Cols   := v_Insert_Cols || ', ' || v_Col_Name;
            v_Select_Cols   := v_Select_Cols || ',' || cLF || '        j2.' || v_Col_Name;
            v_From_Data_Tab := v_From_Data_Tab || ',' || cLF || '        ' || v_Col_Name || ' ' || v_DataType_Token || ' PATH ''$.' || v_Col_Name || '''';
        END IF;
        v_Loop_Idx := p_Type_Dict.NEXT(v_Loop_Idx);
    END LOOP;

    IF v_Insert_Cols IS NOT NULL THEN
        v_Insert_Cols   := LTRIM(v_Insert_Cols, ', ');
        v_From_Data_Tab := LTRIM(v_From_Data_Tab, ',' || cLF);
        v_Select_Cols   := LTRIM(v_Select_Cols, ',');
        
        v_From_Data_Tab := 'FROM JSON_TABLE(v_d, ''$[*]''' || cLF || '    COLUMNS (' || cLF || 
                          v_From_Data_Tab || cLF || '    )) j2';
                          
        IF v_Ins_Ctrl_Cols IS NOT NULL THEN
            v_From_Data_Tab := v_From_Data_Tab || cLF ||
                              '    CROSS JOIN JSON_TABLE(v_c, ''$''' || cLF || '        COLUMNS (' || cLF ||
                              '        ' || LTRIM(v_Ins_Ctrl_Cols, ',' || cLF) || cLF ||
                              '        )) j1';
        END IF;
        
        p_Insert_SQL := 'DECLARE' || cLF || 
                        '    v_c CLOB := :1;' || cLF || 
                        '    v_d CLOB := :2;' || cLF || 
                        'BEGIN' || cLF ||
                        '    INSERT INTO ' || p_Target_Table || ' (' || v_Insert_Cols || ')' || cLF ||
                        '    SELECT' || v_Select_Cols || cLF || 
                        '    ' || v_From_Data_Tab || ';' || cLF ||
                        '    :3 := SQL%ROWCOUNT;' || cLF || 
                        'END;';
    END IF;
  END BUILD_DML_CLAUSES;

  -- =========================================================================
  -- MAIN BUSINESS ENTRY POINT PROCEDURE IMPLEMENTATION
  -- Orator-level pipeline marshalling contextual vectors parameters parsing maps.
  -- Systematically updates performance log data using unique transactional markers.
  -- =========================================================================
  PROCEDURE RECEIVE_TABLE(json_packet CLOB)
  IS
    tLog_Id            PLS_INTEGER := EDS.LOG_ID_SEQUENCE.NEXTVAL;
    tTarget_Table      VARCHAR2(100);
    tRefresh_By_Json   VARCHAR2(4000);
    
    t_Ctrl_For_Del     Column_Map_Type; 
    t_Ctrl_For_Ins     Column_Map_Type; 
    tJson_Obj          JSON_OBJECT_T;
    tJson_Keys         JSON_KEY_LIST;
    
    tKey_Name          VARCHAR2(100); 
    tCol_Name          VARCHAR2(100); 

    tInsert_SQL        CLOB;
    tDelete_SQL        CLOB;
    tError_Msg         VARCHAR2(4000);

    tUpd_Col_Count     PLS_INTEGER := 0;
    tDel_Row_Count     PLS_INTEGER := 0;
    tIns_Row_Count     PLS_INTEGER := 0;
    
    t_Rows_Array       JSON_ARRAY_T;
    t_Row_Obj          JSON_OBJECT_T;
    tTransposed_Json   CLOB;
    t_Type_Dict        Column_Map_Type;
    
    t_Is_Dry_Run       BOOLEAN := FALSE;
    t_Table_Mock       BOOLEAN := FALSE; 
  BEGIN
    INSERT INTO EDS.UTIL_SYS_RFSH_TABLE_LOG (LOG_ID, PAYLOAD_JSON) 
    VALUES (tLog_Id, json_packet);
    COMMIT;

    tJson_Obj := JSON_OBJECT_T.parse(json_packet);
    
    tTarget_Table := UPPER(TRIM(tJson_Obj.get_string(cTargetTableKey)));
    tRefresh_By_Json := TRIM(tJson_Obj.get_string(cRefreshByJsonKey));
    t_Is_Dry_Run := NVL(tJson_Obj.get_Boolean(cDryRunKey), FALSE);

    IF NOT VALIDATE_TABLE_EXIST(tTarget_Table) THEN
        IF t_Is_Dry_Run THEN
            t_Table_Mock := TRUE; 
        ELSE
            RAISE_APPLICATION_ERROR(-20001, 'Target table ' || COALESCE(tTarget_Table, '<null>') || ' does not exist or schema is unavailable.');
        END IF;
    END IF;

    PREPROCESS_JSON_STREAM(tTarget_Table, t_Table_Mock, tRefresh_By_Json, tJson_Obj,
                           t_Type_Dict, t_Ctrl_For_Del, t_Ctrl_For_Ins, t_Rows_Array);

    BUILD_DML_CLAUSES(tTarget_Table, t_Ctrl_For_Del, t_Ctrl_For_Ins, t_Type_Dict, 
                      tDelete_SQL, tInsert_SQL);

    IF t_Rows_Array IS NOT NULL THEN
        tJson_Keys := tJson_Obj.get_keys; 
        tUpd_Col_Count := tJson_Keys.COUNT;
        
        FOR r IN 0 .. t_Rows_Array.get_size() - 1 LOOP
            IF t_Row_Obj IS NULL THEN
                t_Row_Obj := JSON_OBJECT_T(); 
            ELSE
                t_Row_Obj := t_Row_Obj.clone; 
            END IF;
            
            FOR k IN 1 .. tUpd_Col_Count LOOP
                tKey_Name := tJson_Keys(k);   
                tCol_Name := UPPER(tKey_Name); 
                t_Row_Obj.put(tCol_Name, tJson_Obj.get_array(tKey_Name).get(r));
            END LOOP;
            
            t_Rows_Array.put(r, t_Row_Obj, TRUE);
        END LOOP;
        tTransposed_Json := t_Rows_Array.to_clob;
    END IF;

    IF NOT t_Is_Dry_Run THEN
        IF tDelete_SQL IS NOT NULL THEN
            EXECUTE IMMEDIATE tDelete_SQL USING IN tRefresh_By_Json, OUT tDel_Row_Count;
        END IF;
        
        IF tInsert_SQL IS NOT NULL THEN
            EXECUTE IMMEDIATE tInsert_SQL USING IN tRefresh_By_Json, IN tTransposed_Json, OUT tIns_Row_Count;
        END IF;
    END IF;

    UPDATE EDS.UTIL_SYS_RFSH_TABLE_LOG l
    SET l.TARGET_TABLE    = tTarget_Table, 
        l.REFRESH_BY_DICT = tRefresh_By_Json, 
        l.UPDATE_COLUMNS  = tUpd_Col_Count, 
        l.DELETED_ROWS    = tDel_Row_Count, 
        l.INSERTED_ROWS   = tIns_Row_Count, 
        l.DELETE_SCRIPT   = tDelete_SQL,
        l.INSERT_SCRIPT   = tInsert_SQL, 
        l.UPDATED_TIME    = SYSTIMESTAMP
    WHERE l.LOG_ID = tLog_Id;
    COMMIT;

  EXCEPTION 
    WHEN OTHERS THEN
      tError_Msg := SUBSTR(SQLERRM, 1, 4000); 
      ROLLBACK;
      
      UPDATE EDS.UTIL_SYS_RFSH_TABLE_LOG l
      SET l.TARGET_TABLE    = tTarget_Table, 
          l.REFRESH_BY_DICT = tRefresh_By_Json, 
          l.UPDATE_COLUMNS  = tUpd_Col_Count, 
          l.DELETED_ROWS    = tDel_Row_Count, 
          l.INSERTED_ROWS   = tIns_Row_Count, 
          l.DELETE_SCRIPT   = tDelete_SQL,
          l.INSERT_SCRIPT   = tInsert_SQL, 
          l.ERROR_MSG       = tError_Msg, 
          l.UPDATED_TIME    = SYSTIMESTAMP
      WHERE l.LOG_ID = tLog_Id;
      COMMIT; 
      RAISE;
  END RECEIVE_TABLE;


PROCEDURE ARCHIVE_REFRESH_LOG
AS
tLast_Compl   TIMESTAMP(3);
tTimeout_Date TIMESTAMP(3)  := SYSTIMESTAMP - INTERVAL '7' DAY;
BEGIN
    SELECT  MAX(l.UPDATED_TIME) INTO tLast_Compl
    FROM    EDS.UTIL_SYS_RFSH_TABLE_LOG   l
    WHERE   l.UPDATED_TIME  IS NOT NULL;

    IF tLast_Compl IS NULL THEN
        RETURN;
    END IF;

    INSERT INTO EDS.UTIL_SYS_RFSH_TABLE_LOG_HIST (
        LOG_ID,
        RECEIVED_TIME,
        PAYLOAD_JSON,
        TARGET_TABLE,
        REFRESH_BY_DICT,
        UPDATE_COLUMNS,
        DELETE_SCRIPT,
        DELETED_ROWS,
        INSERT_SCRIPT,
        INSERTED_ROWS,
        ERROR_MSG,
        UPDATED_TIME
    )
    SELECT
        l.LOG_ID,
        l.RECEIVED_TIME,
        l.PAYLOAD_JSON,
        l.TARGET_TABLE,
        l.REFRESH_BY_DICT,
        l.UPDATE_COLUMNS,
        l.DELETE_SCRIPT,
        l.DELETED_ROWS,
        l.INSERT_SCRIPT,
        l.INSERTED_ROWS,
        NVL2(l.UPDATED_TIME, l.ERROR_MSG, NVL(l.ERROR_MSG, 'Timed out.')),
        l.UPDATED_TIME
    FROM
        EDS.UTIL_SYS_RFSH_TABLE_LOG l
    WHERE
        l.UPDATED_TIME  <= tLast_Compl
        OR l.RECEIVED_TIME < tTimeout_Date;

    IF SQL%FOUND THEN
        DELETE FROM EDS.UTIL_SYS_RFSH_TABLE_LOG l
        WHERE  l.UPDATED_TIME  <= tLast_Compl
            OR l.RECEIVED_TIME < tTimeout_Date;

        COMMIT;
    END IF;
END ARCHIVE_REFRESH_LOG;


END UTIL_SYS_REFRESH_TABLE_MAIN;
/
