CREATE OR REPLACE PROCEDURE STG.UTIL_SYS_REFRESH_TABLE_RECEIVE(JSON_PACKET STRING)
RETURNS VARCHAR
LANGUAGE SQL
AS
DECLARE
  cTargetTableKey   STRING := '_TARGET_TABLE_';
  cRefreshByJsonKey STRING := '_REFRESH_BY_JSON_';
  cDryRunKey        STRING := '_DRY_RUN_';
  cLF               STRING := CHAR(10);
  eInvalid_Input    EXCEPTION (-20001, 'Invalid refresh table input.');
  vLogId            NUMBER(10, 0);
  vTargetTable      STRING := '';
  vRefreshByJson    STRING := '';
  vIsDryRun         BOOLEAN := FALSE;
  vPayloadObj       VARIANT;
  vRefreshObj       VARIANT;
  vTargetMetaDict   VARIANT := OBJECT_CONSTRUCT(); 
  vKeyArray         ARRAY;   
  vRfshKeyArray     ARRAY;   
  vMap_UpperToOrig  VARIANT;
  vObjC             VARIANT;
  vObjD             VARIANT;
  vInsertObj        VARIANT;
  vKeyStr           STRING := ''; 
  vColStr           STRING := ''; 
  vBaselineKey      STRING := '';
  vDeleteSQL        STRING := '';
  vInsertSQL        STRING := '';
  vInsertCols       STRING := '';
  vInsertVals       STRING := '';
  vRfshKeyCount     NUMBER(9, 0) := 0; 
  vUpdColCount      NUMBER(9, 0) := 0; 
  vDelRowCount      NUMBER(9, 0) := 0;
  vInsRowCount      NUMBER(9, 0) := 0;
  vErrorMsg         STRING := '';
BEGIN
  -- [STEP 1]: IDENTITY ANCHORAGE
  SELECT STG.UTIL_SYS_REFRESH_LOG_ID_SEQUENCE.NEXTVAL INTO :vLogId;
  INSERT INTO STG.UTIL_SYS_REFRESH_TABLE_LOG (LOG_ID, PAYLOAD_JSON) VALUES (:vLogId, :JSON_PACKET);
  COMMIT;

  vPayloadObj := TRY_PARSE_JSON(JSON_PACKET);
  IF (vPayloadObj IS NULL OR TYPEOF(vPayloadObj) <> 'OBJECT') THEN
      vErrorMsg := 'json_packet must be a valid JSON object string.';
      RAISE eInvalid_Input;
  END IF;

  vTargetTable   := UPPER(TRIM(vPayloadObj[cTargetTableKey]));
  vRefreshByJson := TRIM(vPayloadObj[cRefreshByJsonKey]);
  vIsDryRun      := NVL(vPayloadObj[cDryRunKey]::BOOLEAN, FALSE);

  IF (vTargetTable = '' OR NOT REGEXP_LIKE(vTargetTable, '^[A-Z][A-Z0-9_$# *]*(\.[A-Z][A-Z0-9_$# *]*){1,2}$')) THEN
      vErrorMsg := cTargetTableKey || ' must be a valid schema-qualified table name.';
      RAISE eInvalid_Input;
  END IF;

  -- [STEP 2]: FAIL-FAST METADATA PROBE GUARD & RECONCILED ADAPTIVE DRY-RUN FALLBACK
  BEGIN
      EXECUTE IMMEDIATE 'DESC TABLE ' || vTargetTable;
      SELECT OBJECT_AGG(UPPER(name), type::VARIANT) INTO :vTargetMetaDict 
      FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
      WHERE kind = 'COLUMN';
  EXCEPTION
      WHEN OTHER THEN
          IF (NOT vIsDryRun) THEN
              vErrorMsg := 'Target table ' || COALESCE(vTargetTable, '<null>') || ' does not exist.';
              RAISE eInvalid_Input;
          END IF;
          vTargetMetaDict := NULL; 
  END;

  -- [STEP 3]: CONTROL OBJECT INITIAL READ & PURGE-STAGE SCRIPT READINESS
  vRefreshObj := TRY_PARSE_JSON(vRefreshByJson);
  IF (vRefreshObj IS NULL OR TYPEOF(vRefreshObj) <> 'OBJECT') THEN
      vErrorMsg := cRefreshByJsonKey || ' must be a valid JSON object string. Use {} for full refresh.';
      RAISE eInvalid_Input;
  END IF;

  -- [STEP 4]: PARAMETERIZED DELETE SCRIPT GENERATION & GHOST WHOLE-TABLE WIPE GUARD
  vRfshKeyArray := OBJECT_KEYS(vRefreshObj);
  vRfshKeyCount := ARRAY_SIZE(vRfshKeyArray);
  IF (vRfshKeyCount = 0) THEN
      vDeleteSQL := 'DELETE FROM ' || vTargetTable;
  ELSE
      SELECT LISTAGG(UPPER(c.KEY) || ' IS NOT DISTINCT FROM p.rfsh_obj:' || c.KEY, '
  AND '), COUNT(*)
      INTO :vDeleteSQL, :vUpdColCount
      FROM TABLE(FLATTEN(INPUT => :vRefreshObj)) c
      WHERE :vTargetMetaDict IS NULL OR :vTargetMetaDict[UPPER(c.KEY)] IS NOT NULL;

      IF (vUpdColCount = 0) THEN
          vErrorMsg := 'CRITICAL DATA SAFETY BREACH INTERCEPT: All control columns were purged as noise artifacts by target table schemas shield.';
          RAISE eInvalid_Input;
      END IF;

      vDeleteSQL := 'DELETE FROM ' || vTargetTable || CHAR(10) 
                 || 'USING (SELECT TO_OBJECT(:1::VARIANT) AS rfsh_obj) p' || CHAR(10) 
                 || 'WHERE ' || vDeleteSQL;
  END IF;

  -- [STEP 5]: ONE-PASS DUAL-HASH INVERSION LOOP (THE IMMUTABLE REGISTERS LIFECYCLE)
  vKeyArray := OBJECT_KEYS(vPayloadObj);
  vMap_UpperToOrig := OBJECT_CONSTRUCT();
  FOR i IN 0 TO (ARRAY_SIZE(vKeyArray) - 1) DO
      vKeyStr := vKeyArray[i];
      IF (TYPEOF(vPayloadObj[vKeyStr]) = 'ARRAY') THEN
          vBaselineKey := UPPER(vKeyStr);
          IF (vTargetMetaDict IS NULL OR vTargetMetaDict[vBaselineKey] IS NOT NULL) THEN
              vMap_UpperToOrig := OBJECT_INSERT(vMap_UpperToOrig, vBaselineKey, vKeyStr, true);
          END IF;
      END IF;
  END FOR;

  vKeyArray := OBJECT_KEYS(vMap_UpperToOrig); 
  vUpdColCount := ARRAY_SIZE(vKeyArray);       

  vObjD := OBJECT_CONSTRUCT();
  FOR i IN 0 TO (ARRAY_SIZE(vKeyArray) - 1) DO
      vKeyStr := vKeyArray[i]; 
      vObjD := OBJECT_INSERT(vObjD, vKeyStr, vPayloadObj[vMap_UpperToOrig[vKeyStr]]);
  END FOR;

  IF (vBaselineKey <> '') THEN
      vInsRowCount := NVL(ARRAY_SIZE(vObjD[vBaselineKey]), 0);
  END IF;

  vObjC := OBJECT_CONSTRUCT();
  FOR i IN 0 TO (vRfshKeyCount - 1) DO
      vKeyStr := vRfshKeyArray[i]; 
      vColStr := UPPER(vKeyStr);
      IF (vMap_UpperToOrig[vColStr] IS NULL) THEN
          IF (vTargetMetaDict IS NULL OR vTargetMetaDict[vColStr] IS NOT NULL) THEN
              vObjC := OBJECT_INSERT(vObjC, vColStr, vRefreshObj[vKeyStr], true);
          END IF;
      END IF;
  END FOR;

  vInsertObj := OBJECT_CONSTRUCT('c', vObjC, 'd', vObjD);

  -- [STEP 6]: COLUMN MATRIX GENERATION via PIPELINE DUAL-TRACK LISTAGG CONDUIT
  SELECT LISTAGG(ALL_COLS.COL_NAME, ', '), LISTAGG(ALL_COLS.VAL_PROJECTION, ',
    ')
  INTO :vInsertCols, :vInsertVals
  FROM (
      SELECT c.KEY AS COL_NAME, 'p.ins_obj:c.' || c.KEY AS VAL_PROJECTION, c.INDEX AS SORT_ORDINAL, 1 AS IS_CTRL FROM TABLE(FLATTEN(INPUT => :vInsertObj:c)) c
      UNION ALL
      SELECT d.KEY AS COL_NAME, IFF(d.KEY = :vBaselineKey, 'b.VALUE', 'p.ins_obj:d.' || d.KEY || '[b.INDEX]') AS VAL_PROJECTION, d.INDEX AS SORT_ORDINAL, 0 AS IS_CTRL FROM TABLE(FLATTEN(INPUT => :vInsertObj:d)) d
  ) ALL_COLS ORDER BY ALL_COLS.IS_CTRL DESC, ALL_COLS.SORT_ORDINAL;

  -- [STEP 7]: HIGH-PERFORMANCE INSERT SCRIPT ASSEMBLY (THE 'b' BASELINE RECONCILIATION)
  IF (vInsertCols <> '') THEN
      IF (vInsRowCount > 0) THEN
          vInsertSQL := 'INSERT INTO ' || vTargetTable || ' (' || vInsertCols || ')' || cLF 
                     || 'SELECT ' || cLF || '    ' || vInsertVals || cLF 
                     || 'FROM (SELECT TO_OBJECT(:1::VARIANT) AS ins_obj) p, ' || cLF
                     || '     TABLE(FLATTEN(INPUT => p.ins_obj:d.' || vBaselineKey || ')) b';
      ELSE
          vErrorMsg := 'NO ROWS INTERSECTED IN SOURCE VECTOR MATRIX (EMPTY WIPE REFLECTED)';
          RAISE eInvalid_Input;
      END IF;
  ELSE
      vErrorMsg := 'INSERT SQL GENERATION SEVERE ERROR: NO COLUMNS TRANSLATED';
      RAISE eInvalid_Input;
  END IF;

  -- [STEP 8]: ATOMIC SECURE SUBMISSION (CLEAN SHIPPED POSITION BINDING TELEMETRY RUN)
  IF (NOT vIsDryRun) THEN
      IF (vDeleteSQL <> '') THEN
          EXECUTE IMMEDIATE :vDeleteSQL USING (vRefreshObj);
          SELECT $1 INTO :vDelRowCount FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
      END IF;
      
      IF (vInsertSQL <> '') THEN
          EXECUTE IMMEDIATE :vInsertSQL USING (vInsertObj);
          SELECT $1 INTO :vInsRowCount FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
      END IF;
  END IF;

  -- [STEP 9]: FINAL SUCCESS AUDIT REGISTRATION UPDATE PIPELINE
  UPDATE STG.UTIL_SYS_REFRESH_TABLE_LOG
  SET TARGET_TABLE    = :vTargetTable, 
      REFRESH_BY_DICT = :vRefreshObj, 
      UPDATE_COLUMNS  = :vUpdColCount, 
      DELETE_SCRIPT   = :vDeleteSQL, 
      DELETED_ROWS    = :vDelRowCount, 
      INSERT_SCRIPT   = :vInsertSQL, 
      INSERTED_ROWS   = :vInsRowCount, 
      ERROR_MSG       = :vErrorMsg, 
      UPDATED_TIME    = CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(3))
  WHERE LOG_ID = :vLogId;

  return IFF(vIsDryRun, 'DRY_RUN', 'SUCCESS');

EXCEPTION
  WHEN OTHER THEN
    IF (vErrorMsg = '') THEN
        vErrorMsg := :SQLERRM;
    END IF;

    UPDATE STG.UTIL_SYS_REFRESH_TABLE_LOG
    SET TARGET_TABLE    = :vTargetTable, 
        REFRESH_BY_DICT = :vRefreshObj, 
        UPDATE_COLUMNS  = :vUpdColCount, 
        DELETE_SCRIPT   = :vDeleteSQL, 
        DELETED_ROWS    = :vDelRowCount, 
        INSERT_SCRIPT   = :vInsertSQL, 
        INSERTED_ROWS   = :vInsRowCount, 
        ERROR_MSG       = :vErrorMsg, 
        UPDATED_TIME    = CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(3))
    WHERE LOG_ID = :vLogId;
    RAISE;
END;
