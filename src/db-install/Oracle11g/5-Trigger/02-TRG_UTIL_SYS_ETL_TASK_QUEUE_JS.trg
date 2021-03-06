CREATE OR REPLACE TRIGGER {Schema}.TRG_UTIL_SYS_ETL_TASK_QUEUE_JS
BEFORE INSERT OR UPDATE OF EXTRACT_PARAMS, FIELD_MAPPING, MERGE_PARAMS ON {Schema}.UTIL_SYS_ETL_TASK_QUEUE
FOR EACH ROW
BEGIN
    :new.EXTRACT_PARAMS     := TRIM(:new.EXTRACT_PARAMS);

    IF :new.EXTRACT_PARAMS IS NULL THEN
        :new.EXTRACT_PARAMS := '{}';
    ELSIF SUBSTR(:new.EXTRACT_PARAMS, 1, 1) != '{' OR SUBSTR(:new.EXTRACT_PARAMS, -1, 1) != '}' THEN
        RAISE_APPLICATION_ERROR(-20088, 'Entering EXTRACT_PARAMS is not a valid JSON object.');
    END IF;

    :new.FIELD_MAPPING      := TRIM(:new.FIELD_MAPPING);

    IF :new.FIELD_MAPPING IS NULL THEN
        :new.FIELD_MAPPING := '{}';
    ELSIF SUBSTR(:new.FIELD_MAPPING, 1, 1) != '{' OR SUBSTR(:new.FIELD_MAPPING, -1, 1) != '}' THEN
        RAISE_APPLICATION_ERROR(-20088, 'Entering FIELD_MAPPING is not a valid JSON object.');
    END IF;

    :new.MERGE_PARAMS       := TRIM(:new.MERGE_PARAMS);

    IF :new.MERGE_PARAMS IS NULL THEN
        :new.MERGE_PARAMS := '{}';
    ELSIF SUBSTR(:new.MERGE_PARAMS, 1, 1) != '{' OR SUBSTR(:new.MERGE_PARAMS, -1, 1) != '}' THEN
        RAISE_APPLICATION_ERROR(-20088, 'Entering MERGE_PARAMS is not a valid JSON object.');
    END IF;

END TRG_UTIL_SYS_ETL_TASK_QUEUE_JS;
/
