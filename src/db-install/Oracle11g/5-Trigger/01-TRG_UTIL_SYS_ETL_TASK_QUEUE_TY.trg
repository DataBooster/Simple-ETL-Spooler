CREATE OR REPLACE TRIGGER {Schema}.TRG_UTIL_SYS_ETL_TASK_QUEUE_TY
BEFORE INSERT OR UPDATE OF EXTRACT_TYPE, NAMING_CONVENTION, LOAD_TYPE ON {Schema}.UTIL_SYS_ETL_TASK_QUEUE
FOR EACH ROW
BEGIN
    :new.EXTRACT_TYPE       := UPPER(TRIM(:new.EXTRACT_TYPE));
    :new.NAMING_CONVENTION  := UPPER(TRIM(:new.NAMING_CONVENTION));
    :new.LOAD_TYPE          := UPPER(TRIM(:new.LOAD_TYPE));

    IF :new.EXTRACT_TYPE = 'QUERY' OR :new.EXTRACT_TYPE = 'DYNAMIC' THEN
        :new.EXTRACT_TYPE := 'SQL';
    END IF;

    IF :new.EXTRACT_TYPE = 'HTTP' THEN
        :new.EXTRACT_TYPE := 'REST';
    END IF;

    IF :new.EXTRACT_TYPE = 'REST' THEN
        IF :new.EXTRACT_COMMAND IS NULL THEN
            :new.EXTRACT_COMMAND := ' ';
        END IF;
        IF :new.EXTRACT_PARAMS IS NULL OR :new.EXTRACT_PARAMS = '{}' THEN
            :new.EXTRACT_PARAMS := '{"Http-Method": "Post", "Content-Type": "application/json", "Accept": "application/json"}';
        END IF;
    END IF;
END TRG_UTIL_SYS_ETL_TASK_QUEUE_TY;
/
