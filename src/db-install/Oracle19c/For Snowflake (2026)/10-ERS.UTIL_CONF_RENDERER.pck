CREATE OR REPLACE PACKAGE ERS.UTIL_CONF_RENDERER IS

g_Cur_Edb   CONSTANT VARCHAR2(30)   := SYS_CONTEXT('USERENV', 'DB_NAME');


FUNCTION GET_CURRENT_ENV
(
    inEDB_DB_NAME   VARCHAR2    := g_Cur_Edb
)    RETURN VARCHAR2;


FUNCTION GET_ENV_SPECIFIC_SETTING
(
    inSetting_Key   VARCHAR2,
    inEnv_Key       VARCHAR2    := NULL,
    inDefault_Value VARCHAR2    := NULL
)   RETURN VARCHAR2;


FUNCTION RENDER_ENV_PLACEHOLDER
(
    inMarkup    VARCHAR2,
    inEnv_Key   VARCHAR2        := NULL
)   RETURN VARCHAR2;


FUNCTION RENDER_TEMPLATE_SETTING
(
    inTemplate_Key      VARCHAR2,
    inExternal_Params   VARCHAR2  := NULL
)   RETURN VARCHAR2;


END UTIL_CONF_RENDERER;
/
CREATE OR REPLACE PACKAGE BODY ERS.UTIL_CONF_RENDERER IS


c_Env_Key   CONSTANT VARCHAR2(30)   := ERS.UTIL_CONF_RENDERER.GET_CURRENT_ENV();


FUNCTION GET_CURRENT_ENV
(
    inEDB_DB_NAME   VARCHAR2    := g_Cur_Edb
)	RETURN VARCHAR2
AS
tEdb_Db_Name    VARCHAR2(30) := UPPER(TRIM(inEDB_DB_NAME));
tEnv_Key        VARCHAR2(30);
BEGIN
    SELECT m.ENV_KEY INTO tEnv_Key
    FROM ERS.UTIL_CONF_EDB_ENV_MAP m
    WHERE m.DB_NAME = tEdb_Db_Name;

    RETURN tEnv_Key;
EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RAISE_APPLICATION_ERROR(-20097, 'The ENV_NAME setting for ' || tEdb_Db_Name || ' is not found in ERS.UTIL_CONF_EDB_ENV_MAP.');
END GET_CURRENT_ENV;


FUNCTION GET_ENV_SPECIFIC_SETTING
(
    inSetting_Key   VARCHAR2,
    inEnv_Key       VARCHAR2    := NULL,
    inDefault_Value VARCHAR2    := NULL
)   RETURN VARCHAR2
AS
tSetting_Key    VARCHAR2(64)    := UPPER(TRIM(inSetting_Key));
tEnv_Key        VARCHAR2(30)    := NVL(UPPER(TRIM(inEnv_Key)), c_Env_Key);
tSetting_Value  VARCHAR2(4000);
BEGIN
    SELECT s.ITEM_VALUE INTO tSetting_Value
    FROM ERS.UTIL_CONF_ENV_SPECIFIC_SETTING s
    WHERE s.ITEM_KEY    = tSetting_Key
        AND s.ENV_KEY   = tEnv_Key;

    RETURN tSetting_Value;
EXCEPTION
      WHEN NO_DATA_FOUND THEN
        IF inDefault_Value IS NULL THEN
            RAISE_APPLICATION_ERROR(-20096, 'The ' || tSetting_Key || ' setting for ' || tEnv_Key || ' environment is not found in ERS.UTIL_CONF_ENV_SPECIFIC_SETTING.');
        ELSE
            RETURN inDefault_Value;
        END IF;
END GET_ENV_SPECIFIC_SETTING;


FUNCTION RENDER_ENV_PLACEHOLDER
(
    inMarkup    VARCHAR2,
    inEnv_Key   VARCHAR2    := NULL
)   RETURN VARCHAR2
AS
tEnv_Key        VARCHAR2(30)    := NVL(UPPER(TRIM(inEnv_Key)), c_Env_Key);
tNth_Appearance SIMPLE_INTEGER  := 1;
tPlaceholder    VARCHAR2(64);
tSetting_Key    VARCHAR2(60);
tRendering      VARCHAR2(4000)  := inMarkup;
tEnv_Specific_V VARCHAR2(4000);
BEGIN
    LOOP
        tPlaceholder    := REGEXP_SUBSTR(inMarkup, '\[\[\s*\w+(\.\w+)*\s*\]\]', 1, tNth_Appearance);
        EXIT WHEN tPlaceholder IS NULL;

        tSetting_Key    := RTRIM(SUBSTR(tPlaceholder, 3), ']]');
        tEnv_Specific_V := GET_ENV_SPECIFIC_SETTING(tSetting_Key, tEnv_Key);
        tRendering      := REPLACE(tRendering, tPlaceholder, tEnv_Specific_V);
        tNth_Appearance := tNth_Appearance + 1;
    END LOOP;
    RETURN tRendering;
END RENDER_ENV_PLACEHOLDER;


FUNCTION RENDER_TEMPLATE_SETTING
(
    inTemplate_Key      VARCHAR2,
    inExternal_Params   VARCHAR2  := NULL
)   RETURN VARCHAR2
AS
tMarkup     VARCHAR2(4000);
tParams     JSON_OBJECT_T;
tKeys       JSON_KEY_LIST;
tKey        VARCHAR2(1000);
BEGIN
    SELECT  t.TEMPLATE_TEXT INTO tMarkup
    FROM    ERS.UTIL_CONF_TEMPLATE_SETTING  t
    WHERE   t.TEMPLATE_KEY = inTemplate_Key;

    IF inExternal_Params IS NOT NULL THEN
        IF inExternal_Params IS JSON THEN
            tParams := JSON_OBJECT_T.parse(inExternal_Params);
            tKeys:= tParams.get_Keys;
            FOR i IN 1..tKeys.COUNT LOOP
                tKey := tKeys(i);
                tMarkup := REPLACE(tMarkup, '[[' || tKey || ']]', tParams.get_string(tKey));
            END LOOP;
        ELSE
            RAISE_APPLICATION_ERROR(-20094, 'The inExternal_Params input is not a valid JSON object.');
        END IF;
    END IF;

    RETURN RENDER_ENV_PLACEHOLDER(tMarkup);

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RAISE_APPLICATION_ERROR(-20096, 'The ' || inTemplate_Key || ' setting is not found in ERS.UTIL_CONF_TEMPLATE_SETTING.');
    WHEN OTHERS THEN
        RAISE;
END RENDER_TEMPLATE_SETTING;


END UTIL_CONF_RENDERER;
/
