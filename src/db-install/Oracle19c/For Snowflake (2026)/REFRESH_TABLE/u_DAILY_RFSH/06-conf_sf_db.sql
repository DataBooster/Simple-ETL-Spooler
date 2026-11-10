MERGE INTO ERS.UTIL_CONF_ENV_SPECIFIC_SETTING s
USING (
    SELECT
        jt.ITEM_KEY,
        jt.ENV_KEY,
        jt.ITEM_VALUE,
        'Snowflake ' || SUBSTR(jt.ITEM_KEY, 4) || ' for ' || jt.ENV_KEY AS COMMENT_
    FROM JSON_TABLE(q'![
        {
            "ITEM_KEY":"SF_MARKETDATA_DB",
            "ENV_SETTINGS":[
                {"ENV":"DEV01", "VALUE":"DEV_MARKETDATA_DB"},
                {"ENV":"DIT01", "VALUE":"CLN_MARKETDATA_DB"},
                {"ENV":"UAT01", "VALUE":"CLN_MARKETDATA_DB"},
                {"ENV":"SIT01", "VALUE":"CLN_MARKETDATA_DB"},
                {"ENV":"SIT02", "VALUE":"CLN_MARKETDATA_DB"},
                {"ENV":"SIT03", "VALUE":"CLN_MARKETDATA_DB"},
                {"ENV":"PROD",  "VALUE":"PRD_MARKETDATA_DB"}
            ]
        },
        {
            "ITEM_KEY":"SF_PERFORMANCE_DB",
            "ENV_SETTINGS":[
                {"ENV":"DEV01", "VALUE":"DEV_PERFORMANCE_DB"},
                {"ENV":"DIT01", "VALUE":"CLN_PERFORMANCE_DB"},
                {"ENV":"UAT01", "VALUE":"CLN_PERFORMANCE_DB"},
                {"ENV":"SIT01", "VALUE":"CLN_PERFORMANCE_DB"},
                {"ENV":"SIT02", "VALUE":"CLN_PERFORMANCE_DB"},
                {"ENV":"SIT03", "VALUE":"CLN_PERFORMANCE_DB"},
                {"ENV":"PROD",  "VALUE":"PRD_PERFORMANCE_DB"}
            ]
        }
    ]!', '$[*]'
    COLUMNS (
        ITEM_KEY    VARCHAR2(32)    PATH '$.ITEM_KEY',
        NESTED PATH '$.ENV_SETTINGS[*]'
        COLUMNS (
            ENV_KEY     VARCHAR2(8)     PATH '$.ENV',
            ITEM_VALUE  VARCHAR2(32)    PATH '$.VALUE'
        )
    )) jt
) u
ON (s.ENV_KEY = u.ENV_KEY AND s.ITEM_KEY = u.ITEM_KEY)
WHEN MATCHED THEN
    UPDATE SET
        s.ITEM_VALUE = u.ITEM_VALUE,
        s.COMMENT_ = u.COMMENT_
    WHERE
        s.ITEM_VALUE <> u.ITEM_VALUE
WHEN NOT MATCHED THEN
    INSERT (s.ENV_KEY, s.ITEM_KEY, s.ITEM_VALUE, s.COMMENT_)
    VALUES (u.ENV_KEY, u.ITEM_KEY, u.ITEM_VALUE, u.COMMENT_);

COMMIT;

--select * from ers.VIEW_CONF_ENV_SPECIFIC_SETTING t
--where t.SETTING_KEY like 'SF%';
