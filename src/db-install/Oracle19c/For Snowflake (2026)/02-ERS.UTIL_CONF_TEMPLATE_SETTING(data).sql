DELETE FROM ERS.UTIL_CONF_TEMPLATE_SETTING t WHERE t.TEMPLATE_KEY IN ('SNOWFLAKE.EDW_DB', 'SNOWFLAKE.RISK_DB');

insert into ERS.UTIL_CONF_TEMPLATE_SETTING (TEMPLATE_KEY, TEMPLATE_TEXT, COMMENT_)
values ('SNOWFLAKE.EDW_DB', '{
    "ConnectionString": "account=oy84163.canada-central.privatelink;user=[[SNOWFLAKE_RISK_USER]];warehouse=[[SNOWFLAKE_RISK_COMPUTING_WAREHOUSE]];db=[[SNOWFLAKE_EDW_DB]];maxPoolSize=32;password=\"{0}\";",
    "ConnectionUser": "[[SNOWFLAKE_RISK_USER]]",
    "CyberArk.Endpoint": "[[CYBERARK.ENDPOINT]]",
    "CyberArk.AppID": "[[CYBERARK.TF_APPID]]",
    "CyberArk.Entry": "[[SNOWFLAKE_RISK_USER]]@oy84163.canada-central.privatelink.snowflakecomputing.com"
}', null);

insert into ERS.UTIL_CONF_TEMPLATE_SETTING (TEMPLATE_KEY, TEMPLATE_TEXT, COMMENT_)
values ('SNOWFLAKE.RISK_DB', '{
    "ConnectionString": "account=oy84163.canada-central.privatelink;user=[[SNOWFLAKE_RISK_USER]];warehouse=[[SNOWFLAKE_RISK_COMPUTING_WAREHOUSE]];db=[[SNOWFLAKE_RISK_DB]];password=\"{0}\";",
    "ConnectionUser": "[[SNOWFLAKE_RISK_USER]]",
    "CyberArk.Endpoint": "[[CYBERARK.ENDPOINT]]",
    "CyberArk.AppID": "[[CYBERARK.TF_APPID]]",
    "CyberArk.Entry": "[[SNOWFLAKE_RISK_USER]]@oy84163.canada-central.privatelink.snowflakecomputing.com"
}', null);

COMMIT;
