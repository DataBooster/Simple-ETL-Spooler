using System;
using System.Collections.Concurrent;

namespace DataBooster.Simple_ETL_Spooler.InvokeSnowflake
{
    public static class SnowflakeConfig
    {
        private struct CacheConnectionString
        {
            public string ConnectionString { get; set; }
            public DateTime Expiration { get; set; }
        }

        private static TimeSpan _cacheExpiration = TimeSpan.FromHours(12);
        private static ConcurrentDictionary<string, CacheConnectionString> _connectionStringCache = new ConcurrentDictionary<string, CacheConnectionString>(StringComparer.OrdinalIgnoreCase);

        private static string GetCacheConnString(string strCacheKey)
        {
            if (_connectionStringCache.TryGetValue(strCacheKey, out CacheConnectionString cacheConnString))
            {
                if (cacheConnString.Expiration > DateTime.Now)
                    return cacheConnString.ConnectionString;
            }
            return null;
        }

        private static void SetCacheConnString(string strCacheKey, string connectionString)
        {
            var cacheConnString = new CacheConnectionString
            {
                ConnectionString = connectionString,
                Expiration = DateTime.Now.Add(_cacheExpiration)
            };
            _connectionStringCache.AddOrUpdate(strCacheKey, cacheConnString, (key, oldValue) => cacheConnString);
        }

        // This code requires to be customized.
        #region This region is for a rough illustration; you will need to modify the interface and implementation according to your actual environment.

        public static string GetConnectionString(string confServer, string strTplKey, string strOvrParams = "{}", bool fresh = false)
        {
            string strCacheKey = strTplKey + strOvrParams;
            string strConnectionString = GetCacheConnString(strCacheKey);
            if (string.IsNullOrWhiteSpace(strConnectionString) || fresh)
            {
                var (connStringTemplate, secureVaultSettings) = GetSettingsFromConf(confServer, strTplKey, strOvrParams);
                string snowflakePAT = GetSnowflakePatFromVault(secureVaultSettings);

                strConnectionString = string.Format(connStringTemplate, snowflakePAT);
                SetCacheConnString(strCacheKey, strConnectionString);
            }
            return strConnectionString;
        }

        private static (string connStringTemplate, string secureVaultSettings) GetSettingsFromConf(string confServer, string strTplKey, string strOvrParams)
        {
            // Implement the logic to retrieve the Snowflake connection string template and secure vault settings (for retrieve your Snowflake PAT) from your configuration server using the strTplKey and strOvrParams
            throw new NotImplementedException();
        }

        private static string GetSnowflakePatFromVault(string secureVaultSettings)
        {
            // Implement the logic to retrieve the Snowflake PAT from your secure vault
            throw new NotImplementedException();
        }

        #endregion

        private static TimeSpan _retryDelay = TimeSpan.FromSeconds(10); // Wait for 10 second before retrying
        public static TimeSpan RetryDelay
        {
            get => _retryDelay;
            set
            {
                if (value > TimeSpan.FromSeconds(1))
                    _retryDelay = value;
            }
        }

        public static TimeSpan CacheExpiration
        {
            get => _cacheExpiration;
            set
            {
                if (value > TimeSpan.FromHours(1))
                    _cacheExpiration = value;
            }
        }
    }
}
