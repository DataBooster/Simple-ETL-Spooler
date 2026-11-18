// This code requires to be customized.

using System;
using System.Data.Common;
using System.Text.RegularExpressions;

namespace DataBooster.PsInvokeDb
{
    public static class Customizer
    {
        private static readonly Regex _regexLiteralServerName = new Regex(@"^\s*\w+\s*$");
        private static Regex _regexOracleServerName = new Regex(@"^ED\w+$", RegexOptions.IgnoreCase);

        public static string OracleServerNamePattern
        {
            get => _regexOracleServerName.ToString();
            set
            {
                if (!string.IsNullOrWhiteSpace(value) && value != _regexOracleServerName.ToString())
                {
                    try
                    {
                        _regexOracleServerName = new Regex(value, RegexOptions.IgnoreCase);
                    }
                    catch (ArgumentException ex)
                    {
                        throw new ArgumentException($"Invalid regular expression pattern for Oracle server name setting: {value}", nameof(OracleServerNamePattern), ex);
                    }
                }
            }
        }

        public static bool IsOracle(string dbServerName)
        {
            // TODO: Determine whether the database server is Oracle (or SQL Server) based on your organization's database server naming convention or a detailed list.
            // throw new NotImplementedException(@"The end product requires you to implement your custom code (src\PsInvokeDb\Customizer.cs) to determine whether the database server you enter is Oracle (otherwise SQL Server) based on your organization's database server naming convention or a detailed list.");
            // For example:
            if (dbServerName == null)
                throw new ArgumentNullException(nameof(dbServerName));

            return _regexOracleServerName.IsMatch(dbServerName);
        }

        public static (DbProviderFactory ProviderFactory, string ConnectionString) GetDbProvider(string dbServerName)
        {
            DbProviderFactory dbProviderFactory;
            string connectionString;

            if (IsOracle(dbServerName))
            {
                dbProviderFactory = DbProviderFactories.GetFactory("Oracle.DataAccess.Client");
                connectionString = $"Data Source={dbServerName};User Id=/";
            }
            else if (_regexLiteralServerName.IsMatch(dbServerName))
            {
                dbProviderFactory = DbProviderFactories.GetFactory("System.Data.SqlClient");
                connectionString = $"Data Source={dbServerName};Integrated Security=SSPI";
            }
            else
            {
                throw new ArgumentException($"Unsupported database server: {dbServerName}", nameof(dbServerName));
            }

            return (dbProviderFactory, connectionString);
        }
    }
}
