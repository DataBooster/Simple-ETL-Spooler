﻿// This code requires to be customized.

using System;
using System.Data.Common;

namespace DataBooster.PsInvokeDb
{
    public static class Customizer
    {
        public static bool IsOracle(string dbServerName)
        {
            // TODO: Determine whether the database server is Oracle (or SQL Server) based on your organization's database server naming convention or a detailed list.
            throw new NotImplementedException(@"The end product requires you to implement your custom code (src\PsInvokeDb\Customizer.cs) to determine whether the database server you enter is Oracle (otherwise SQL Server) based on your organization's database server naming convention or a detailed list.");
            // For example:
            // return dbServerName.StartsWith("ED", StringComparison.OrdinalIgnoreCase);
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
            else
            {
                dbProviderFactory = DbProviderFactories.GetFactory("System.Data.SqlClient");
                connectionString = $"Data Source={dbServerName};Integrated Security=SSPI";
            }

            return (dbProviderFactory, connectionString);
        }
    }
}
