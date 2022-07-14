using System;
using System.Configuration;

namespace DataBooster.Simple_ETL_Spooler
{
    public static class ConfigHelper
    {
        private const string _serverSettingKey = "MainDB";
        private const string _packageSettingKey = "MainPackage";
        private const string _intervalSecondsSettingKey = "IntervalSeconds";
        private const string _stopSerialOnErrorSettingKey = "StopSerialOnError";

        private static readonly string _mainDB;
        private static readonly string _mainPackage;
        private static readonly int _intervalMilliseconds;
        private static readonly bool _stopSerialOnError;

        static ConfigHelper()
        {
            _mainDB = ConfigurationManager.AppSettings[_serverSettingKey];
            _mainPackage = ConfigurationManager.AppSettings[_packageSettingKey];
            _intervalMilliseconds = int.Parse(ConfigurationManager.AppSettings[_intervalSecondsSettingKey] ?? "10") * 1000;
            _stopSerialOnError = bool.Parse(ConfigurationManager.AppSettings[_stopSerialOnErrorSettingKey] ?? "false");

            if (string.IsNullOrWhiteSpace(_mainDB))
                throw new MissingFieldException($"{_serverSettingKey} is missing from appSettings of App.config.");
            if (string.IsNullOrWhiteSpace(_packageSettingKey))
                throw new MissingFieldException($"{_packageSettingKey} is missing from appSettings of App.config.");
        }

        public static string MainDB => _mainDB;
        public static string MainPackage => _mainPackage;
        public static int IntervalMilliseconds => _intervalMilliseconds;
        public static bool StopSerialOnError => _stopSerialOnError;
    }
}
