# Simple ETL Spooler
Simple ETL Task Spooler Service

To simplify the massive data movement within the enterprise, the Simple ETL Spooler provides a high-throughput, easily scalable generic product for data movement between Oracle, SQL Server, OLAP and RESTful data sources. It works like a Print Spooler Server, monitoring the task instructions in the queue submitted by clients, and organizing them into corresponding serial or parallel batches for execution.

The ETL task queue is presented as a database table, each row is a specific ETL task instruction, and each task instruction specifies the extraction source and load destination information:

1. Extraction Source Parts
   - Extract_Type

     Can be one of the following types *(currently supported)*:

     - 'SP': Stored Procedure
     - 'SQL': Dynamic selecy query
     - 'MDX': MDX query
     - 'REST': RESTful service call

   - Extract_Source

     The database server(instance) name of 'SP'/'SQL', or the full connection string of 'MDX', or the URL of 'REST';

   - Extract_Command

     - For 'SP': The fully qualified name of the stored procedure;
     - For 'SQL'/'MDX': A complete dynamic query statement;
     - For 'REST': A complete JSON string as the HTTP body content to be sent to the RESTful service;

   - Extract_Params (Optional)

     - For 'SP': A valid JSON string containing all input parameters to be passed to the stored procedure;
     - For 'REST': A valid JSON string containing any custom HTTP headers (name-value pairs) - if need;

   - Extract_Timeout (Optional)

     The time in seconds to wait for the extraction command to execute;

2. Load Destination Parts
   - Load_Type

     *(currently only supported)*:

     - 'SP': Stored Procedure *(Oracle now)*

   - Field_Mapping (Optional)

     A valid JSON string specifying some special name mapping between source columns and destination input parameters.
     All fields without specified custom name-mapping, will continue to be automatically matched by name     
     (the **column name** in the extraction result set **-->** the **parameter name** in the load stored procedure);

   - Merge_Params (Optional)

     Any additional input parameters required by the destination SP can be entered in this JSON;

   - Load_Destination

     The destination database server(instance) name;

   - Load_Command

     The fully qualified name of the stored procedure for loading;

   - Load_Timeout

     The time in seconds to wait for the loading command to execute;

