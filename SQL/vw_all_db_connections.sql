create or replace view TABLEAU.TABLEAU_REST.VW_ALL_DB_CONNECTIONS(
	"Site_LUID",
	"Item_LUID",
	"Item_Type",
	"Item_Name",
	"Connection_LUID",
	"Connection_Type",
	"Server_Name",
	"Server_Port",
	"User_Name",
	"Database_Name",
	"Embed_Password",
	"Admin_Insights_Published_At"
) as 

SELECT "Site_LUID", "Item_LUID", "Item_Type", "Item_Name", "Connection_LUID", "Connection_Type", "Server_Name", "Server_Port", "User_Name", "Database_Name", "Embed_Password"
, MAX("Admin_Insights_Published_At") AS "Admin_Insights_Published_At"
FROM 
(
SELECT "Site_LUID", "LUID" AS "Item_LUID", 'datasource' AS "Item_Type", "Datasource_Name" AS "Item_Name", "Connection_LUID", "Connection_Type", "Server_Name", "Server_Port", "User_Name", "Database_Name", "Embed_Password", "Admin_Insights_Published_At"
FROM TABLEAU.TABLEAU_REST.DB_CONN_DATASOURCES
UNION 
SELECT "Site_LUID", "LUID" AS "Item_LUID", 'flow' AS "Item_Type", "Flow_Name" AS "Item_Name", "Connection_LUID", "Connection_Type", "Server_Name", "Server_Port", "User_Name", "Database_Name", "Embed_Password", "Admin_Insights_Published_At"
FROM TABLEAU.TABLEAU_REST.DB_CONN_FLOWS
UNION
SELECT "Site_LUID", "LUID" AS "Item_LUID", 'virtualconnection' AS "Item_Type", "Virtual_Connection_Name" AS "Item_Name", "Connection_LUID", "Connection_Type", "Server_Name", "Server_Port", "User_Name", "Database_Name", "Embed_Password", "Admin_Insights_Published_At"
FROM TABLEAU.TABLEAU_REST.DB_CONN_VIRTUAL_CONNECTIONS
UNION
SELECT "Site_LUID", "LUID" AS "Item_LUID", 'workbook' AS "Item_Type", "Workbook_Name" AS "Item_Name", "Connection_LUID", "Connection_Type", "Server_Name", "Server_Port", "User_Name", "Database_Name", "Embed_Password", "Admin_Insights_Published_At"
FROM TABLEAU.TABLEAU_REST.DB_CONN_WORKBOOKS
)
GROUP BY "Site_LUID", "Item_LUID", "Item_Type", "Item_Name", "Connection_LUID", "Connection_Type", "Server_Name", "Server_Port", "User_Name", "Database_Name", "Embed_Password";
