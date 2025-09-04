create or replace view TABLEAU.TABLEAU_REST.VW_ALL_DB_CONNECTION_DETAILS(
	"Item_LUID",
	"Item_Name",
	"Item_Type",
	"Table_LUID",
	"Table_Name",
	"Table_Schema",
	"Table_Full_Name",
	"Database_LUID",
	"Database_Name",
	"Database_Connection_Type",
	"Site_LUID",
	"Admin_Insights_Published_At"
) as 

SELECT "Item_LUID", "Item_Name", "Item_Type", "Table_LUID", "Table_Name", "Table_Schema", "Table_Full_Name", "Database_LUID", "Database_Name", "Database_Connection_Type", "Site_LUID"
, MAX("Admin_Insights_Published_At") AS "Admin_Insights_Published_At"
FROM (
SELECT "Datasource_LUID" AS "Item_LUID", "Datasource_Name" AS "Item_Name", 'datasource' AS "Item_Type"
, "Table_LUID", "Table_Name", "Table_Schema", "Table_Full_Name", "Database_LUID", "Database_Name", "Database_Connection_Type", "Admin_Insights_Published_At", "Site_LUID"
FROM TABLEAU.TABLEAU_REST.DB_CONN_DATASOURCE_DETAILS
UNION
SELECT "Flow_LUID" AS "Item_LUID", "Flow_Name" AS "Item_Name", 'flow' AS "Item_Type"
, "Table_LUID", "Table_Name", "Table_Schema", "Table_Full_Name", "Database_LUID", "Database_Name", "Database_Connection_Type", "Admin_Insights_Published_At", "Site_LUID"
FROM TABLEAU.TABLEAU_REST.DB_CONN_FLOW_DETAILS
UNION
SELECT "Virtual_Connection_LUID" AS "Item_LUID", "Virtual_Connection_Name" AS "Item_Name", 'virtualconnection' AS "Item_Type"
, "Table_LUID", "Table_Name", "Table_Schema", "Table_Full_Name", "Database_LUID", "Database_Name", "Database_Connection_Type", "Admin_Insights_Published_At", "Site_LUID"
FROM TABLEAU.TABLEAU_REST.DB_CONN_VIRTUAL_CONNECTION_DETAILS
UNION
SELECT "Workbook_LUID" AS "Item_LUID", "Workbook_Name" AS "Item_Name", 'workbook' AS "Item_Type"
, "Table_LUID", "Table_Name", "Table_Schema", "Table_Full_Name", "Database_LUID", "Database_Name", "Database_Connection_Type", "Admin_Insights_Published_At", "Site_LUID"
FROM TABLEAU.TABLEAU_REST.DB_CONN_WORKBOOK_DETAILS
) A
GROUP BY "Item_LUID", "Item_Name", "Item_Type", "Table_LUID", "Table_Name", "Table_Schema", "Table_Full_Name", "Database_LUID", "Database_Name", "Database_Connection_Type", "Site_LUID";
