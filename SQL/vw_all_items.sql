create or replace view TABLEAU.TABLEAU_REST.VW_ALL_ITEMS(
	"Site_LUID",
	"Item_LUID",
	"Item_Type",
	"Name",
	"Description",
	"Owner_LUID",
	"Project_LUID",
	"Created_At",
	"Updated_At",
	"Content_URL",
	"Webpage_URL",
	"Is_Certified",
	"Type",
	"Size",
	"Has_Extracts",
	"Default_View_LUID",
	"Admin_Insights_Published_At"
) as 

SELECT "Site_LUID", "Item_LUID", "Item_Type", "Name", "Description", "Owner_LUID", "Project_LUID", "Created_At", "Updated_At", "Content_URL", "Webpage_URL", "Is_Certified", "Type", "Size", "Has_Extracts", "Default_View_LUID"
, MAX("Admin_Insights_Published_At") AS "Admin_Insights_Published_At"
FROM
(
SELECT "Site_LUID", LUID AS "Item_LUID", 'datasource' AS "Item_Type", "Name", "Description", "Owner_LUID", "Project_LUID", "Created_At", "Updated_At", "Admin_Insights_Published_At", "Content_URL", NULL AS "Webpage_URL"
, "Is_Certified", "Type", "Size", "Has_Extracts", NULL AS "Default_View_LUID"
FROM TABLEAU.TABLEAU_REST.ITEMS_DATASOURCES
UNION
SELECT "Site_LUID", LUID AS "Item_LUID", 'flow' AS "Item_Type", "Name", "Description", "Owner_LUID", "Project_LUID", "Created_At", "Updated_At", "Admin_Insights_Published_At", NULL AS "Content_URL", "Webpage_URL"
, FALSE AS "Is_Certified", NULL AS "Type", 0 AS "Size", FALSE AS "Has_Extracts", NULL AS "Default_View_LUID"
FROM TABLEAU.TABLEAU_REST.ITEMS_FLOWS
UNION
SELECT "Site_LUID", LUID AS "Item_LUID", 'project' AS "Item_Type", "Name", "Description", "Owner_LUID", "Parent_Project_LUID" AS "Project_LUID", "Created_At", "Updated_At", "Admin_Insights_Published_At", NULL AS "Content_URL", NULL AS "Webpage_URL"
, FALSE AS "Is_Certified", NULL AS "Type", 0 AS "Size", FALSE AS "Has_Extracts", NULL AS "Default_View_LUID"
FROM TABLEAU.TABLEAU_REST.ITEMS_PROJECTS
UNION
SELECT "Site_LUID", LUID AS "Item_LUID", 'virtualconnection' AS "Item_Type", "Name", "Description", "Owner_LUID", "Project_LUID", "Created_At", "Updated_At", "Admin_Insights_Published_At", NULL AS "Content_URL", NULL AS "Webpage_URL"
, "Is_Certified", NULL AS "Type", 0 AS "Size", "Has_Extracts", NULL AS "Default_View_LUID"
FROM TABLEAU.TABLEAU_REST.ITEMS_VIRTUAL_CONNECTIONS
UNION
SELECT "Site_LUID", LUID AS "Item_LUID", 'workbook' AS "Item_Type", "Name", "Description", "Owner_LUID", "Project_LUID", "Created_At", "Updated_At", "Admin_Insights_Published_At", "Content_URL", "Webpage_URL"
, NULL AS "Is_Certified", NULL AS "Type", "Size", NULL AS "Has_Extracts", "Default_View_LUID"
FROM TABLEAU.TABLEAU_REST.ITEMS_WORKBOOKS
) A
GROUP BY "Site_LUID", "Item_LUID", "Item_Type", "Name", "Description", "Owner_LUID", "Project_LUID", "Created_At", "Updated_At", "Content_URL", "Webpage_URL", "Is_Certified", "Type", "Size", "Has_Extracts", "Default_View_LUID";
