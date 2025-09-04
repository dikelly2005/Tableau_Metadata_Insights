create or replace view TABLEAU.TABLEAU_REST.VW_EFFECTIVE_PERMISSIONS(
	"Site_LUID",
	"Item_Type",
	"Item_LUID",
	"User_LUID",
	"Project_LUID",
	"Capability_Name",
	"Capability_Mode",
	"Capability_Source",
	"Admin_Insights_Published_At"
) as 

WITH items AS (
SELECT LUID AS "Item_LUID", 'datasource' AS "Item_Type", "Project_LUID", "Owner_LUID", "Name", "Description", NULL AS "Item_Hyperlink", "Site_LUID" FROM TABLEAU.TABLEAU_REST.ITEMS_DATASOURCES
UNION
SELECT LUID AS "Item_LUID", 'flow' AS "Item_Type", "Project_LUID", "Owner_LUID", "Name", "Description", "Webpage_URL" AS "Item_Hyperlink", "Site_LUID" FROM TABLEAU.TABLEAU_REST.ITEMS_FLOWS
UNION
SELECT LUID AS "Item_LUID", 'virtualconnection' AS "Item_Type", "Project_LUID", "Owner_LUID", "Name", "Description", NULL AS "Item_Hyperlink", "Site_LUID" FROM TABLEAU.TABLEAU_REST.ITEMS_VIRTUAL_CONNECTIONS
UNION
SELECT LUID AS "Item_LUID", 'workbook' AS "Item_Type", "Project_LUID", "Owner_LUID", "Name", "Description", "Webpage_URL" AS "Item_Hyperlink", "Site_LUID" FROM TABLEAU.TABLEAU_REST.ITEMS_WORKBOOKS
UNION
SELECT LUID AS "Item_LUID", 'project' AS "Item_Type", "LUID" AS "Project_LUID", "Owner_LUID", "Name", "Description", NULL AS "Item_Hyperlink", "Site_LUID" FROM TABLEAU.TABLEAU_REST.ITEMS_PROJECTS
)

, users AS (
SELECT LUID AS "User_LUID", "Full_Name"
, CASE "Site_Role"
    WHEN 'Unlicensed' THEN 0
    WHEN 'Viewer' THEN 1
    WHEN 'Explorer' THEN 2
    WHEN 'ExplorerCanPublish' THEN 3
    WHEN 'Creator' THEN 4
    WHEN 'SiteAdministratorExplorer' THEN 99
    WHEN 'SiteAdministratorCreator' THEN 99
    END AS "Site_Role_Permission_Value"
FROM TABLEAU.TABLEAU_REST.USERS WHERE LOWER("Site_Role") != 'unlicensed'
)

, capabilities AS (
SELECT DISTINCT LEFT("Content_Type",LEN("Content_Type") - 1) AS "Content_Type", "Capability_Name"
, CASE 
    WHEN LOWER("Content_Type") = 'projects' AND LOWER("Capability_Name") = 'read' THEN 1
    WHEN LOWER("Content_Type") = 'projects' AND LOWER("Capability_Name") = 'write' THEN 3
    WHEN LOWER("Content_Type") = 'workbooks' AND LOWER("Capability_Name") = 'read' THEN 1
    WHEN LOWER("Content_Type") = 'workbooks' AND LOWER("Capability_Name") = 'filter' THEN 1
    WHEN LOWER("Content_Type") = 'workbooks' AND LOWER("Capability_Name") = 'viewcomments' THEN 1
    WHEN LOWER("Content_Type") = 'workbooks' AND LOWER("Capability_Name") = 'addcomment' THEN 1
    WHEN LOWER("Content_Type") = 'workbooks' AND LOWER("Capability_Name") = 'exportimage' THEN 1
    WHEN LOWER("Content_Type") = 'workbooks' AND LOWER("Capability_Name") = 'exportdata' THEN 1
    WHEN LOWER("Content_Type") = 'workbooks' AND LOWER("Capability_Name") = 'shareview' THEN 2
    WHEN LOWER("Content_Type") = 'workbooks' AND LOWER("Capability_Name") = 'viewunderlyingdata' THEN 2
    WHEN LOWER("Content_Type") = 'workbooks' AND LOWER("Capability_Name") = 'webauthoring' THEN 2
    WHEN LOWER("Content_Type") = 'workbooks' AND LOWER("Capability_Name") = 'runexplaindata' THEN 2
    WHEN LOWER("Content_Type") = 'workbooks' AND LOWER("Capability_Name") = 'exportxml' THEN 2
    WHEN LOWER("Content_Type") = 'workbooks' AND LOWER("Capability_Name") = 'write' THEN 3
    WHEN LOWER("Content_Type") = 'workbooks' AND LOWER("Capability_Name") = 'createrefreshmetrics' THEN 3
    WHEN LOWER("Content_Type") = 'workbooks' AND LOWER("Capability_Name") = 'changehierarchy' THEN 3
    WHEN LOWER("Content_Type") = 'workbooks' AND LOWER("Capability_Name") = 'delete' THEN 3
    WHEN LOWER("Content_Type") = 'workbooks' AND LOWER("Capability_Name") = 'changepermissions' THEN 3
    WHEN LOWER("Content_Type") = 'workbooks' AND LOWER("Capability_Name") = 'extractrefresh' THEN 3
    WHEN LOWER("Content_Type") = 'datasources' AND LOWER("Capability_Name") = 'read' THEN 1
    WHEN LOWER("Content_Type") = 'datasources' AND LOWER("Capability_Name") = 'connect' THEN 1
    WHEN LOWER("Content_Type") = 'datasources' AND LOWER("Capability_Name") = 'exportxml' THEN 2
    WHEN LOWER("Content_Type") = 'datasources' AND LOWER("Capability_Name") = 'write' THEN 3
    WHEN LOWER("Content_Type") = 'datasources' AND LOWER("Capability_Name") = 'saveas' THEN 3
    WHEN LOWER("Content_Type") = 'datasources' AND LOWER("Capability_Name") = 'vizqldataapiaccess' THEN 3
    WHEN LOWER("Content_Type") = 'datasources' AND LOWER("Capability_Name") = 'pulsemetricdefine' THEN 3
    WHEN LOWER("Content_Type") = 'datasources' AND LOWER("Capability_Name") = 'delete' THEN 3
    WHEN LOWER("Content_Type") = 'datasources' AND LOWER("Capability_Name") = 'changehierarchy' THEN 3
    WHEN LOWER("Content_Type") = 'datasources' AND LOWER("Capability_Name") = 'changepermissions' THEN 3
    WHEN LOWER("Content_Type") = 'datasources' AND LOWER("Capability_Name") = 'extractrefresh' THEN 3
    WHEN LOWER("Content_Type") = 'flows' AND LOWER("Capability_Name") = 'read' THEN 1
    WHEN LOWER("Content_Type") = 'flows' AND LOWER("Capability_Name") = 'exportxml' THEN 2
    WHEN LOWER("Content_Type") = 'flows' AND LOWER("Capability_Name") = 'execute' THEN 4
    WHEN LOWER("Content_Type") = 'flows' AND LOWER("Capability_Name") = 'write' THEN 4
    WHEN LOWER("Content_Type") = 'flows' AND LOWER("Capability_Name") = 'webauthoringforflows' THEN 4
    WHEN LOWER("Content_Type") = 'flows' AND LOWER("Capability_Name") = 'delete' THEN 3
    WHEN LOWER("Content_Type") = 'flows' AND LOWER("Capability_Name") = 'changepermissions' THEN 3
    WHEN LOWER("Content_Type") = 'flows' AND LOWER("Capability_Name") = 'changehierarchy' THEN 3
    WHEN LOWER("Content_Type") = 'virtualconnections' AND LOWER("Capability_Name") = 'read' THEN 1
    WHEN LOWER("Content_Type") = 'virtualconnections' AND LOWER("Capability_Name") = 'connect' THEN 4
    WHEN LOWER("Content_Type") = 'virtualconnections' AND LOWER("Capability_Name") = 'write' THEN 4
    WHEN LOWER("Content_Type") = 'virtualconnections' AND LOWER("Capability_Name") = 'changehierarchy' THEN 4
    WHEN LOWER("Content_Type") = 'virtualconnections' AND LOWER("Capability_Name") = 'delete' THEN 3
    WHEN LOWER("Content_Type") = 'virtualconnections' AND LOWER("Capability_Name") = 'changepermissions' THEN 3
    END AS "Minimum_Site_Role_Permission_Value",
    "Admin_Insights_Published_At"
FROM TABLEAU.TABLEAU_REST.PERMISSIONS_EXPLICIT
WHERE LOWER(LEFT("Content_Type",LEN("Content_Type") - 1)) IN ('datasource','workbook','virtualconnection','flow','project')
AND "Capability_Name" NOT ILIKE '%project%'
)

, permission_matrix AS (
SELECT
i.*, u.*, c."Capability_Name", c."Minimum_Site_Role_Permission_Value", c."Admin_Insights_Published_At"
FROM items i
CROSS JOIN users u
JOIN capabilities c
    ON LOWER(i."Item_Type") = LOWER(c."Content_Type")
)

, default_permissions AS (
SELECT cp."Project_LUID", dp."Grantee_Type", dp."Grantee_LUID", dp."Content_Type", dp."Capability_Name", dp."Capability_Mode", dp."Admin_Insights_Published_At"
FROM TABLEAU.TABLEAU_REST.VW_DEFAULT_PROJECT_PERMISSIONS cp
JOIN TABLEAU.TABLEAU_REST.PERMISSIONS_DEFAULT dp
    ON cp."Default_Controlling_Permissions_Project_LUID" = dp."Project_LUID"
)

, group_default_permissions AS (
SELECT p."Content_Type", p."Project_LUID", g."User_LUID", p."Capability_Name"
, MAX(CASE WHEN p."Capability_Mode" = 'Deny' THEN 1 ELSE 0 END) AS "Capability_Denied"
, MAX(CASE WHEN p."Capability_Mode" = 'Allow' THEN 1 ELSE 0 END) AS "Capability_Allowed"
FROM TABLEAU.TABLEAU_REST.PERMISSIONS_DEFAULT p
JOIN TABLEAU.TABLEAU_REST.GROUP_USERS g
    ON p."Grantee_LUID" = g."Group_LUID"
WHERE LOWER(p."Grantee_Type") = 'group'
GROUP BY p."Content_Type", p."Project_LUID", g."User_LUID", p."Capability_Name"
)

, group_explicit_permissions AS (
SELECT p."Content_LUID" AS "Item_LUID", LEFT(p."Content_Type",LEN(p."Content_Type") - 1) AS "Item_Type", g."User_LUID", p."Capability_Name"
, MAX(CASE WHEN p."Capability_Mode" = 'Deny' THEN 1 ELSE 0 END) AS "Capability_Denied"
, MAX(CASE WHEN p."Capability_Mode" = 'Allow' THEN 1 ELSE 0 END) AS "Capability_Allowed"
FROM TABLEAU.TABLEAU_REST.PERMISSIONS_EXPLICIT p
JOIN TABLEAU.TABLEAU_REST.GROUP_USERS g
    ON p."Grantee_LUID" = g."Group_LUID"
WHERE LOWER(p."Grantee_Type") = 'group'
GROUP BY p."Content_LUID", LEFT(p."Content_Type",LEN(p."Content_Type") - 1), g."User_LUID", p."Capability_Name"
)

, group_permissions AS (
SELECT pm."Item_LUID", pm."Item_Type", pm."Project_LUID", pm."Owner_LUID", pm."User_LUID", pm."Capability_Name"  
, CASE 
    WHEN MAX(CASE WHEN e."Capability_Denied" = 1 THEN 1 ELSE 0 END) = 1 THEN 'Deny' 
    WHEN MAX(CASE WHEN e."Capability_Allowed" = 1 THEN 1 ELSE 0 END) = 1 THEN 'Allow'
    WHEN MAX(CASE WHEN d."Capability_Denied" = 1 AND p."Content_Permissions" != 'ManagedByOwner' THEN 1 ELSE 0 END) = 1 THEN 'Deny' 
    WHEN MAX(CASE WHEN d."Capability_Allowed" = 1 AND p."Content_Permissions" != 'ManagedByOwner' THEN 1 ELSE 0 END) = 1 THEN 'Allow'
    ELSE 'None'
    END AS "Capability_Mode"
FROM permission_matrix pm
LEFT JOIN TABLEAU.TABLEAU_REST.ITEMS_PROJECTS p
    ON pm."Project_LUID" = p.luid
LEFT JOIN group_default_permissions d
    ON pm."Project_LUID" = d."Project_LUID"
    AND LOWER(pm."Item_Type") = LOWER(d."Content_Type")
    AND pm."User_LUID" = d."User_LUID"
    AND pm."Capability_Name" = d."Capability_Name"
LEFT JOIN group_explicit_permissions e
    ON pm."Item_LUID" = e."Item_LUID"
    AND LOWER(pm."Item_Type") = LOWER(e."Item_Type")
    AND pm."User_LUID" = e."User_LUID"
    AND pm."Capability_Name" = e."Capability_Name"
GROUP BY pm."Item_LUID", pm."Item_Type", pm."Project_LUID", pm."Owner_LUID", pm."User_LUID", pm."Capability_Name"    
)

, user_default_permissions AS (
SELECT p."Content_Type", p."Project_LUID", p."Grantee_LUID" AS "User_LUID", p."Capability_Name"
, MAX(CASE WHEN p."Capability_Mode" = 'Deny' THEN 1 ELSE 0 END) AS "Capability_Denied"
, MAX(CASE WHEN p."Capability_Mode" = 'Allow' THEN 1 ELSE 0 END) AS "Capability_Allowed"
FROM TABLEAU.TABLEAU_REST.PERMISSIONS_DEFAULT p
WHERE LOWER(p."Grantee_Type") = 'user'
GROUP BY p."Content_Type", p."Project_LUID", p."Grantee_LUID", p."Capability_Name"
)

, user_explicit_permissions AS (
SELECT p."Content_LUID" AS "Item_LUID", LEFT(p."Content_Type",LEN(p."Content_Type") - 1) AS "Item_Type", p."Grantee_LUID" AS "User_LUID", p."Capability_Name"
, MAX(CASE WHEN p."Capability_Mode" = 'Deny' THEN 1 ELSE 0 END) AS "Capability_Denied"
, MAX(CASE WHEN p."Capability_Mode" = 'Allow' THEN 1 ELSE 0 END) AS "Capability_Allowed"
FROM TABLEAU.TABLEAU_REST.PERMISSIONS_EXPLICIT p
WHERE LOWER(p."Grantee_Type") = 'user'
GROUP BY p."Content_LUID", LEFT(p."Content_Type",LEN(p."Content_Type") - 1), p."Grantee_LUID", p."Capability_Name"
)

, user_permissions AS (
SELECT pm."Item_LUID", pm."Item_Type", pm."Project_LUID", pm."Owner_LUID", pm."User_LUID", pm."Capability_Name"  
, CASE 
    WHEN MAX(CASE WHEN e."Capability_Denied" = 1 THEN 1 ELSE 0 END) = 1 THEN 'Deny' 
    WHEN MAX(CASE WHEN e."Capability_Allowed" = 1 THEN 1 ELSE 0 END) = 1 THEN 'Allow'
    WHEN MAX(CASE WHEN d."Capability_Denied" = 1 AND p."Content_Permissions" != 'ManagedByOwner' THEN 1 ELSE 0 END) = 1 THEN 'Deny' 
    WHEN MAX(CASE WHEN d."Capability_Allowed" = 1 AND p."Content_Permissions" != 'ManagedByOwner' THEN 1 ELSE 0 END) = 1 THEN 'Allow'
    ELSE 'None'
    END AS "Capability_Mode"
FROM permission_matrix pm
LEFT JOIN TABLEAU.TABLEAU_REST.ITEMS_PROJECTS p
    ON pm."Project_LUID" = p.luid
LEFT JOIN user_default_permissions d
    ON pm."Project_LUID" = d."Project_LUID"
    AND LOWER(pm."Item_Type") = LOWER(d."Content_Type")
    AND pm."User_LUID" = d."User_LUID"
    AND pm."Capability_Name" = d."Capability_Name"
LEFT JOIN user_explicit_permissions e
    ON pm."Item_LUID" = e."Item_LUID"
    AND LOWER(pm."Item_Type") = LOWER(e."Item_Type")
    AND pm."User_LUID" = e."User_LUID"
    AND pm."Capability_Name" = e."Capability_Name"
GROUP BY pm."Item_LUID", pm."Item_Type", pm."Project_LUID", pm."Owner_LUID", pm."User_LUID", pm."Capability_Name"    
)

, project_owners AS (
SELECT pp."Project_LUID", gu."User_LUID"
FROM TABLEAU.TABLEAU_REST.VW_DEFAULT_PROJECT_PERMISSIONS pp
JOIN TABLEAU.TABLEAU_REST.PERMISSIONS_EXPLICIT pe
    ON pp."Default_Controlling_Permissions_Project_LUID" = pe."Content_LUID"
    AND pe."Capability_Name" ILIKE '%project%'
JOIN TABLEAU.TABLEAU_REST.GROUP_USERS gu
    ON pe."Grantee_LUID" = gu."Group_LUID"
WHERE pe."Grantee_Type" = 'Group'
UNION
SELECT pp."Project_LUID", pe."Grantee_LUID" AS "User_LUID"
FROM TABLEAU.TABLEAU_REST.VW_DEFAULT_PROJECT_PERMISSIONS pp
JOIN TABLEAU.TABLEAU_REST.PERMISSIONS_EXPLICIT pe
    ON pp."Default_Controlling_Permissions_Project_LUID" = pe."Content_LUID"
    AND pe."Capability_Name" ILIKE '%project%'
WHERE pe."Grantee_Type" = 'User'
UNION
SELECT pp."Project_LUID", gu."User_LUID"
FROM TABLEAU.TABLEAU_REST.VW_DEFAULT_PROJECT_PERMISSIONS pp
JOIN TABLEAU.TABLEAU_REST.PERMISSIONS_EXPLICIT pe
    ON pp."Project_LUID" = pe."Content_LUID"
    AND pe."Capability_Name" ILIKE '%project%'
JOIN TABLEAU.TABLEAU_REST.GROUP_USERS gu
    ON pe."Grantee_LUID" = gu."Group_LUID"
WHERE pe."Grantee_Type" = 'Group'
UNION
SELECT pp."Project_LUID", pe."Grantee_LUID" AS "User_LUID"
FROM TABLEAU.TABLEAU_REST.VW_DEFAULT_PROJECT_PERMISSIONS pp
JOIN TABLEAU.TABLEAU_REST.PERMISSIONS_EXPLICIT pe
    ON pp."Project_LUID" = pe."Content_LUID"
    AND pe."Capability_Name" ILIKE '%project%'
WHERE pe."Grantee_Type" = 'User'
)

SELECT
pm."Site_LUID", pm."Item_Type", pm."Item_LUID", pm."User_LUID", pm."Project_LUID", pm."Capability_Name"
, CASE 
    WHEN u."Site_Role" IN ('SiteAdministratorCreator','SiteAdministratorExplorer') THEN 'Allow'
    WHEN pm."Site_Role_Permission_Value" < "Minimum_Site_Role_Permission_Value" THEN 'Deny'
    WHEN po."User_LUID" IS NOT NULL THEN 'Allow'
    WHEN pm."Owner_LUID" = pm."User_LUID" THEN 'Allow'
    WHEN up."Capability_Mode" = 'Deny' THEN 'Deny'
    WHEN up."Capability_Mode" = 'Allow' THEN 'Allow'
    WHEN gp."Capability_Mode" = 'Deny' THEN 'Deny'
    WHEN gp."Capability_Mode" = 'Allow' THEN 'Allow'
    ELSE 'Deny'
    END AS "Capability_Mode"
, CASE 
    WHEN u."Site_Role" IN ('SiteAdministratorCreator','SiteAdministratorExplorer') THEN 'Site Administrator'
    WHEN pm."Site_Role_Permission_Value" < "Minimum_Site_Role_Permission_Value" THEN 'Outside Scope of Site Role'
    WHEN po."User_LUID" IS NOT NULL THEN 'Project Owner'
    WHEN pm."Owner_LUID" = pm."User_LUID" THEN 'Content Owner'
    WHEN up."Capability_Mode" = 'Deny' THEN 'User Denied'
    WHEN up."Capability_Mode" = 'Allow' THEN 'User Allowed'
    WHEN gp."Capability_Mode" = 'Deny' THEN 'Group Denied'
    WHEN gp."Capability_Mode" = 'Allow' THEN 'Group Allowed'
    ELSE 'No Capabilities Defined'
    END AS "Capability_Source"
, pm."Admin_Insights_Published_At"    
FROM permission_matrix pm
JOIN TABLEAU.TABLEAU_REST.USERS u
    ON pm."User_LUID" = u.LUID
LEFT JOIN project_owners po
    ON pm."Project_LUID" = po."Project_LUID"
    AND pm."User_LUID" = po."User_LUID" 
LEFT JOIN user_permissions up
    ON LOWER(pm."Item_Type") = LOWER(up."Item_Type")
    AND pm."Item_LUID" = up."Item_LUID"
    AND pm."User_LUID" = up."User_LUID"
    AND pm."Capability_Name" = up."Capability_Name"
LEFT JOIN group_permissions gp
    ON LOWER(pm."Item_Type") = LOWER(gp."Item_Type")
    AND pm."Item_LUID" = gp."Item_LUID"
    AND pm."User_LUID" = gp."User_LUID"
    AND pm."Capability_Name" = gp."Capability_Name";
