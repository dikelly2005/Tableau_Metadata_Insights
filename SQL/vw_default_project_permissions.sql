create or replace view TABLEAU.TABLEAU_REST.VW_DEFAULT_PROJECT_PERMISSIONS(
	"Project_LUID",
	"Default_Controlling_Permissions_Project_LUID"
) as

WITH RECURSIVE permission_finder AS (
    -- Start with each project and its parent
    SELECT 
        p.luid as original_project,
        p.luid as current_project,
        p."Parent_Project_LUID",
        parent."Content_Permissions" as parent_permissions,
        0 as levels_up
    FROM TABLEAU.TABLEAU_REST.ITEMS_PROJECTS p
    LEFT JOIN TABLEAU.TABLEAU_REST.ITEMS_PROJECTS parent 
        ON p."Parent_Project_LUID" = parent.luid
    
    UNION ALL
    
    -- Keep going up while parent locks permissions
    SELECT 
        pf.original_project,
        parent.luid as current_project,
        parent."Parent_Project_LUID",
        grandparent."Content_Permissions" as parent_permissions,
        pf.levels_up + 1
    FROM permission_finder pf
    JOIN TABLEAU.TABLEAU_REST.ITEMS_PROJECTS parent 
        ON pf."Parent_Project_LUID" = parent.luid
    LEFT JOIN TABLEAU.TABLEAU_REST.ITEMS_PROJECTS grandparent
        ON parent."Parent_Project_LUID" = grandparent.luid
    WHERE pf.parent_permissions = 'LockedToProject'
        AND pf.levels_up < 20  -- Simple safety limit
)
SELECT DISTINCT
    original_project AS "Project_LUID",
    FIRST_VALUE(current_project) OVER (
        PARTITION BY original_project 
        ORDER BY 
            CASE WHEN parent_permissions != 'LockedToProject' OR parent_permissions IS NULL THEN 0 ELSE 1 END,
            levels_up
    ) AS "Default_Controlling_Permissions_Project_LUID"
FROM permission_finder;
