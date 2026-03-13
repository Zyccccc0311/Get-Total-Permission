with combined as (
select
	*
from
	(
with all_permissions as (
	-- 场景1：grantee_type = Group，展开到user
	select
		ngp.*,
		w.name as workbookname,
		gu.user_id,
		su.name as username,
		g.name as groupname
	from
		public.next_gen_permissions ngp
	inner join public.workbooks w on
		w.id = ngp.authorizable_id
	inner join public.group_users gu on
		gu.group_id = ngp.grantee_id
	left join public.groups g on
		g.id = ngp.grantee_id
	left join users u on
		u.id = gu.user_id
	left join system_users su on
		su.id = u.system_user_id
	where
		ngp.site_id = 8
		and ngp.capability_id = 1
		and ngp.authorizable_type = 'Workbook'
		and ngp.grantee_type = 'Group'
union all
	-- 场景2：grantee_type = User，grantee_id就是user_id
	select
		ngp.*,
		w.name as workbookname,
		ngp.grantee_id as user_id,
		su.name as username,
		null as groupname
	from
		public.next_gen_permissions ngp
	inner join public.workbooks w on
		w.id = ngp.authorizable_id
	left join users u on
		u.id = ngp.grantee_id
	left join system_users su on
		su.id = u.system_user_id
	where
		ngp.site_id = 8
		and ngp.capability_id = 1
		and ngp.authorizable_type = 'Workbook'
		and ngp.grantee_type = 'User'
),
	ranked as (
	select
		ap.*,
		row_number() over (
               partition by ap.user_id,
		ap.authorizable_id
	order by
		ap.permission desc
           ) as rn
	from
		all_permissions ap
)
	select
		pc.project_id ,
		p."name" as project_name,
		r.authorizable_id as content_id,
		r.workbookname as workbook_name,
		r.user_id,
		r.username as user_name,
		r."permission",
		1 as source
	from
		ranked r
	inner join projects_contents pc on
		pc.content_id = r.authorizable_id
	inner join projects p on
		pc.project_id = p.id
	where
		r.rn = 1
		and pc.content_type = 'workbook'
		and r.username <> 'guest'

) as q1
union all
select
	*
from
	(
with recursive project_tree as (
	select
		id as project_id
	from
		public.projects
	where
		site_id = 8
		and controlled_permissions_enabled = true
		and nested_projects_permissions_included = true
union
	select
		sub_pc.content_id as project_id
	from
		public.projects_contents sub_pc
	inner join project_tree pt on
		sub_pc.project_id = pt.project_id
	where
		sub_pc.site_id = 8
		and sub_pc.content_type = 'project'
),
	all_workbooks as (
	select
		pc.id,
		pc.project_id,
		pc.site_id,
		pc.content_id,
		pc.content_type,
		'锁定不嵌套' as source
	from
		public.projects_contents pc
	inner join public.projects p on
		pc.project_id = p.id
	where
		pc.site_id = 8
		and pc.content_type = 'workbook'
		and p.controlled_permissions_enabled = true
		and p.nested_projects_permissions_included = false
union all
	select
		pc.id,
		pc.project_id,
		pc.site_id,
		pc.content_id,
		pc.content_type,
		'锁定含嵌套' as source
	from
		public.projects_contents pc
	inner join project_tree pt on
		pc.project_id = pt.project_id
	where
		pc.site_id = 8
		and pc.content_type = 'workbook'
),
	all_permissions as (
	-- 场景1: Group 权限展开到 User
	select
		aw.project_id,
		aw.content_id,
		p.name as project_name,
		w.name as workbook_name,
		u.id as user_id,
		su.name as user_name,
		pt.permission,
		aw.source,
		'Group' as grantee_type
	from
		all_workbooks aw
	inner join permissions_templates pt on
		pt.container_id = aw.project_id
	inner join public.projects p on
		p.id = aw.project_id
	inner join public.workbooks w on
		w.id = aw.content_id
	inner join public.group_users gu on
		gu.group_id = pt.grantee_id
	left join users u on
		u.id = gu.user_id
	left join system_users su on
		su.id = u.system_user_id
	where
		pt.capability_id = 1
		and pt.template_type = 'Workbook'
		and pt.grantee_type = 'Group'
union all
	-- 场景2: User 直接权限
	select
		aw.project_id,
		aw.content_id,
		p.name as project_name,
		w.name as workbook_name,
		u.id as user_id,
		su.name as user_name,
		pt.permission,
		aw.source,
		'User' as grantee_type
	from
		all_workbooks aw
	inner join permissions_templates pt on
		pt.container_id = aw.project_id
	inner join public.projects p on
		p.id = aw.project_id
	inner join public.workbooks w on
		w.id = aw.content_id
	left join users u on
		u.id = pt.grantee_id
	left join system_users su on
		su.id = u.system_user_id
	where
		pt.capability_id = 1
		and pt.template_type = 'Workbook'
		and pt.grantee_type = 'User'
),
	ranked as (
	select
		*,
		row_number() over (
               partition by user_id,
		content_id
	order by
		permission desc
           ) as rn
	from
		all_permissions
)
	select
		project_id,
		project_name,
		content_id,
		workbook_name,
		user_id,
		user_name,
		permission,
		2 as source
	from
		ranked
	where
		rn = 1
	order by
		project_name,
		workbook_name,
		user_name

) as q2
),
final_ranked as (
select
	*,
	row_number() over (
               partition by content_id,
	user_id
order by
	permission desc
           ) as rn
from
	combined
)
select
	project_id,
	project_name,
	content_id,
	workbook_name,
	user_id,
	permission
from
	final_ranked
where
	rn = 1
order by
	project_name,
	workbook_name,
	user_name;
