/*********************************************************************
-- FUNCTION: public.set_jobcontrol(character varying,character varying)
-- DROP FUNCTION public.set_jobcontrol(character varying,character varying);
*********************************************************************/

CREATE OR REPLACE FUNCTION public.set_jobcontrol(
	in_appscope character varying(255),
    in_jobid character varying(255),
    in_jobtype character varying(255),
    in_laststatus character varying(255))
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$
/*********************************************************************
Name: set_jobcontrol
Auth: DF
Date: 11.04.2019
Notes:
    sets dataflow job metadata
*********************************************************************/
DECLARE 
    l_jobcontrolid bigint;
BEGIN
    --check if the record exists
    select count(1)
    into l_jobcontrolid
    from public.jobcontrol jc
    where jc.appscope=in_appscope
    and jc.jobid=in_jobid;

    --exit if it's already present
    if l_jobcontrolid > 0 then
        update public.jobcontrol jc
            set jobtype=in_jobtype,
                laststatus=in_laststatus,
				lasttouched=now()
        where jc.appscope=in_appscope
        and jc.jobid=in_jobid;

        return;
    end if;

    --otherwise insert it
    insert into public.jobcontrol
    (
        appscope,
        jobid,
        jobtype,
        laststatus
    )
    values
    (
        in_appscope,
        in_jobid,
        in_jobtype,
        in_laststatus
    );

    --trim the job archive for this appscope
    perform delete_jobcontrolarchive(in_appscope);
END

$BODY$;

ALTER FUNCTION public.set_jobcontrol(character varying,character varying,character varying,character varying) OWNER TO postgres;
GRANT ALL ON FUNCTION public.set_jobcontrol(character varying,character varying,character varying,character varying) to dataflowcontroluser;


CREATE OR REPLACE FUNCTION public.set_jobstatus(
    in_jobid character varying(255),
    in_status character varying(255))
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

/*********************************************************************
Name: set_jobstatus
Auth: DF
Date: 06.05.2019
Notes:
    Sets a job status
*********************************************************************/
BEGIN
    update public.jobcontrol jc
        set laststatus=case when in_status!=laststatus then in_status else laststatus end,
		lasttouched=case when in_status!=laststatus then now() else lasttouched end
    where jc.jobid=in_jobid;
END

$BODY$;

ALTER FUNCTION public.set_jobstatus(character varying,character varying) OWNER TO postgres;
GRANT ALL ON FUNCTION public.set_jobstatus(character varying,character varying) to dataflowcontroluser;


CREATE OR REPLACE FUNCTION public.get_jobcontrol(
    in_jobid character varying(255))
    RETURNS JSONB
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

/*********************************************************************
Name: get_jobcontrol
Auth: DF
Date: 11.04.2019
Notes:
    Returns a jobcontrol record
*********************************************************************/
DECLARE 
    l_result jsonb;
BEGIN
    select row_to_json(dat1)
    into l_result
    from
    (
        select
            jc.jobcontrolid,
            jc.appscope,
            jc.jobid,
            jc.jobtype,
            jc.laststatus,
            jc.createddate
        from public.jobcontrol jc
        where jc.jobid=in_jobid
    ) dat1;

    return l_result;
END

$BODY$;

ALTER FUNCTION public.get_jobcontrol(character varying) OWNER TO postgres;
GRANT ALL ON FUNCTION public.get_jobcontrol(character varying) to dataflowcontroluser;


CREATE OR REPLACE FUNCTION public.get_appscopejobcontrol(
	in_appscope character varying(255),
    in_jobtype character varying(255) default null,
    in_laststatus character varying(255) default null)
    RETURNS JSONB
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

/*********************************************************************
Name: get_appscopejobcontrol
Auth: DF
Date: 11.04.2019
Notes:
    Returns a jobcontrol record
*********************************************************************/
DECLARE 
    l_result jsonb;
BEGIN
	select json_agg(row_to_json(uac))
	into l_result
	from
	(
		select
			jc.jobcontrolid,
			jc.appscope,
			jc.jobid,
			jc.jobtype,
			jc.laststatus,
			jc.createddate,
			jc.lasttouched
		from public.jobcontrol jc
		where jc.appscope=in_appscope
        and (nullif(in_jobtype,'') is null or jc.jobtype=in_jobtype)
        and (nullif(in_laststatus,'') is null or jc.laststatus=in_laststatus)
	) uac;

	return l_result;
END

$BODY$;

ALTER FUNCTION public.get_appscopejobcontrol(character varying,character varying,character varying) OWNER TO postgres;
GRANT ALL ON FUNCTION public.get_appscopejobcontrol(character varying,character varying,character varying) to dataflowcontroluser;

CREATE OR REPLACE FUNCTION public.get_latestappscopejobcontrol(
	in_appscope character varying(255),
    in_jobtype character varying(255),
    in_limit int)
    RETURNS JSONB
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

/*********************************************************************
Name: get_latestappscopejobcontrol
Auth: DF
Date: 11.04.2019
Notes:
    Returns the latest jobcontrol record for an appscope and jobtype
*********************************************************************/
DECLARE 
    l_result jsonb;
    l_dtlimit timestamp;
BEGIN
    select now() - interval '24 hours'
    into l_dtlimit;

	select json_agg(row_to_json(uac))
	into l_result
	from
	(
		select
			jc.jobcontrolid,
			jc.appscope,
			jc.jobid,
			jc.jobtype,
			jc.laststatus,
			jc.createddate,
			jc.lasttouched
		from public.jobcontrol jc
		where jc.appscope=in_appscope
        and jc.jobtype=in_jobtype
        and jc.createddate >= l_dtlimit
        order by jc.createddate desc
        limit in_limit
	) uac;

	return l_result;
END

$BODY$;

ALTER FUNCTION public.get_latestappscopejobcontrol(character varying,character varying, int) OWNER TO postgres;
GRANT ALL ON FUNCTION public.get_latestappscopejobcontrol(character varying,character varying, int) to dataflowcontroluser;


-- FUNCTION: public.delete_jobcontrol(character varying)
-- DROP FUNCTION public.delete_jobcontrol(character varying);

CREATE OR REPLACE FUNCTION public.delete_jobcontrol(
	in_appscope character varying(255),
    in_jobid character varying(255))
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$
/*********************************************************************
Name: delete_jobcontrol
Auth: DF
Date: 11.04.2019
Notes:
    Removes a jobcontrol record
*********************************************************************/
DECLARE 
    l_jobcontrolid bigint;
BEGIN
    --check if the record exists
    select count(1)
    into l_jobcontrolid
    from public.jobcontrol jc
    where jc.appscope=in_appscope
    and jc.jobid=in_jobid;

    --delete if it's present
    if l_jobcontrolid > 0 then
        delete from public.jobcontrol jc
        where jc.appscope=in_appscope
        and jc.jobid=in_jobid;
    end if;

	return;
END

$BODY$;

ALTER FUNCTION public.delete_jobcontrol(character varying,character varying) OWNER TO postgres;
GRANT ALL ON FUNCTION public.delete_jobcontrol(character varying,character varying) to dataflowcontroluser;


-- FUNCTION: public.delete_jobcontrolarchive(character varying)
-- DROP FUNCTION public.delete_jobcontrolarchive(character varying);

CREATE OR REPLACE FUNCTION public.delete_jobcontrolarchive(
	in_appscope character varying(255))
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$
/*********************************************************************
Name: delete_jobcontrolarchive
Auth: DF
Date: 11.04.2019
Notes:
    Removes old jobcontrol records
*********************************************************************/
DECLARE 
    l_limit timestamp;
BEGIN
    select now() - interval '24 hours'
    into l_limit;

    --delete old jobs
	delete from public.jobcontrol jc
	where jc.appscope=in_appscope
	and jc.createddate < l_limit;

	return;
END

$BODY$;

ALTER FUNCTION public.delete_jobcontrolarchive(character varying) OWNER TO postgres;
GRANT ALL ON FUNCTION public.delete_jobcontrolarchive(character varying) to dataflowcontroluser;
