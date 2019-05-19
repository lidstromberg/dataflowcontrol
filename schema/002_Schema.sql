CREATE TABLE public.jobcontrol
(
    jobcontrolid bigserial not null,
    appscope character varying(255) COLLATE pg_catalog."default" NOT NULL,
    jobid character varying(255) COLLATE pg_catalog."default" NOT NULL,
    jobtype character varying(255) COLLATE pg_catalog."default" NOT NULL,
    laststatus character varying(255) COLLATE pg_catalog."default" NOT NULL,
    createddate timestamp with time zone NOT NULL DEFAULT now(),
    lasttouched timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT pk_jobcontrol PRIMARY KEY (jobcontrolid),
    CONSTRAINT uc_jobcontrol_1 UNIQUE (jobid),
    CONSTRAINT uc_jobcontrol_2 UNIQUE (appscope,jobid)
);

CREATE INDEX IX_jobcontrol_1 on public.jobcontrol(appscope,jobtype,laststatus,jobid);

ALTER TABLE public.jobcontrol OWNER to postgres;

GRANT ALL ON TABLE public.jobcontrol to dataflowcontroluser;
GRANT ALL ON SEQUENCE jobcontrol_jobcontrolid_seq to dataflowcontroluser;