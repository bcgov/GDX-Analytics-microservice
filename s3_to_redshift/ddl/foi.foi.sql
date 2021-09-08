CREATE TABLE IF NOT EXISTS foi.foi
 (
         "request_id" VARCHAR(100)   ENCODE zstd,
         "team" VARCHAR(255)   ENCODE zstd,
         "manager" VARCHAR(255)   ENCODE zstd,
         "ministry" VARCHAR(255)   ENCODE zstd,
         "proc_org" VARCHAR(255)   ENCODE zstd,
         "type" VARCHAR(255)   ENCODE zstd,
         "applicant_type" VARCHAR(255)   ENCODE zstd,
         "subject" VARCHAR(255)   ENCODE zstd,
         "start_date" TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd,
         "end_date" TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd,
         "duedate" TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd,
         "total_process_days" INTEGER   ENCODE zstd,
         "current_activity" VARCHAR(255)   ENCODE zstd,
         "current_activity_date" TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd,
         "analyst" VARCHAR(255)   ENCODE zstd,
         "cross_gov_no" VARCHAR(255)   ENCODE zstd,
         "description" VARCHAR(8192)   ENCODE zstd,
         "status" VARCHAR(255)   ENCODE zstd,
         "count_on_time" INTEGER   ENCODE zstd,
         "count_overdue" INTEGER   ENCODE zstd,
         "on_hold_days" INTEGER   ENCODE zstd,
         "days_overdue" INTEGER   ENCODE zstd,
         "not_closed" VARCHAR(255)   ENCODE zstd,
         "fees_est" NUMERIC(18,0)   ENCODE zstd,
         "fees_waived" NUMERIC(18,0)   ENCODE zstd,
         "fees_paid" NUMERIC(18,0)   ENCODE zstd,
         "disposition" VARCHAR(255)   ENCODE zstd,
         "publication" VARCHAR(255)   ENCODE zstd,
         "publication_reason" VARCHAR(255)   ENCODE zstd,
         "extension" VARCHAR(255)   ENCODE zstd,
         "exec_cmts" VARCHAR(255)   ENCODE zstd,
	   "no_pages_delivered" INTEGER   ENCODE zstd,
	   "no_pages_in_request" INTEGER   ENCODE zstd,
	   "start_fyr" VARCHAR(255)   ENCODE zstd,
         "end_fyr" VARCHAR(255)   ENCODE zstd,
         "start_fqtr" VARCHAR(255)   ENCODE zstd,
         "end_fqtr" VARCHAR(255)   ENCODE zstd
 )
 DISTSTYLE EVEN;
 ALTER TABLE foi.foi owner to microservice;
 GRANT SELECT ON foi.foi TO "looker";
