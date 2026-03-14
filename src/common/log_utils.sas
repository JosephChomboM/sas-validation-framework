/* =========================================================================
log_utils.sas - Utilidades de logging por step via PROC PRINTTO
========================================================================= */

%macro fw_log_start(step_name=, run_id=, fw_root=, log_stem=);
    %global _fw_log_path _fw_log_step _fw_log_run;

    %let _fw_log_step=&step_name.;
    %let _fw_log_run=&run_id.;
    %let _fw_log_path=&fw_root./outputs/runs/&run_id./logs/&log_stem..log;

    proc printto log="&_fw_log_path." new;
    run;

    %put NOTE:======================================================;
    %put NOTE: [fw_log_start] step=&step_name.;
    %put NOTE: [fw_log_start] run_id=&run_id.;
    %put NOTE: [fw_log_start] log=&_fw_log_path.;
    %put NOTE: [fw_log_start] started_at=%sysfunc(datetime(), E8601DT19.).;
    %if %symexist(ctx_scope)=1 %then
        %put NOTE: [fw_log_start] ctx_scope=&ctx_scope.;
    %if %symexist(ctx_troncal_id)=1 %then
        %put NOTE: [fw_log_start] ctx_troncal_id=&ctx_troncal_id.;
    %if %symexist(ctx_split)=1 %then
        %put NOTE: [fw_log_start] ctx_split=&ctx_split.;
    %if %symexist(ctx_seg_id)=1 %then
        %put NOTE: [fw_log_start] ctx_seg_id=&ctx_seg_id.;
    %put NOTE:======================================================;
%mend fw_log_start;

%macro fw_log_stop(step_name=, step_rc=0);
    %put NOTE:======================================================;
    %put NOTE: [fw_log_stop] step=&step_name.;
    %put NOTE: [fw_log_stop] step_rc=&step_rc.;
    %put NOTE: [fw_log_stop] finished_at=%sysfunc(datetime(), E8601DT19.).;
    %put NOTE:======================================================;

    proc printto;
    run;
%mend fw_log_stop;
