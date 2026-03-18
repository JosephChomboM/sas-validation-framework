/* =========================================================================
steps/methods/metod_9/step_challenge.sas
Step de modulo: Challenge Gradient Boosting (METOD9)
========================================================================= */
%include "&fw_root./src/common/common_public.sas";
%include "&fw_root./src/dispatch/run_module.sas";

/* ---- CONFIGURACION DEL MODULO (editar aqui) --------------------------- */
%let chall_mode=AUTO;
%let chall_score_source=AUTO;
%let chall_top_k=5;
%let chall_top_models=5;
%let chall_penalty_lambda=0.5;
%let chall_presample_enabled=AUTO;
%let chall_presample_max_cells=25000000;
%let chall_partition_pct=70;
%let chall_seed=12345;
%let chall_gb_stagnation=0;
%let chall_gb_ntrees_cap=800;

/* Overrides CUSTOM */
%let chall_custom_vars_num=;
%let chall_custom_vars_cat=;
%let chall_custom_target=;
%let chall_custom_score_var=;

%macro _step_challenge;

    %local _run_challenge _step_rc _step_status;
    %let _step_rc=0;
    %let _step_status=OK;
    %if %symexist(run_challenge)=1 %then %let _run_challenge=&run_challenge.;
    %else %let _run_challenge=0;

    %fw_log_start(step_name=step_challenge, run_id=&run_id.,
        fw_root=&fw_root., log_stem=metod_9_step_challenge);

    %if &_run_challenge. ne 1 %then %do;
        %put NOTE: [step_challenge] Modulo deshabilitado
            (run_challenge=&_run_challenge.). Saltando.;
        %let _step_status=SKIP;
        %goto _step_challenge_end;
    %end;

    %put NOTE: [step_challenge] Iniciando - scope=&ctx_scope.
        mode=&chall_mode. score_source=&chall_score_source.
        top_k=&chall_top_k. top_models=&chall_top_models.
        workers=5;

    %_create_caslib(cas_path=&fw_root./data/processed, caslib_name=PROC,
        lib_caslib=PROC, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1);
    %_create_caslib(cas_path=&fw_root./outputs/runs/&run_id., caslib_name=OUT,
        lib_caslib=OUT, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1);

    %if %upcase(&ctx_scope.)=SEGMENTO %then %do;
        %if &ctx_n_segments.=0 %then %do;
            %put WARNING: [step_challenge] Troncal &ctx_troncal_id. tiene 0
                segmentos. Nada que ejecutar.;
        %end;
        %else %if %upcase(&ctx_seg_id.) ne ALL %then %do;
            %run_module(module=challenge, troncal_id=&ctx_troncal_id., split=,
                seg_id=&ctx_seg_id., run_id=&run_id., dual_input=1);
        %end;
        %else %do;
            %do _sg=1 %to &ctx_n_segments.;
                %run_module(module=challenge, troncal_id=&ctx_troncal_id.,
                    split=, seg_id=&_sg., run_id=&run_id., dual_input=1);
            %end;
        %end;
    %end;
    %else %if %upcase(&ctx_scope.)=UNIVERSO %then %do;
        %run_module(module=challenge, troncal_id=&ctx_troncal_id., split=,
            seg_id=, run_id=&run_id., dual_input=1);
    %end;
    %else %do;
        %put ERROR: [step_challenge] ctx_scope=&ctx_scope. no reconocido.;
    %end;

    %_drop_caslib(caslib_name=OUT, cas_sess_name=conn, del_prom_tables=1);
    %_drop_caslib(caslib_name=PROC, cas_sess_name=conn, del_prom_tables=1);

    %put NOTE:======================================================;
    %put NOTE: [step_challenge] Completado (scope=&ctx_scope.
        mode=&chall_mode.);
    %put NOTE:======================================================;

%_step_challenge_end:
    %fw_log_stop(step_name=step_challenge, step_rc=&_step_rc.,
        step_status=&_step_status);

%mend _step_challenge;
%_step_challenge;
