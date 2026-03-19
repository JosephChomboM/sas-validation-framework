/* =========================================================================
steps/methods/metod_9/step_gradient_boosting.sas
Step de modulo: Gradient Boosting Challenge (METOD9)
========================================================================= */
%include "&fw_root./src/common/common_public.sas";
%include "&fw_root./src/dispatch/run_module.sas";

/* ---- CONFIGURACION DEL MODULO (editar aqui) --------------------------- */
%let gb_mode=AUTO;
%let gb_score_source=AUTO;
%let gb_top_k=5;
%let gb_top_models=5;
%let gb_penalty_lambda=0.5;
%let gb_presample_enabled=AUTO;
%let gb_presample_max_cells=25000000;
%let gb_partition_pct=70;
%let gb_seed=12345;
%let gb_stagnation=0;
%let gb_ntrees_cap=800;

/* Overrides CUSTOM */
%let gb_custom_vars_num=;
%let gb_custom_vars_cat=;
%let gb_custom_target=;
%let gb_custom_score_var=;

%macro _step_gradient_boosting;

    %local _run_gradient_boosting _step_rc _step_status;
    %let _step_rc=0;
    %let _step_status=OK;
    %if %symexist(run_gradient_boosting)=1 %then
        %let _run_gradient_boosting=&run_gradient_boosting.;
    %else %let _run_gradient_boosting=0;

    %fw_log_start(step_name=step_gradient_boosting, run_id=&run_id.,
        fw_root=&fw_root., log_stem=metod_9_step_gradient_boosting);

    %if &_run_gradient_boosting. ne 1 %then %do;
        %put NOTE: [step_gradient_boosting] Modulo deshabilitado
            (run_gradient_boosting=&_run_gradient_boosting.). Saltando.;
        %let _step_status=SKIP;
        %goto _step_gradient_boosting_end;
    %end;

    %put NOTE: [step_gradient_boosting] Iniciando - scope=&ctx_scope.
        mode=&gb_mode. score_source=&gb_score_source.
        top_k=&gb_top_k. top_models=&gb_top_models.
        workers=5;

    %_create_caslib(cas_path=&fw_root./data/processed, caslib_name=PROC,
        lib_caslib=PROC, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1);
    %_create_caslib(cas_path=&fw_root./outputs/runs/&run_id., caslib_name=OUT,
        lib_caslib=OUT, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1);

    %if %upcase(&ctx_scope.)=SEGMENTO %then %do;
        %if &ctx_n_segments.=0 %then %do;
            %put WARNING: [step_gradient_boosting] Troncal &ctx_troncal_id. tiene 0
                segmentos. Nada que ejecutar.;
        %end;
        %else %if %upcase(&ctx_seg_id.) ne ALL %then %do;
            %run_module(module=gradient_boosting,
                troncal_id=&ctx_troncal_id., split=, seg_id=&ctx_seg_id.,
                run_id=&run_id., dual_input=1);
        %end;
        %else %do;
            %do _sg=1 %to &ctx_n_segments.;
                %run_module(module=gradient_boosting,
                    troncal_id=&ctx_troncal_id.,
                    split=, seg_id=&_sg., run_id=&run_id., dual_input=1);
            %end;
        %end;
    %end;
    %else %if %upcase(&ctx_scope.)=UNIVERSO %then %do;
        %if &ctx_n_segments. > 0 %then %do;
            %put NOTE: [step_gradient_boosting] UNIVERSO con segmentacion.
                Se entrenaran challengers por segmento para el champion
                global.;
            %do _sg=1 %to &ctx_n_segments.;
                %run_module(module=gradient_boosting,
                    troncal_id=&ctx_troncal_id., split=, seg_id=&_sg.,
                    run_id=&run_id., dual_input=1);
            %end;
        %end;
        %else %do;
            %run_module(module=gradient_boosting,
                troncal_id=&ctx_troncal_id., split=, seg_id=,
                run_id=&run_id., dual_input=1);
        %end;
    %end;
    %else %do;
        %put ERROR: [step_gradient_boosting] ctx_scope=&ctx_scope.
            no reconocido.;
    %end;

    %_drop_caslib(caslib_name=OUT, cas_sess_name=conn, del_prom_tables=1);
    %_drop_caslib(caslib_name=PROC, cas_sess_name=conn, del_prom_tables=1);

    %put NOTE:======================================================;
    %put NOTE: [step_gradient_boosting] Completado (scope=&ctx_scope.
        mode=&gb_mode.);
    %put NOTE:======================================================;

%_step_gradient_boosting_end:
    %fw_log_stop(step_name=step_gradient_boosting, step_rc=&_step_rc.,
        step_status=&_step_status);

%mend _step_gradient_boosting;
%_step_gradient_boosting;
