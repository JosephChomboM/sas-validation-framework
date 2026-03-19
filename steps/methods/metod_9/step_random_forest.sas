/* =========================================================================
steps/methods/metod_9/step_random_forest.sas
Step de modulo: Random Forest Challenge (METOD9)
========================================================================= */
%include "&fw_root./src/common/common_public.sas";
%include "&fw_root./src/dispatch/run_module.sas";

/* ---- CONFIGURACION DEL MODULO (editar aqui) --------------------------- */
%let rf_mode=AUTO;
%let rf_score_source=AUTO;
%let rf_top_k=5;
%let rf_top_models=5;
%let rf_penalty_lambda=0.5;
%let rf_presample_enabled=AUTO;
%let rf_presample_max_cells=25000000;
%let rf_partition_pct=70;
%let rf_seed=12345;
%let rf_ntrees_cap=350;
%let rf_maxdepth_cap=30;

/* Overrides CUSTOM */
%let rf_custom_vars_num=;
%let rf_custom_vars_cat=;
%let rf_custom_target=;
%let rf_custom_score_var=;

%macro _step_random_forest;

    %local _run_random_forest _step_rc _step_status;
    %let _step_rc=0;
    %let _step_status=OK;
    %if %symexist(run_random_forest)=1 %then
        %let _run_random_forest=&run_random_forest.;
    %else %let _run_random_forest=0;

    %fw_log_start(step_name=step_random_forest, run_id=&run_id.,
        fw_root=&fw_root., log_stem=metod_9_step_random_forest);

    %if &_run_random_forest. ne 1 %then %do;
        %put NOTE: [step_random_forest] Modulo deshabilitado
            (run_random_forest=&_run_random_forest.). Saltando.;
        %let _step_status=SKIP;
        %goto _step_random_forest_end;
    %end;

    %put NOTE: [step_random_forest] Iniciando - scope=&ctx_scope.
        mode=&rf_mode. score_source=&rf_score_source.
        top_k=&rf_top_k. top_models=&rf_top_models.
        workers=5;

    %_create_caslib(cas_path=&fw_root./data/processed, caslib_name=PROC,
        lib_caslib=PROC, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1);
    %_create_caslib(cas_path=&fw_root./outputs/runs/&run_id., caslib_name=OUT,
        lib_caslib=OUT, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1);

    %if %upcase(&ctx_scope.)=SEGMENTO %then %do;
        %if &ctx_n_segments.=0 %then %do;
            %put WARNING: [step_random_forest] Troncal &ctx_troncal_id. tiene 0
                segmentos. Nada que ejecutar.;
        %end;
        %else %if %upcase(&ctx_seg_id.) ne ALL %then %do;
            %run_module(module=random_forest,
                troncal_id=&ctx_troncal_id., split=, seg_id=&ctx_seg_id.,
                run_id=&run_id., dual_input=1);
        %end;
        %else %do;
            %do _sg=1 %to &ctx_n_segments.;
                %run_module(module=random_forest,
                    troncal_id=&ctx_troncal_id.,
                    split=, seg_id=&_sg., run_id=&run_id., dual_input=1);
            %end;
        %end;
    %end;
    %else %if %upcase(&ctx_scope.)=UNIVERSO %then %do;
        %if &ctx_n_segments. > 0 %then %do;
            %put NOTE: [step_random_forest] UNIVERSO con segmentacion.
                Se entrenaran challengers por segmento para el champion
                global.;
            %do _sg=1 %to &ctx_n_segments.;
                %run_module(module=random_forest,
                    troncal_id=&ctx_troncal_id., split=, seg_id=&_sg.,
                    run_id=&run_id., dual_input=1);
            %end;
        %end;
        %else %do;
            %run_module(module=random_forest, troncal_id=&ctx_troncal_id.,
                split=, seg_id=, run_id=&run_id., dual_input=1);
        %end;
    %end;
    %else %do;
        %put ERROR: [step_random_forest] ctx_scope=&ctx_scope.
            no reconocido.;
    %end;

    %_drop_caslib(caslib_name=OUT, cas_sess_name=conn, del_prom_tables=1);
    %_drop_caslib(caslib_name=PROC, cas_sess_name=conn, del_prom_tables=1);

    %put NOTE:======================================================;
    %put NOTE: [step_random_forest] Completado (scope=&ctx_scope.
        mode=&rf_mode.);
    %put NOTE:======================================================;

%_step_random_forest_end:
    %fw_log_stop(step_name=step_random_forest, step_rc=&_step_rc.,
        step_status=&_step_status);

%mend _step_random_forest;
%_step_random_forest;
