/* =========================================================================
steps/methods/metod_9/step_decision_tree.sas
Step de modulo: Decision Tree Challenge (METOD9)
========================================================================= */
%include "&fw_root./src/common/common_public.sas";
%include "&fw_root./src/dispatch/run_module.sas";

/* ---- CONFIGURACION DEL MODULO (editar aqui) --------------------------- */
%let dt_mode=AUTO;
%let dt_score_source=AUTO;
%let dt_top_k=5;
%let dt_top_models=5;
%let dt_penalty_lambda=0.5;
%let dt_presample_enabled=AUTO;
%let dt_presample_max_cells=25000000;
%let dt_partition_pct=70;
%let dt_seed=12345;
%let dt_maxdepth_cap=12;
%let dt_numbin_cap=100;

/* Overrides CUSTOM */
%let dt_custom_vars_num=;
%let dt_custom_vars_cat=;
%let dt_custom_target=;
%let dt_custom_score_var=;

%macro _step_decision_tree;

    %local _run_decision_tree _step_rc _step_status;
    %let _step_rc=0;
    %let _step_status=OK;
    %if %symexist(run_decision_tree)=1 %then
        %let _run_decision_tree=&run_decision_tree.;
    %else %let _run_decision_tree=0;

    %fw_log_start(step_name=step_decision_tree, run_id=&run_id.,
        fw_root=&fw_root., log_stem=metod_9_step_decision_tree);

    %if &_run_decision_tree. ne 1 %then %do;
        %put NOTE: [step_decision_tree] Modulo deshabilitado
            (run_decision_tree=&_run_decision_tree.). Saltando.;
        %let _step_status=SKIP;
        %goto _step_decision_tree_end;
    %end;

    %put NOTE: [step_decision_tree] Iniciando - scope=&ctx_scope.
        mode=&dt_mode. score_source=&dt_score_source.
        top_k=&dt_top_k. top_models=&dt_top_models.
        workers=5;

    %_create_caslib(cas_path=&fw_root./data/processed, caslib_name=PROC,
        lib_caslib=PROC, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1);
    %_create_caslib(cas_path=&fw_root./outputs/runs/&run_id., caslib_name=OUT,
        lib_caslib=OUT, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1);

    %if %upcase(&ctx_scope.)=SEGMENTO %then %do;
        %if &ctx_n_segments.=0 %then %do;
            %put WARNING: [step_decision_tree] Troncal &ctx_troncal_id. tiene 0
                segmentos. Nada que ejecutar.;
        %end;
        %else %if %upcase(&ctx_seg_id.) ne ALL %then %do;
            %run_module(module=decision_tree,
                troncal_id=&ctx_troncal_id., split=, seg_id=&ctx_seg_id.,
                run_id=&run_id., dual_input=1);
        %end;
        %else %do;
            %do _sg=1 %to &ctx_n_segments.;
                %run_module(module=decision_tree,
                    troncal_id=&ctx_troncal_id.,
                    split=, seg_id=&_sg., run_id=&run_id., dual_input=1);
            %end;
        %end;
    %end;
    %else %if %upcase(&ctx_scope.)=UNIVERSO %then %do;
        %if &ctx_n_segments. > 0 %then %do;
            %put NOTE: [step_decision_tree] UNIVERSO con segmentacion.
                Se entrenaran challengers por segmento para el champion
                global.;
            %do _sg=1 %to &ctx_n_segments.;
                %run_module(module=decision_tree,
                    troncal_id=&ctx_troncal_id., split=, seg_id=&_sg.,
                    run_id=&run_id., dual_input=1);
            %end;
        %end;
        %else %do;
            %run_module(module=decision_tree, troncal_id=&ctx_troncal_id.,
                split=, seg_id=, run_id=&run_id., dual_input=1);
        %end;
    %end;
    %else %do;
        %put ERROR: [step_decision_tree] ctx_scope=&ctx_scope.
            no reconocido.;
    %end;

    %_drop_caslib(caslib_name=OUT, cas_sess_name=conn, del_prom_tables=1);
    %_drop_caslib(caslib_name=PROC, cas_sess_name=conn, del_prom_tables=1);

    %put NOTE:======================================================;
    %put NOTE: [step_decision_tree] Completado (scope=&ctx_scope.
        mode=&dt_mode.);
    %put NOTE:======================================================;

%_step_decision_tree_end:
    %fw_log_stop(step_name=step_decision_tree, step_rc=&_step_rc.,
        step_status=&_step_status);

%mend _step_decision_tree;
%_step_decision_tree;
