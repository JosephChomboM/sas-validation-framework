/* =========================================================================
steps/methods/metod_9/step_challenge.sas
Step de modulo: Challenge Champion Selector (METOD9)
========================================================================= */
%include "&fw_root./src/common/common_public.sas";
%include "&fw_root./src/modules/challenge/challenge_run.sas";

/* ---- CONFIGURACION DEL MODULO (editar aqui) --------------------------- */
%let challenge_mode=AUTO;

%macro _step_challenge;

    %local _run_challenge _run_gradient_boosting _run_random_forest
        _run_decision_tree _step_rc _step_status;
    %let _step_rc=0;
    %let _step_status=OK;
    %if %symexist(run_challenge)=1 %then %let _run_challenge=&run_challenge.;
    %else %let _run_challenge=0;
    %if %symexist(run_gradient_boosting)=1 %then
        %let _run_gradient_boosting=&run_gradient_boosting.;
    %else %let _run_gradient_boosting=0;
    %if %symexist(run_random_forest)=1 %then
        %let _run_random_forest=&run_random_forest.;
    %else %let _run_random_forest=0;
    %if %symexist(run_decision_tree)=1 %then
        %let _run_decision_tree=&run_decision_tree.;
    %else %let _run_decision_tree=0;

    %fw_log_start(step_name=step_challenge, run_id=&run_id.,
        fw_root=&fw_root., log_stem=metod_9_step_challenge);

    %if &_run_challenge. ne 1 and
        (%eval(&_run_gradient_boosting.=1) or %eval(&_run_random_forest.=1)
        or %eval(&_run_decision_tree.=1))
    %then %do;
        %let _run_challenge=1;
        %put NOTE: [step_challenge] Activado automaticamente porque hay
            algoritmos ML seleccionados
            (gb=&_run_gradient_boosting. rf=&_run_random_forest.
            dt=&_run_decision_tree.).;
    %end;

    %if &_run_challenge. ne 1 %then %do;
        %put NOTE: [step_challenge] Modulo deshabilitado
            (run_challenge=&_run_challenge.). Saltando.;
        %let _step_status=SKIP;
        %goto _step_challenge_end;
    %end;

    %_create_caslib(cas_path=&fw_root./data/processed, caslib_name=PROC,
        lib_caslib=PROC, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1);
    %_create_caslib(cas_path=&fw_root./outputs/runs/&run_id., caslib_name=OUT,
        lib_caslib=OUT, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1);

    %put NOTE: [step_challenge] Iniciando - scope=&ctx_scope.
        mode=&challenge_mode.;

    %if %upcase(&ctx_scope.)=SEGMENTO %then %do;
        %if &ctx_n_segments.=0 %then %do;
            %put WARNING: [step_challenge] Troncal &ctx_troncal_id. tiene 0
                segmentos. Nada que ejecutar.;
        %end;
        %else %if %upcase(&ctx_seg_id.) ne ALL %then %do;
            %challenge_run(troncal_id=&ctx_troncal_id.,
                scope=seg%sysfunc(putn(&ctx_seg_id., z3.)), run_id=&run_id.);
        %end;
        %else %do;
            %do _sg=1 %to &ctx_n_segments.;
                %challenge_run(troncal_id=&ctx_troncal_id.,
                    scope=seg%sysfunc(putn(&_sg., z3.)), run_id=&run_id.);
            %end;
        %end;
    %end;
    %else %if %upcase(&ctx_scope.)=UNIVERSO %then %do;
        %challenge_run(troncal_id=&ctx_troncal_id., scope=base,
            run_id=&run_id.);
    %end;
    %else %do;
        %put ERROR: [step_challenge] ctx_scope=&ctx_scope. no reconocido.;
    %end;

    %put NOTE:======================================================;
    %put NOTE: [step_challenge] Completado (scope=&ctx_scope.
        mode=&challenge_mode.);
    %put NOTE:======================================================;

    %_drop_caslib(caslib_name=OUT, cas_sess_name=conn, del_prom_tables=1);
    %_drop_caslib(caslib_name=PROC, cas_sess_name=conn, del_prom_tables=1);

%_step_challenge_end:
    %fw_log_stop(step_name=step_challenge, step_rc=&_step_rc.,
        step_status=&_step_status);

%mend _step_challenge;
%_step_challenge;
