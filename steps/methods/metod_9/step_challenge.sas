/* =========================================================================
steps/methods/metod_9/step_challenge.sas
Step de modulo: Challenge Champion Selector (METOD9)
========================================================================= */
%include "&fw_root./src/common/common_public.sas";
%include "&fw_root./src/modules/challenge/challenge_run.sas";

/* ---- CONFIGURACION DEL MODULO (editar aqui) --------------------------- */
%let challenge_mode=AUTO;

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

%_step_challenge_end:
    %fw_log_stop(step_name=step_challenge, step_rc=&_step_rc.,
        step_status=&_step_status);

%mend _step_challenge;
%_step_challenge;
