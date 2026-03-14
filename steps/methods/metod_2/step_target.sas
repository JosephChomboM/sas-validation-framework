/* =========================================================================
steps/methods/metod_2/step_target.sas
Step de modulo: Target - Describe Target (Metodo 2.1)

Analiza el comportamiento del target (ratio de default): evolutivo RD,
materialidad, bandas +/-2s, target ponderado por monto (promedio y suma),
y ratio normalizado. Usa def_cld como fecha de cierre de default.

Usa run_module con dual_input=1 (compara TRAIN vs OOT en un solo report).

Dependencias:
- &ctx_scope (SEGMENTO | UNIVERSO) - seteado por context_and_modules.sas
- &run_target (0|1) - seteado por context_and_modules.sas
- &ctx_troncal_id - contexto comun
- SEGMENTO: &ctx_n_segments, &ctx_seg_id (ALL|N)
- casuser.cfg_troncales (promovida en Step 02)
- &fw_root., &run_id (Steps 01 y 02)

Cada step es independiente: carga sus propias dependencias.
========================================================================= */
/* ---- Dependencias ----------------------------------------------------- */
%include "&fw_root./src/common/common_public.sas";
%include "&fw_root./src/dispatch/run_module.sas";

/* ---- EJECUCION -------------------------------------------------------- */
%macro _step_target;
    %local _step_rc;
    %let _step_rc=0;

    %fw_log_start(step_name=step_target, run_id=&run_id.,
        fw_root=&fw_root., log_stem=metod_2_step_target);

    /* ---- 0) Check flag de habilitacion -------------------------------- */
    %if &run_target. ne 1 %then %do;
        %put NOTE: [step_target] Modulo deshabilitado
            (run_target=&run_target.). Saltando.;
        %goto _step_target_end;
    %end;

    %put NOTE: [step_target] Iniciando - scope=&ctx_scope.;
    %put NOTE: [step_target] Target usa dual_input=1 (TRAIN + OOT
        combinados). Usa def_cld para default cerrado.;

    /* ---- 1) Crear CASLIBs PROC + OUT --------------------------------- */
    %_create_caslib( cas_path=&fw_root./data/processed, caslib_name=PROC,
        lib_caslib=PROC, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1 );
    %_create_caslib( cas_path=&fw_root./outputs/runs/&run_id., caslib_name=OUT,
        lib_caslib=OUT, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1 );

    /* ---- 2) Iterar segun ctx_scope ----------------------------------- */
    %if %upcase(&ctx_scope.)=SEGMENTO %then %do;

        %put NOTE: [step_target] SEGMENTO: troncal=&ctx_troncal_id.
            n_segments=&ctx_n_segments. seg_id=&ctx_seg_id.;

        %if &ctx_n_segments.=0 %then %do;
            %put WARNING: [step_target] Troncal &ctx_troncal_id. tiene 0
                segmentos. Nada que ejecutar.;
        %end;
        %else %if %upcase(&ctx_seg_id.) ne ALL %then %do;
            /* Segmento especifico */
            %run_module(module=target, troncal_id=&ctx_troncal_id., split=,
                seg_id=&ctx_seg_id., run_id=&run_id., dual_input=1);
        %end;
        %else %do;
            /* Todos los segmentos */
            %do _sg=1 %to &ctx_n_segments.;
                %run_module(module=target, troncal_id=&ctx_troncal_id., split=,
                    seg_id=&_sg., run_id=&run_id., dual_input=1);
            %end;
        %end;

    %end; /* fin SEGMENTO */
    %else %if %upcase(&ctx_scope.)=UNIVERSO %then %do;

        %put NOTE: [step_target] UNIVERSO: troncal=&ctx_troncal_id.;

        %run_module(module=target, troncal_id=&ctx_troncal_id., split=, seg_id=,
            run_id=&run_id., dual_input=1);

    %end; /* fin UNIVERSO */
    %else %do;
        %put ERROR: [step_target] ctx_scope=&ctx_scope. no reconocido. Debe ser
            SEGMENTO o UNIVERSO.;
    %end;

    /* ---- 3) Cleanup CASLIBs ------------------------------------------ */
    %_drop_caslib(caslib_name=OUT, cas_sess_name=conn, del_prom_tables=1);
    %_drop_caslib(caslib_name=PROC, cas_sess_name=conn, del_prom_tables=1);

    %put NOTE:======================================================;
    %put NOTE: [step_target] Completado (scope=&ctx_scope.);
    %put NOTE:======================================================;

%_step_target_end:
    %fw_log_stop(step_name=step_target, step_rc=&_step_rc.);

%mend _step_target;
%_step_target;
