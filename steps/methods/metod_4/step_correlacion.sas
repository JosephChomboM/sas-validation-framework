/* =========================================================================
steps/methods/metod_4/step_correlacion.sas
Step de módulo: Correlación (Método 4.3)

Flujo:
1) Check flag run_correlacion (skip si deshabilitado)
2) Configuración propia del módulo (corr_mode, corr_custom_vars)
3) Crear CASLIBs PROC + OUT
4) Iteración según ctx_scope:
- SEGMENTO → itera segmentos del troncal definido en context.sas
- UNIVERSO → corre base del troncal definido en context.sas
5) Cleanup CASLIBs

Dependencias:
- &ctx_scope (SEGMENTO | UNIVERSO) — seteado por context.sas
- &run_correlacion (0|1) — seteado por select_modules.sas
- SEGMENTO: &ctx_segment_troncal_id, &ctx_segment_split,
&ctx_segment_n_segments, &ctx_segment_seg_id (ALL|N)
- UNIVERSO: &ctx_universe_troncal_id, &ctx_universe_split
- casuser.cfg_troncales / cfg_segmentos (promovidas en Step 02)
- &fw_root., &run_id (Steps 01 y 02)

Cada step es independiente: carga sus propias dependencias.
========================================================================= */
/* ---- Dependencias ----------------------------------------------------- */
%include "&fw_root./src/common/common_public.sas";
%include "&fw_root./src/dispatch/run_module.sas";

/* ---- CONFIGURACIÓN DEL MÓDULO (editar aquí) --------------------------- */
/* corr_mode:
AUTO   → usa variables de cfg_segmentos/cfg_troncales (num_list/num_unv)
Outputs van a reports/ y tables/ (validación estándar).
CUSTOM → usa corr_custom_vars (lista manual de variables numéricas)
Outputs van a experiments/ (análisis exploratorio).           */
%let corr_mode=AUTO;
%let corr_custom_vars=;

/* ---- EJECUCIÓN -------------------------------------------------------- */
%macro _step_correlacion;

    /* ---- 0) Check flag de habilitación ---------------------------------- */
    %if &run_correlacion. ne 1 %then %do;
        %put NOTE: [step_correlacion] Módulo deshabilitado
            (run_correlacion=&run_correlacion.). Saltando.;
        %return;
    %end;

    %put NOTE: [step_correlacion] Iniciando — scope=&ctx_scope.
        corr_mode=&corr_mode.;

    /* ---- 1) Crear CASLIBs PROC + OUT ----------------------------------- */
    %_create_caslib( cas_path=&fw_root./data/processed, caslib_name=PROC,
        lib_caslib=PROC, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1 );
    %_create_caslib( cas_path=&fw_root./outputs/runs/&run_id., caslib_name=OUT,
        lib_caslib=OUT, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1 );

    /* ---- 2) Resolver splits --------------------------------------------- */
    %local _sp1 _sp2 _tid _nsg;

    %if %upcase(&ctx_scope.)=SEGMENTO %then %do;

        %let _tid=&ctx_segment_troncal_id.;
        %let _nsg=&ctx_segment_n_segments.;

        %if %upcase(&ctx_segment_split.)=TRAIN %then %do;
            %let _sp1=train;
            %let _sp2= ;
        %end;
        %else %if %upcase(&ctx_segment_split.)=OOT %then %do;
            %let _sp1=oot;
            %let _sp2= ;
        %end;
        %else %do;
            %let _sp1=train;
            %let _sp2=oot;
        %end;

        %put NOTE: [step_correlacion] SEGMENTO: troncal=&_tid. n_segments=&_nsg.
            seg_id=&ctx_segment_seg_id. split=&ctx_segment_split.;

        %if &_nsg.=0 %then %do;
            %put WARNING: [step_correlacion] Troncal &_tid. tiene 0 segmentos.
                Nada que ejecutar.;
        %end;
        %else %if %upcase(&ctx_segment_seg_id.) ne ALL %then %do;
            /* Segmento específico */
            %if %superq(_sp1) ne %then %run_module(module=correlacion,
                troncal_id=&_tid., split=&_sp1., seg_id=&ctx_segment_seg_id.,
                run_id=&run_id.);
            %if %superq(_sp2) ne %then %run_module(module=correlacion,
                troncal_id=&_tid., split=&_sp2., seg_id=&ctx_segment_seg_id.,
                run_id=&run_id.);
        %end;
        %else %do;
            /* Todos los segmentos */
            %do _sg=1 %to &_nsg.;
                %if %superq(_sp1) ne %then %run_module(module=correlacion,
                    troncal_id=&_tid., split=&_sp1., seg_id=&_sg.,
                    run_id=&run_id.);
                %if %superq(_sp2) ne %then %run_module(module=correlacion,
                    troncal_id=&_tid., split=&_sp2., seg_id=&_sg.,
                    run_id=&run_id.);
            %end;
        %end;

    %end; /* fin SEGMENTO */
    %else %if %upcase(&ctx_scope.)=UNIVERSO %then %do;

        %let _tid=&ctx_universe_troncal_id.;

        %if %upcase(&ctx_universe_split.)=TRAIN %then %do;
            %let _sp1=train;
            %let _sp2= ;
        %end;
        %else %if %upcase(&ctx_universe_split.)=OOT %then %do;
            %let _sp1=oot;
            %let _sp2= ;
        %end;
        %else %do;
            %let _sp1=train;
            %let _sp2=oot;
        %end;

        %put NOTE: [step_correlacion] UNIVERSO: troncal=&_tid.
            split=&ctx_universe_split.;

        /* Ejecutar base (universo) del troncal */
        %if %superq(_sp1) ne %then %run_module(module=correlacion,
            troncal_id=&_tid., split=&_sp1., seg_id=, run_id=&run_id.);
        %if %superq(_sp2) ne %then %run_module(module=correlacion,
            troncal_id=&_tid., split=&_sp2., seg_id=, run_id=&run_id.);

    %end; /* fin UNIVERSO */
    %else %do;
        %put ERROR: [step_correlacion] ctx_scope=&ctx_scope. no reconocido. Debe
            ser SEGMENTO o UNIVERSO.;
    %end;

    /* ---- 3) Cleanup CASLIBs --------------------------------------------- */
    %_drop_caslib(caslib_name=OUT, cas_sess_name=conn, del_prom_tables=1);
    %_drop_caslib(caslib_name=PROC, cas_sess_name=conn, del_prom_tables=1);

    %put NOTE:======================================================;
    %put NOTE: [step_correlacion] Completado (scope=&ctx_scope.
        mode=&corr_mode.);
    %put NOTE:======================================================;

%mend _step_correlacion;
%_step_correlacion;
