/* =========================================================================
   steps/methods/metod_4/step_correlacion.sas
   Step de módulo: Correlación (Método 4.3)

   Dependencias:
     - &ctx_scope (SEGMENTO | UNIVERSO) - seteado por context_and_modules.sas
     - &run_correlacion (0|1) - seteado por context_and_modules.sas
     - &ctx_troncal_id, &ctx_split - contexto común
     - SEGMENTO: &ctx_n_segments, &ctx_seg_id (ALL|N)
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
%let corr_custom_vars=&_id_custom_vars_num.;

/* ---- EJECUCIÓN -------------------------------------------------------- */
%macro _step_correlacion;

    /* ---- 0) Check flag de habilitación ---------------------------------- */
    %if &run_correlacion. ne 1 %then %do;
        %put NOTE: [step_correlacion] Módulo deshabilitado
            (run_correlacion=&run_correlacion.). Saltando.;
        %return;
    %end;

    %put NOTE: [step_correlacion] Iniciando - scope=&ctx_scope.
        corr_mode=&corr_mode.;

    /* ---- 1) Crear CASLIBs PROC + OUT ----------------------------------- */
    %_create_caslib( cas_path=&fw_root./data/processed, caslib_name=PROC,
        lib_caslib=PROC, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1 );
    %_create_caslib( cas_path=&fw_root./outputs/runs/&run_id., caslib_name=OUT,
        lib_caslib=OUT, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1 );

    /* ---- 2) Resolver splits (común para ambos scopes) ------------------- */
    %local _sp1 _sp2;

    %if %upcase(&ctx_split.)=TRAIN %then %do;
        %let _sp1=train;
        %let _sp2= ;
    %end;
    %else %if %upcase(&ctx_split.)=OOT %then %do;
        %let _sp1=oot;
        %let _sp2= ;
    %end;
    %else %do;
        %let _sp1=train;
        %let _sp2=oot;
    %end;

    /* ---- 3) Iterar según ctx_scope -------------------------------------- */
    %if %upcase(&ctx_scope.)=SEGMENTO %then %do;

        %put NOTE: [step_correlacion] SEGMENTO: troncal=&ctx_troncal_id.
            n_segments=&ctx_n_segments. seg_id=&ctx_seg_id. split=&ctx_split.;

        %if &ctx_n_segments.=0 %then %do;
            %put WARNING: [step_correlacion] Troncal &ctx_troncal_id. tiene
                0 segmentos. Nada que ejecutar.;
        %end;
        %else %if %upcase(&ctx_seg_id.) ne ALL %then %do;
            /* Segmento específico */
            %if %superq(_sp1) ne %then %run_module(module=correlacion,
                troncal_id=&ctx_troncal_id., split=&_sp1., seg_id=&ctx_seg_id.,
                run_id=&run_id.);
            %if %superq(_sp2) ne %then %run_module(module=correlacion,
                troncal_id=&ctx_troncal_id., split=&_sp2., seg_id=&ctx_seg_id.,
                run_id=&run_id.);
        %end;
        %else %do;
            /* Todos los segmentos */
            %do _sg=1 %to &ctx_n_segments.;
                %if %superq(_sp1) ne %then %run_module(module=correlacion,
                    troncal_id=&ctx_troncal_id., split=&_sp1., seg_id=&_sg.,
                    run_id=&run_id.);
                %if %superq(_sp2) ne %then %run_module(module=correlacion,
                    troncal_id=&ctx_troncal_id., split=&_sp2., seg_id=&_sg.,
                    run_id=&run_id.);
            %end;
        %end;

    %end; /* fin SEGMENTO */
    %else %if %upcase(&ctx_scope.)=UNIVERSO %then %do;

        %put NOTE: [step_correlacion] UNIVERSO: troncal=&ctx_troncal_id.
            split=&ctx_split.;

        /* Ejecutar base (universo) del troncal */
        %if %superq(_sp1) ne %then %run_module(module=correlacion,
            troncal_id=&ctx_troncal_id., split=&_sp1., seg_id=, run_id=&run_id.);
        %if %superq(_sp2) ne %then %run_module(module=correlacion,
            troncal_id=&ctx_troncal_id., split=&_sp2., seg_id=, run_id=&run_id.);

    %end; /* fin UNIVERSO */
    %else %do;
        %put ERROR: [step_correlacion] ctx_scope=&ctx_scope. no reconocido. Debe
            ser SEGMENTO o UNIVERSO.;
    %end;

    /* ---- 4) Cleanup CASLIBs --------------------------------------------- */
    %_drop_caslib(caslib_name=OUT, cas_sess_name=conn, del_prom_tables=1);
    %_drop_caslib(caslib_name=PROC, cas_sess_name=conn, del_prom_tables=1);

    %put NOTE:======================================================;
    %put NOTE: [step_correlacion] Completado (scope=&ctx_scope.
        mode=&corr_mode.);
    %put NOTE:======================================================;

%mend _step_correlacion;
%_step_correlacion;