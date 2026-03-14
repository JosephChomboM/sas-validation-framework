/* =========================================================================
steps/methods/metod_4/step_bootstrap.sas
Step de modulo: Bootstrapping de Coeficientes (Metodo 4.3)

Flujo:
1) Check flag run_bootstrap (skip si deshabilitado)
2) Configuracion propia del modulo (boot_mode, nrounds, seed, etc.)
3) Crear CASLIBs PROC + OUT
4) Iteracion segun ctx_scope:
   - SEGMENTO -> itera segmentos via run_module(dual_input=1)
   - UNIVERSO -> ejecuta base via run_module(dual_input=1)
5) Cleanup CASLIBs

NOTA IMPORTANTE:
Bootstrap compara TRAIN vs OOT -> usa run_module con dual_input=1.
run_module en modo B promueve ambas tablas (_train_input, _oot_input),
ejecuta %bootstrap_run, y dropea ambas tablas promovidas.

Funcionalidad:
- PROC SURVEYSELECT bootstrap estratificado por target
- PROC LOGISTIC por replicado (no-ponderada: BY Replicate, rapido)
  o con rebalanceo (ponderada: loop por iteracion)
- Analisis de estabilidad de signo de coeficientes
- Intervalos de confianza bootstrapeados (p5-p95)
- Pesos por variable (|beta * mean_diff|)
- Comparacion betas TRAIN vs OOT con alertas
- Reportes: Excel multi-hoja + HTML + JPEG

Dependencias:
- &ctx_scope (SEGMENTO | UNIVERSO) - seteado por context_and_modules.sas
- &run_bootstrap (0|1) - seteado por context_and_modules.sas
- &ctx_troncal_id - contexto comun
- SEGMENTO: &ctx_n_segments, &ctx_seg_id (ALL|N)
- casuser.cfg_troncales / cfg_segmentos (promovidas en Step 02)
- &fw_root., &run_id (Steps 01 y 02)

Cada step es independiente: carga sus propias dependencias.
========================================================================= */
/* ---- Dependencias ----------------------------------------------------- */
%include "&fw_root./src/common/common_public.sas";
%include "&fw_root./src/dispatch/run_module.sas";

/* ---- CONFIGURACION DEL MODULO (editar aqui) --------------------------- */
/* boot_mode:
    AUTO   -> usa variables de cfg_segmentos/cfg_troncales
              (num_list / num_unv) + target + def_cld de config.
              seed=12345 por defecto.
              Outputs van a reports/ + images/ + tables/ (validacion estandar).
    CUSTOM -> usa boot_custom_vars + target + def_cld de config.
              Outputs van a experiments/ (analisis exploratorio).            */
%let boot_mode = AUTO;

/* Numero de rondas bootstrap (iteraciones)                                */
%let boot_nrounds = 100;

/* Seed para PROC SURVEYSELECT (reproducibilidad)                          */
%let boot_seed = 12345;

/* Tasa de muestreo (proporcion de observaciones por replicado)            */
%let boot_samprate = 1;

/* Logistica ponderada: 0=estandar, 1=rebalanceo por evento               */
%let boot_ponderada = 0;

/* Variables personalizadas (solo si boot_mode=CUSTOM)
   Lista separada por espacios. target y def_cld siempre vienen de config. */
%let boot_custom_vars = ;

/* ---- EJECUCION -------------------------------------------------------- */
%macro _step_bootstrap;
    %local _step_rc;
    %let _step_rc=0;

    %fw_log_start(step_name=step_bootstrap, run_id=&run_id.,
        fw_root=&fw_root., log_stem=metod_4_step_bootstrap);

    /* ---- 0) Check flag de habilitacion -------------------------------- */
    %if &run_bootstrap. ne 1 %then %do;
        %put NOTE: [step_bootstrap] Modulo deshabilitado
            (run_bootstrap=&run_bootstrap.). Saltando.;
        %goto _step_bootstrap_end;
    %end;

    %put NOTE: [step_bootstrap] Iniciando - scope=&ctx_scope.
        boot_mode=&boot_mode. nrounds=&boot_nrounds. seed=&boot_seed.;

    /* ---- 1) Crear CASLIBs PROC + OUT --------------------------------- */
    %_create_caslib(cas_path=&fw_root./data/processed, caslib_name=PROC,
        lib_caslib=PROC, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1);
    %_create_caslib(cas_path=&fw_root./outputs/runs/&run_id., caslib_name=OUT,
        lib_caslib=OUT, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1);

    /* ---- 2) Iterar segun ctx_scope ----------------------------------- */
    %if %upcase(&ctx_scope.) = SEGMENTO %then %do;

        %put NOTE: [step_bootstrap] SEGMENTO: troncal=&ctx_troncal_id.
            n_segments=&ctx_n_segments. seg_id=&ctx_seg_id.;

        %if &ctx_n_segments. = 0 %then %do;
            %put WARNING: [step_bootstrap] Troncal &ctx_troncal_id. tiene 0
                segmentos. Nada que ejecutar.;
        %end;
        %else %if %upcase(&ctx_seg_id.) ne ALL %then %do;
            /* Segmento especifico */
            %run_module(module=bootstrap, troncal_id=&ctx_troncal_id., split=,
                seg_id=&ctx_seg_id., run_id=&run_id., dual_input=1);
        %end;
        %else %do;
            /* Todos los segmentos */
            %do _sg = 1 %to &ctx_n_segments.;
                %run_module(module=bootstrap, troncal_id=&ctx_troncal_id.,
                    split=, seg_id=&_sg., run_id=&run_id., dual_input=1);
            %end;
        %end;

    %end; /* fin SEGMENTO */
    %else %if %upcase(&ctx_scope.) = UNIVERSO %then %do;

        %put NOTE: [step_bootstrap] UNIVERSO: troncal=&ctx_troncal_id.;

        %run_module(module=bootstrap, troncal_id=&ctx_troncal_id., split=,
            seg_id=, run_id=&run_id., dual_input=1);

    %end; /* fin UNIVERSO */
    %else %do;
        %put ERROR: [step_bootstrap] ctx_scope=&ctx_scope. no reconocido.
            Debe ser SEGMENTO o UNIVERSO.;
    %end;

    /* ---- 3) Cleanup CASLIBs ------------------------------------------ */
    %_drop_caslib(caslib_name=OUT, cas_sess_name=conn, del_prom_tables=1);
    %_drop_caslib(caslib_name=PROC, cas_sess_name=conn, del_prom_tables=1);

    %put NOTE:======================================================;
    %put NOTE: [step_bootstrap] Completado (scope=&ctx_scope.
        mode=&boot_mode.);
    %put NOTE:======================================================;

%_step_bootstrap_end:
    %fw_log_stop(step_name=step_bootstrap, step_rc=&_step_rc.);

%mend _step_bootstrap;
%_step_bootstrap;
