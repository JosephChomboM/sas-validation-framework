/* =========================================================================
   steps/methods/metod_4/step_psi.sas
   Step de modulo: PSI - Population Stability Index (Metodo 4.2)

   Dependencias:
     - &ctx_scope (SEGMENTO | UNIVERSO) - seteado por context_and_modules.sas
     - &run_psi (0|1) - seteado por context_and_modules.sas
     - &ctx_troncal_id - contexto comun
     - SEGMENTO: &ctx_n_segments, &ctx_seg_id (ALL|N)
     - casuser.cfg_troncales / cfg_segmentos (promovidas en Step 02)
     - &fw_root., &run_id (Steps 01 y 02)

   PSI compara TRAIN vs OOT -> necesita ambos splits simultaneamente.
   Usa run_module con dual_input=1 (promueve train + oot automaticamente).
   El split del contexto se ignora (PSI siempre usa train+oot).

   Cada step es independiente: carga sus propias dependencias.
   ========================================================================= */

/* ---- Dependencias ----------------------------------------------------- */
%include "&fw_root./src/common/common_public.sas";
%include "&fw_root./src/dispatch/run_module.sas";

/* ---- CONFIGURACION DEL MODULO (editar aqui) --------------------------- */

/* psi_mode:
     AUTO   -> usa variables de cfg_segmentos/cfg_troncales
              (num_list, cat_list, mes_var).
              Outputs van a reports/ + tables/ + images/ (validacion estandar).
     CUSTOM -> usa psi_custom_vars_num/cat y psi_custom_byvar.
              Outputs van a experiments/ (analisis exploratorio).           */
%let psi_mode = AUTO;

/* Numero de bins para discretizacion de variables continuas              */
%let psi_n_buckets = 10;

/* Calcular PSI mensual (1) o solo total (0)                              */
%let psi_mensual = 1;

/* Variables personalizadas (solo si psi_mode=CUSTOM)                     */
%let psi_custom_vars_num = ;
%let psi_custom_vars_cat = ;
%let psi_custom_byvar    = ;

/* ---- EJECUCION -------------------------------------------------------- */
%macro _step_psi;

    /* ---- 0) Check flag de habilitacion -------------------------------- */
    %if &run_psi. ne 1 %then %do;
        %put NOTE: [step_psi] Modulo deshabilitado (run_psi=&run_psi.). Saltando.;
        %return;
    %end;

    %put NOTE: [step_psi] Iniciando - scope=&ctx_scope. psi_mode=&psi_mode.;
    %put NOTE: [step_psi] PSI siempre compara TRAIN vs OOT (ignora split del contexto).;

    /* ---- 1) Crear CASLIBs PROC + OUT --------------------------------- */
    %_create_caslib(
        cas_path     = &fw_root./data/processed,
        caslib_name  = PROC,
        lib_caslib   = PROC,
        global       = Y,
        cas_sess_name = conn,
        term_global_sess = 0,
        subdirs_flg  = 1
    );
    %_create_caslib(
        cas_path     = &fw_root./outputs/runs/&run_id.,
        caslib_name  = OUT,
        lib_caslib   = OUT,
        global       = Y,
        cas_sess_name = conn,
        term_global_sess = 0,
        subdirs_flg  = 1
    );

    /* ---- 2) Iterar segun ctx_scope ----------------------------------- */
    %if %upcase(&ctx_scope.) = SEGMENTO %then %do;

        %put NOTE: [step_psi] SEGMENTO: troncal=&ctx_troncal_id.
            n_segments=&ctx_n_segments. seg_id=&ctx_seg_id.;

        %if &ctx_n_segments. = 0 %then %do;
            %put WARNING: [step_psi] Troncal &ctx_troncal_id. tiene
                0 segmentos. Nada que ejecutar.;
        %end;
        %else %if %upcase(&ctx_seg_id.) ne ALL %then %do;
            /* Segmento especifico */
            %run_module(module=psi, troncal_id=&ctx_troncal_id.,
                seg_id=&ctx_seg_id., run_id=&run_id., dual_input=1);
        %end;
        %else %do;
            /* Todos los segmentos */
            %do _sg = 1 %to &ctx_n_segments.;
                %run_module(module=psi, troncal_id=&ctx_troncal_id.,
                    seg_id=&_sg., run_id=&run_id., dual_input=1);
            %end;
        %end;

    %end; /* fin SEGMENTO */
    %else %if %upcase(&ctx_scope.) = UNIVERSO %then %do;

        %put NOTE: [step_psi] UNIVERSO: troncal=&ctx_troncal_id.;

        %run_module(module=psi, troncal_id=&ctx_troncal_id.,
            seg_id=, run_id=&run_id., dual_input=1);

    %end; /* fin UNIVERSO */
    %else %do;
        %put ERROR: [step_psi] ctx_scope=&ctx_scope. no reconocido.
            Debe ser SEGMENTO o UNIVERSO.;
    %end;

    /* ---- 3) Cleanup CASLIBs ------------------------------------------ */
    %_drop_caslib(caslib_name=OUT,  cas_sess_name=conn, del_prom_tables=1);
    %_drop_caslib(caslib_name=PROC, cas_sess_name=conn, del_prom_tables=1);

    %put NOTE: ======================================================;
    %put NOTE: [step_psi] Completado (scope=&ctx_scope. mode=&psi_mode.);
    %put NOTE: ======================================================;

%mend _step_psi;
%_step_psi;
