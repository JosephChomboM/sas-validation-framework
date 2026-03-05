/* =========================================================================
   steps/methods/metod_4/step_psi.sas
   Step de módulo: PSI — Population Stability Index (Método 4.2)

   Flujo:
     1) Check flag run_psi (skip si deshabilitado)
     2) Configuración propia del módulo (psi_mode, n_buckets, variables)
     3) Crear CASLIBs PROC + OUT
     4) Iteración según ctx_scope:
        - SEGMENTO → itera segmentos: promueve train + oot, ejecuta PSI, limpia
        - UNIVERSO → promueve train + oot base, ejecuta PSI, limpia
     5) Cleanup CASLIBs

   NOTA IMPORTANTE:
     PSI compara TRAIN vs OOT → necesita ambos splits simultáneamente.
     Por eso NO usa run_module.sas (que promueve un solo input).
     step_psi promueve ambas tablas directamente vía _promote_castable.
     El split del contexto se ignora (PSI siempre usa train+oot).

   Dependencias:
     - &ctx_scope (SEGMENTO | UNIVERSO) — seteado por context_and_modules.sas
     - &run_psi (0|1) — seteado por context_and_modules.sas
     - &ctx_troncal_id — contexto común
     - SEGMENTO: &ctx_n_segments, &ctx_seg_id (ALL|N)
     - casuser.cfg_troncales / cfg_segmentos (promovidas en Step 02)
     - &fw_root., &run_id (Steps 01 y 02)

   Cada step es independiente: carga sus propias dependencias.
   ========================================================================= */

/* ---- Dependencias ----------------------------------------------------- */
%include "&fw_root./src/common/common_public.sas";

/* ---- CONFIGURACIÓN DEL MÓDULO (editar aquí) --------------------------- */

/* psi_mode:
     AUTO   → usa variables de cfg_segmentos/cfg_troncales
              (num_list, cat_list, mes_var).
              Outputs van a reports/ + tables/ + images/ (validación estándar).
     CUSTOM → usa psi_custom_vars_num/cat y psi_custom_byvar.
              Outputs van a experiments/ (análisis exploratorio).           */
%let psi_mode = AUTO;

/* Número de bins para discretización de variables continuas              */
%let psi_n_buckets = 10;

/* Calcular PSI mensual (1) o solo total (0)                              */
%let psi_mensual = 1;

/* Variables personalizadas (solo si psi_mode=CUSTOM)                     */
%let psi_custom_vars_num = ;
%let psi_custom_vars_cat = ;
%let psi_custom_byvar    = ;

/* ---- EJECUCIÓN -------------------------------------------------------- */
%macro _step_psi;

    /* ---- 0) Check flag de habilitación -------------------------------- */
    %if &run_psi. ne 1 %then %do;
        %put NOTE: [step_psi] Módulo deshabilitado (run_psi=&run_psi.). Saltando.;
        %return;
    %end;

    %put NOTE: [step_psi] Iniciando — scope=&ctx_scope. psi_mode=&psi_mode.;
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

    /* ---- 2) Iterar según ctx_scope ----------------------------------- */
    %local _scope _train_path _oot_path;

    %if %upcase(&ctx_scope.) = SEGMENTO %then %do;

        %put NOTE: [step_psi] SEGMENTO: troncal=&ctx_troncal_id. n_segments=&ctx_n_segments. seg_id=&ctx_seg_id.;

        %if &ctx_n_segments. = 0 %then %do;
            %put WARNING: [step_psi] Troncal &ctx_troncal_id. tiene 0 segmentos. Nada que ejecutar.;
        %end;
        %else %if %upcase(&ctx_seg_id.) ne ALL %then %do;

            /* ---- Segmento específico ---- */
            %let _scope = seg%sysfunc(putn(&ctx_seg_id., z3.));

            %fw_path_processed(outvar=_train_path, troncal_id=&ctx_troncal_id., split=train, seg_id=&ctx_seg_id.);
            %fw_path_processed(outvar=_oot_path,   troncal_id=&ctx_troncal_id., split=oot,   seg_id=&ctx_seg_id.);

            %_promote_castable(m_cas_sess_name=conn, m_input_caslib=PROC,
                m_subdir_data=&_train_path., m_output_caslib=PROC, m_output_data=_psi_train);
            %_promote_castable(m_cas_sess_name=conn, m_input_caslib=PROC,
                m_subdir_data=&_oot_path.,   m_output_caslib=PROC, m_output_data=_psi_oot);

            %psi_run(
                input_caslib = PROC,
                train_table  = _psi_train,
                oot_table    = _psi_oot,
                output_caslib = OUT,
                troncal_id   = &ctx_troncal_id.,
                scope        = &_scope.,
                run_id       = &run_id.
            );

            proc cas;
                session conn;
                table.dropTable / caslib="PROC" name="_psi_train" quiet=true;
                table.dropTable / caslib="PROC" name="_psi_oot"   quiet=true;
            quit;

        %end;
        %else %do;

            /* ---- Todos los segmentos ---- */
            %do _sg = 1 %to &ctx_n_segments.;
                %let _scope = seg%sysfunc(putn(&_sg., z3.));

                %fw_path_processed(outvar=_train_path, troncal_id=&ctx_troncal_id., split=train, seg_id=&_sg.);
                %fw_path_processed(outvar=_oot_path,   troncal_id=&ctx_troncal_id., split=oot,   seg_id=&_sg.);

                %_promote_castable(m_cas_sess_name=conn, m_input_caslib=PROC,
                    m_subdir_data=&_train_path., m_output_caslib=PROC, m_output_data=_psi_train);
                %_promote_castable(m_cas_sess_name=conn, m_input_caslib=PROC,
                    m_subdir_data=&_oot_path.,   m_output_caslib=PROC, m_output_data=_psi_oot);

                %psi_run(
                    input_caslib = PROC,
                    train_table  = _psi_train,
                    oot_table    = _psi_oot,
                    output_caslib = OUT,
                    troncal_id   = &ctx_troncal_id.,
                    scope        = &_scope.,
                    run_id       = &run_id.
                );

                proc cas;
                    session conn;
                    table.dropTable / caslib="PROC" name="_psi_train" quiet=true;
                    table.dropTable / caslib="PROC" name="_psi_oot"   quiet=true;
                quit;
            %end;

        %end;

    %end; /* fin SEGMENTO */
    %else %if %upcase(&ctx_scope.) = UNIVERSO %then %do;

        %put NOTE: [step_psi] UNIVERSO: troncal=&ctx_troncal_id.;

        /* Promote train + oot del universo (base) */
        %fw_path_processed(outvar=_train_path, troncal_id=&ctx_troncal_id., split=train, seg_id=);
        %fw_path_processed(outvar=_oot_path,   troncal_id=&ctx_troncal_id., split=oot,   seg_id=);

        %_promote_castable(m_cas_sess_name=conn, m_input_caslib=PROC,
            m_subdir_data=&_train_path., m_output_caslib=PROC, m_output_data=_psi_train);
        %_promote_castable(m_cas_sess_name=conn, m_input_caslib=PROC,
            m_subdir_data=&_oot_path.,   m_output_caslib=PROC, m_output_data=_psi_oot);

        %psi_run(
            input_caslib  = PROC,
            train_table   = _psi_train,
            oot_table     = _psi_oot,
            output_caslib = OUT,
            troncal_id    = &ctx_troncal_id.,
            scope         = base,
            run_id        = &run_id.
        );

        proc cas;
            session conn;
            table.dropTable / caslib="PROC" name="_psi_train" quiet=true;
            table.dropTable / caslib="PROC" name="_psi_oot"   quiet=true;
        quit;

    %end; /* fin UNIVERSO */
    %else %do;
        %put ERROR: [step_psi] ctx_scope=&ctx_scope. no reconocido. Debe ser SEGMENTO o UNIVERSO.;
    %end;

    /* ---- 3) Cleanup CASLIBs ------------------------------------------ */
    %_drop_caslib(caslib_name=OUT,  cas_sess_name=conn, del_prom_tables=1);
    %_drop_caslib(caslib_name=PROC, cas_sess_name=conn, del_prom_tables=1);

    %put NOTE: ======================================================;
    %put NOTE: [step_psi] Completado (scope=&ctx_scope. mode=&psi_mode.);
    %put NOTE: ======================================================;

%mend _step_psi;
%_step_psi;
