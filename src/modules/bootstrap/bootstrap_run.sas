/* =========================================================================
bootstrap_run.sas - Macro publica del modulo Bootstrap (Metodo 4.3)

API:
%bootstrap_run(
    input_caslib  = PROC,
    train_table   = _train_input,
    oot_table     = _oot_input,
    output_caslib = OUT,
    troncal_id    = <id>,
    scope         = base | segNNN,
    run_id        = <run_id>
)

Flujo interno:
1) Resolver variables desde cfg_troncales/cfg_segmentos
   (target, lista_variables num, def_cld, byvar)
2) Ejecutar contract (validaciones)
3) Copiar CAS -> work con filtro def_cld y keep= (Pattern B, optimizado)
4) Calcular bootstrap (_boot_compute)
5) Generar reportes HTML + Excel + JPEG (_boot_report)
6) Persistir tablas como .sas7bdat
7) Cleanup work

NOTA: Bootstrap usa target -> fecha de corte es def_cld.

Dual-input: recibe train + oot promovidas por run_module(dual_input=1).

Compatibilidad: segmento y universo.

Modos de ejecucion (configurados en step_bootstrap.sas):
AUTO   - resuelve vars desde config (cfg_segmentos.num_list,
         fallback cfg_troncales.num_unv + target). seed=12345.
CUSTOM - usa boot_custom_vars (lista manual) + target de config.
         Outputs van a experiments/ (analisis exploratorio).
========================================================================= */
/* ---- Incluir componentes del modulo ---------------------------------- */
%include "&fw_root./src/modules/bootstrap/bootstrap_contract.sas";
%include "&fw_root./src/modules/bootstrap/impl/bootstrap_compute.sas";
%include "&fw_root./src/modules/bootstrap/impl/bootstrap_report.sas";

%macro bootstrap_run(input_caslib=PROC, train_table=_train_input,
    oot_table=_oot_input, output_caslib=OUT, troncal_id=, scope=, run_id=);

    /* ---- Return code -------------------------------------------------- */
    %global _boot_rc;
    %let _boot_rc = 0;

    %local _boot_vars _boot_target _boot_byvar _boot_def_cld
        _report_path _images_path _tables_path _file_prefix _tbl_prefix
        _scope_abbr _boot_is_custom _seg_num _dir_rc;

    %put NOTE:======================================================;
    %put NOTE: [bootstrap_run] INICIO;
    %put NOTE: troncal=&troncal_id. scope=&scope.;
    %put NOTE: train=&input_caslib..&train_table.;
    %put NOTE: oot=&input_caslib..&oot_table.;
    %put NOTE:======================================================;

    /* ==================================================================
    1) Resolver variables
    ================================================================== */
    %let _boot_vars = ;
    %let _boot_target = ;
    %let _boot_byvar = ;
    %let _boot_def_cld = ;
    %let _boot_is_custom = 0;

    /* --- Resolver target, byvar y def_cld del troncal (siempre) ------- */
    proc sql noprint;
        select strip(target) into :_boot_target trimmed
        from casuser.cfg_troncales where troncal_id = &troncal_id.;

        select strip(byvar) into :_boot_byvar trimmed
        from casuser.cfg_troncales where troncal_id = &troncal_id.;

        select def_cld into :_boot_def_cld trimmed
        from casuser.cfg_troncales where troncal_id = &troncal_id.;
    quit;

    /* --- Modo CUSTOM: variables personalizadas ------------------------- */
    %if %upcase(&boot_mode.) = CUSTOM %then %do;
        %if %length(%superq(boot_custom_vars)) > 0 %then %do;
            %let _boot_vars = &boot_custom_vars.;
            %let _boot_is_custom = 1;
            %put NOTE: [bootstrap_run] Modo CUSTOM activado.;
        %end;
        %else %do;
            %put WARNING: [bootstrap_run] boot_mode=CUSTOM pero sin variables
                custom. Fallback a AUTO.;
        %end;
    %end;

    /* --- Modo AUTO (o fallback): variables de configuracion ------------ */
    %if &_boot_is_custom. = 0 %then %do;
        %put NOTE: [bootstrap_run] Modo AUTO - resolviendo vars desde config.;

        /* Si es segmento, intentar override desde cfg_segmentos */
        %if %substr(&scope., 1, 3) = seg %then %do;
            %let _seg_num = %sysfunc(inputn(%substr(&scope., 4), best.));

            proc sql noprint;
                select strip(num_list) into :_boot_vars trimmed
                from casuser.cfg_segmentos
                where troncal_id = &troncal_id. and seg_id = &_seg_num.;
            quit;
        %end;

        /* Fallback a vars del troncal si no hay override */
        %if %length(%superq(_boot_vars)) = 0 %then %do;
            proc sql noprint;
                select strip(num_unv) into :_boot_vars trimmed
                from casuser.cfg_troncales where troncal_id = &troncal_id.;
            quit;
        %end;
    %end;

    %put NOTE: [bootstrap_run] Variables resueltas:;
    %put NOTE: [bootstrap_run] target=&_boot_target.;
    %put NOTE: [bootstrap_run] byvar=&_boot_byvar.;
    %put NOTE: [bootstrap_run] vars=&_boot_vars.;
    %put NOTE: [bootstrap_run] def_cld=&_boot_def_cld.;

    /* ==================================================================
    Determinar rutas de salida
    ================================================================== */
    %if %substr(&scope., 1, 3) = seg %then %let _scope_abbr = &scope.;
    %else %let _scope_abbr = base;

    %if &_boot_is_custom. = 1 %then %do;
        %let _report_path = &fw_root./outputs/runs/&run_id./experiments;
        %let _images_path = &fw_root./outputs/runs/&run_id./experiments;
        %let _tables_path = &fw_root./outputs/runs/&run_id./experiments;
        %let _file_prefix = custom_boot_troncal_&troncal_id._&_scope_abbr.;
        %let _tbl_prefix  = cx_boot_t&troncal_id._&_scope_abbr.;
        %put NOTE: [bootstrap_run] Output -> experiments/ (exploratorio);
    %end;
    %else %do;
        %let _report_path = &fw_root./outputs/runs/&run_id./reports/METOD4.3;
        %let _images_path = &fw_root./outputs/runs/&run_id./images/METOD4.3;
        %let _tables_path = &fw_root./outputs/runs/&run_id./tables/METOD4.3;
        %let _file_prefix = boot_troncal_&troncal_id._&_scope_abbr.;
        %let _tbl_prefix  = boot_t&troncal_id._&_scope_abbr.;
        %put NOTE: [bootstrap_run] Output -> reports/METOD4.3/;
    %end;

    /* ==================================================================
    2) Contract - validaciones
    ================================================================== */
    %bootstrap_contract(input_caslib=&input_caslib., train_table=&train_table.,
        oot_table=&oot_table., lista_variables=&_boot_vars.,
        target=&_boot_target.);

    %if &_boot_rc. ne 0 %then %do;
        %put ERROR: [bootstrap_run] Contract fallido - modulo abortado.;
        %return;
    %end;

    /* ==================================================================
    3) Copiar CAS -> work con filtro def_cld y keep= (Pattern B)
    Optimizacion: solo columnas necesarias, filtro de fecha de cierre
    ================================================================== */
    %if %length(&_boot_def_cld.) > 0 and %length(&_boot_byvar.) > 0
    %then %do;
        data work._boot_train;
            set &input_caslib..&train_table.(
                keep=&_boot_vars. &_boot_target. &_boot_byvar.);
            where &_boot_byvar. <= &_boot_def_cld.;
        run;

        data work._boot_oot;
            set &input_caslib..&oot_table.(
                keep=&_boot_vars. &_boot_target. &_boot_byvar.);
            where &_boot_byvar. <= &_boot_def_cld.;
        run;
    %end;
    %else %do;
        data work._boot_train;
            set &input_caslib..&train_table.(
                keep=&_boot_vars. &_boot_target.);
        run;

        data work._boot_oot;
            set &input_caslib..&oot_table.(
                keep=&_boot_vars. &_boot_target.);
        run;
    %end;

    /* ==================================================================
    4) Compute: bootstrap
    ================================================================== */
    %_boot_compute(
        train_data=work._boot_train,
        oot_data=work._boot_oot,
        lista_variables=&_boot_vars.,
        target=&_boot_target.,
        nrounds=&boot_nrounds.,
        samprate=&boot_samprate.,
        seed=&boot_seed.,
        ponderada=&boot_ponderada.
    );

    /* ==================================================================
    5) Crear directorios de salida
    ================================================================== */
    %if &_boot_is_custom. = 0 %then %do;
        %let _dir_rc = %sysfunc(dcreate(
            METOD4.3, &fw_root./outputs/runs/&run_id./reports));
        %let _dir_rc = %sysfunc(dcreate(
            METOD4.3, &fw_root./outputs/runs/&run_id./images));
        %let _dir_rc = %sysfunc(dcreate(
            METOD4.3, &fw_root./outputs/runs/&run_id./tables));
    %end;

    /* ==================================================================
    6) Report: HTML + Excel + JPEG
    ================================================================== */
    %_boot_report(
        report_path=&_report_path.,
        images_path=&_images_path.,
        file_prefix=&_file_prefix.
    );

    /* ==================================================================
    7) Persistir tablas como .sas7bdat
    ================================================================== */
    %let _dir_rc = %sysfunc(dcreate(., &_tables_path.));

    libname _outlib "&_tables_path.";

    data _outlib.&_tbl_prefix._rpt;
        set work._boot_report_final;
    run;

    data _outlib.&_tbl_prefix._cubo;
        set work._boot_cubo_wide;
    run;

    libname _outlib clear;

    %put NOTE: [bootstrap_run] Tablas persistidas en &_tables_path.:;
    %put NOTE: [bootstrap_run] &_tbl_prefix._rpt.sas7bdat;
    %put NOTE: [bootstrap_run] &_tbl_prefix._cubo.sas7bdat;

    /* ==================================================================
    8) Cleanup work
    ================================================================== */
    proc datasets library=work nolist nowarn;
        delete _boot_train _boot_oot _boot_tablaout
            _boot_cubo_wide _boot_report_final;
    quit;

    %put NOTE:======================================================;
    %put NOTE: [bootstrap_run] FIN - &_file_prefix. (mode=&boot_mode.);
    %put NOTE:======================================================;

%mend bootstrap_run;
