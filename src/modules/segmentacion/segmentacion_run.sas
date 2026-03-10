/* =========================================================================
segmentacion_run.sas - Macro publica del modulo Segmentacion (Metodo 3)

API:
%segmentacion_run(
    input_caslib  = PROC,
    input_table   = _active_input,
    output_caslib = OUT,
    troncal_id    = <id>,
    split         = train | oot,
    scope         = base | segNNN,
    run_id        = <run_id>
)

Flujo interno:
1) Resolver variables desde cfg_troncales
   (target, var_seg, byvar, id_var_id, def_cld, primer_mes, ultimo_mes)
2) Determinar has_segm flag (si var_seg existe y esta en el data)
3) Ejecutar contract (validaciones)
4) Copiar CAS -> work con filtro def_cld y keep= (Pattern B, optimizado)
5) Calcular segmentacion (_seg_compute)
6) Generar reportes HTML + Excel + JPEG (_seg_report)
7) Persistir tablas como .sas7bdat
8) Cleanup work

NOTA: Segmentacion usa target -> fecha de corte es def_cld.

Single-input: recibe un solo dataset promovido por run_module(dual_input=0).
Cada split (train/oot) se ejecuta independientemente.

Compatibilidad: solo UNIVERSO (necesita datos completos con segmentos).
La validacion de scope se hace en step_segmentacion.sas.

Modos de ejecucion (configurados en step_segmentacion.sas):
AUTO   - resuelve target, var_seg, byvar, id_var_id desde config.
         Outputs van a reports/ + images/ + tables/ (validacion estandar).
CUSTOM - usa seg_custom_* overrides + target/byvar de config.
         Outputs van a experiments/ (analisis exploratorio).
========================================================================= */
/* ---- Incluir componentes del modulo ---------------------------------- */
%include "&fw_root./src/modules/segmentacion/segmentacion_contract.sas";
%include "&fw_root./src/modules/segmentacion/impl/segmentacion_compute.sas";
%include "&fw_root./src/modules/segmentacion/impl/segmentacion_report.sas";

%macro segmentacion_run(input_caslib=PROC, input_table=_active_input,
    output_caslib=OUT, troncal_id=, split=, scope=, run_id=);

    /* ---- Return code -------------------------------------------------- */
    %global _seg_rc;
    %let _seg_rc = 0;

    %local _seg_target _seg_segvar _seg_byvar _seg_idvar _seg_def_cld
        _seg_primer_mes _seg_ultimo_mes _seg_has_segm _seg_has_id
        _report_path _images_path _tables_path _file_prefix _tbl_prefix
        _scope_abbr _spl_abbr _seg_is_custom _seg_has_col _dir_rc;

    %put NOTE:======================================================;
    %put NOTE: [segmentacion_run] INICIO;
    %put NOTE: troncal=&troncal_id. split=&split. scope=&scope.;
    %put NOTE: input=&input_caslib..&input_table.;
    %put NOTE:======================================================;

    /* ==================================================================
    1) Resolver variables
    ================================================================== */
    %let _seg_target = ;
    %let _seg_segvar = ;
    %let _seg_byvar = ;
    %let _seg_idvar = ;
    %let _seg_def_cld = ;
    %let _seg_primer_mes = ;
    %let _seg_ultimo_mes = ;
    %let _seg_is_custom = 0;
    %let _seg_has_segm = 0;
    %let _seg_has_id = 0;

    /* --- Resolver desde cfg_troncales (siempre) ----------------------- */
    proc sql noprint;
        select strip(target) into :_seg_target trimmed
        from casuser.cfg_troncales where troncal_id = &troncal_id.;

        select strip(byvar) into :_seg_byvar trimmed
        from casuser.cfg_troncales where troncal_id = &troncal_id.;

        select def_cld into :_seg_def_cld trimmed
        from casuser.cfg_troncales where troncal_id = &troncal_id.;

        select strip(var_seg) into :_seg_segvar trimmed
        from casuser.cfg_troncales where troncal_id = &troncal_id.;

        select strip(id_var_id) into :_seg_idvar trimmed
        from casuser.cfg_troncales where troncal_id = &troncal_id.;
    quit;

    /* --- Resolver primer/ultimo mes segun split ----------------------- */
    %if %upcase(&split.) = TRAIN %then %do;
        proc sql noprint;
            select train_min_mes into :_seg_primer_mes trimmed
            from casuser.cfg_troncales where troncal_id = &troncal_id.;
            select train_max_mes into :_seg_ultimo_mes trimmed
            from casuser.cfg_troncales where troncal_id = &troncal_id.;
        quit;
    %end;
    %else %do;
        proc sql noprint;
            select oot_min_mes into :_seg_primer_mes trimmed
            from casuser.cfg_troncales where troncal_id = &troncal_id.;
            select oot_max_mes into :_seg_ultimo_mes trimmed
            from casuser.cfg_troncales where troncal_id = &troncal_id.;
        quit;
    %end;

    /* --- Modo CUSTOM: overrides desde step ----------------------------- */
    %if %upcase(&seg_mode.) = CUSTOM %then %do;
        %let _seg_is_custom = 1;
        %put NOTE: [segmentacion_run] Modo CUSTOM activado.;

        %if %length(%superq(seg_custom_target)) > 0 %then
            %let _seg_target = &seg_custom_target.;
        %if %length(%superq(seg_custom_segvar)) > 0 %then
            %let _seg_segvar = &seg_custom_segvar.;
        %if %length(%superq(seg_custom_byvar)) > 0 %then
            %let _seg_byvar = &seg_custom_byvar.;
        %if %length(%superq(seg_custom_idvar)) > 0 %then
            %let _seg_idvar = &seg_custom_idvar.;
    %end;

    %put NOTE: [segmentacion_run] Variables resueltas:;
    %put NOTE: [segmentacion_run] target=&_seg_target.;
    %put NOTE: [segmentacion_run] segvar=&_seg_segvar.;
    %put NOTE: [segmentacion_run] byvar=&_seg_byvar.;
    %put NOTE: [segmentacion_run] idvar=&_seg_idvar.;
    %put NOTE: [segmentacion_run] def_cld=&_seg_def_cld.;
    %put NOTE: [segmentacion_run] primer_mes=&_seg_primer_mes.
        ultimo_mes=&_seg_ultimo_mes.;

    /* ==================================================================
    Determinar rutas de salida
    ================================================================== */
    %if %upcase(&split.) = TRAIN %then %let _spl_abbr = trn;
    %else %let _spl_abbr = oot;

    %if %substr(&scope., 1, 3) = seg %then %let _scope_abbr = &scope.;
    %else %let _scope_abbr = base;

    %if &_seg_is_custom. = 1 %then %do;
        %let _report_path = &fw_root./outputs/runs/&run_id./experiments;
        %let _images_path = &fw_root./outputs/runs/&run_id./experiments;
        %let _tables_path = &fw_root./outputs/runs/&run_id./experiments;
        %let _file_prefix = custom_seg_troncal_&troncal_id._&split._&_scope_abbr.;
        %let _tbl_prefix  = cx_seg_t&troncal_id._&_spl_abbr._&_scope_abbr.;
        %put NOTE: [segmentacion_run] Output -> experiments/ (exploratorio);
    %end;
    %else %do;
        %let _report_path = &fw_root./outputs/runs/&run_id./reports/METOD3;
        %let _images_path = &fw_root./outputs/runs/&run_id./images/METOD3;
        %let _tables_path = &fw_root./outputs/runs/&run_id./tables/METOD3;
        %let _file_prefix = seg_troncal_&troncal_id._&split._&_scope_abbr.;
        %let _tbl_prefix  = seg_t&troncal_id._&_spl_abbr._&_scope_abbr.;
        %put NOTE: [segmentacion_run] Output -> reports/METOD3/;
    %end;

    /* ==================================================================
    2) Contract - validaciones
    ================================================================== */
    %segmentacion_contract(input_caslib=&input_caslib.,
        input_table=&input_table., target=&_seg_target.,
        byvar=&_seg_byvar., segvar=&_seg_segvar., idvar=&_seg_idvar.);

    %if &_seg_rc. ne 0 %then %do;
        %put ERROR: [segmentacion_run] Contract fallido - modulo abortado.;
        %return;
    %end;

    /* ==================================================================
    3) Determinar has_segm y has_id (post-contract, columnas validadas)
    ================================================================== */
    %if %length(%superq(_seg_segvar)) > 0 %then %do;
        %let _seg_has_col = 0;
        proc sql noprint;
            select count(*) into :_seg_has_col trimmed
            from dictionary.columns
            where upcase(libname) = upcase("&input_caslib.")
              and upcase(memname) = upcase("&input_table.")
              and upcase(name) = upcase("&_seg_segvar.");
        quit;
        %if &_seg_has_col. > 0 %then %let _seg_has_segm = 1;
    %end;

    %if %length(%superq(_seg_idvar)) > 0 %then %do;
        %let _seg_has_col = 0;
        proc sql noprint;
            select count(*) into :_seg_has_col trimmed
            from dictionary.columns
            where upcase(libname) = upcase("&input_caslib.")
              and upcase(memname) = upcase("&input_table.")
              and upcase(name) = upcase("&_seg_idvar.");
        quit;
        %if &_seg_has_col. > 0 %then %let _seg_has_id = 1;
    %end;

    %put NOTE: [segmentacion_run] has_segm=&_seg_has_segm.
        has_id=&_seg_has_id.;

    /* ==================================================================
    4) Copiar CAS -> work con filtro def_cld y keep= (Pattern B)
    Solo columnas necesarias para optimizar transferencia.
    ================================================================== */
    %local _keep_list;
    %let _keep_list = &_seg_target. &_seg_byvar.;
    %if &_seg_has_segm. = 1 %then
        %let _keep_list = &_keep_list. &_seg_segvar.;
    %if &_seg_has_id. = 1 %then
        %let _keep_list = &_keep_list. &_seg_idvar.;

    %if %length(&_seg_def_cld.) > 0 and %length(&_seg_byvar.) > 0
    %then %do;
        data work._seg_data;
            set &input_caslib..&input_table.(keep=&_keep_list.);
            where &_seg_byvar. <= &_seg_def_cld.;
        run;
    %end;
    %else %do;
        data work._seg_data;
            set &input_caslib..&input_table.(keep=&_keep_list.);
        run;
    %end;

    /* ==================================================================
    5) Compute: segmentacion
    ================================================================== */
    %_seg_compute(
        data=work._seg_data,
        target=&_seg_target.,
        segvar=&_seg_segvar.,
        byvar=&_seg_byvar.,
        idvar=&_seg_idvar.,
        data_type=%upcase(&split.),
        min_obs=&seg_min_obs.,
        min_target=&seg_min_target.,
        primer_mes=&_seg_primer_mes.,
        ultimo_mes=&_seg_ultimo_mes.,
        has_segm=&_seg_has_segm.
    );

    /* ==================================================================
    6) Crear directorios de salida
    ================================================================== */
    %if &_seg_is_custom. = 0 %then %do;
        %let _dir_rc = %sysfunc(dcreate(
            METOD3, &fw_root./outputs/runs/&run_id./reports));
        %let _dir_rc = %sysfunc(dcreate(
            METOD3, &fw_root./outputs/runs/&run_id./images));
        %let _dir_rc = %sysfunc(dcreate(
            METOD3, &fw_root./outputs/runs/&run_id./tables));
    %end;

    /* ==================================================================
    7) Report: HTML + Excel + JPEG
    ================================================================== */
    %_seg_report(
        report_path=&_report_path.,
        images_path=&_images_path.,
        file_prefix=&_file_prefix.,
        data=work._seg_data,
        target=&_seg_target.,
        byvar=&_seg_byvar.,
        segvar=&_seg_segvar.,
        has_segm=&_seg_has_segm.,
        data_type=%upcase(&split.),
        plot_sep=&seg_plot_sep.
    );

    /* ==================================================================
    8) Persistir tablas como .sas7bdat
    ================================================================== */
    %let _dir_rc = %sysfunc(dcreate(., &_tables_path.));

    libname _outlib "&_tables_path.";

    /* Materialidad global (siempre) */
    data _outlib.&_tbl_prefix._mtd;
        set work._seg_mtd_global;
    run;

    /* KS results (si hay segmentos) */
    %if &_seg_has_segm. = 1 and %sysfunc(exist(work._seg_ks_results))
    %then %do;
        data _outlib.&_tbl_prefix._ks;
            set work._seg_ks_results;
        run;
    %end;

    /* Migracion (si existe) */
    %if &_seg_has_segm. = 1 and %sysfunc(exist(work._seg_mig_tipos))
    %then %do;
        data _outlib.&_tbl_prefix._mig;
            set work._seg_mig_tipos;
        run;
    %end;

    libname _outlib clear;

    %put NOTE: [segmentacion_run] Tablas persistidas en &_tables_path.;

    /* ==================================================================
    9) Cleanup work
    ================================================================== */
    proc datasets library=work nolist nowarn;
        delete _seg_data _seg_mtd_global _seg_mtd_segm _seg_mtd_resumen
            _seg_ks_results _seg_ks_resumen
            _seg_kw_means _seg_kw_test
            _seg_mig_resumen _seg_mig_cruce _seg_mig_tipos;
    quit;

    %put NOTE:======================================================;
    %put NOTE: [segmentacion_run] FIN - &_file_prefix. (mode=&seg_mode.);
    %put NOTE:======================================================;

%mend segmentacion_run;
