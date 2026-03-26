/* =========================================================================
segmentacion_run.sas - Macro publica del modulo Segmentacion (Metodo 3)

API publica compatible:
%segmentacion_run(
    input_caslib  = PROC,
    input_table   = _scope_input,
    output_caslib = OUT,
    troncal_id    = <id>,
    split         = <compatibilidad, ignorado>,
    scope         = base | segNNN,
    run_id        = <run_id>
)

Flujo interno:
1) Resolver variables y ventanas desde cfg_troncales
2) Determinar has_segm / has_id
3) Ejecutar contract sobre el input unificado
4) Construir una sola tabla canonica en CAS con periodos derivados
5) Calcular resultados consolidados (CAS-first)
6) Generar un unico reporte HTML + Excel y un unico grafico JPEG
7) Persistir tablas clave como .sas7bdat
8) Cleanup en work y casuser

NOTA: Segmentacion usa target -> fecha de corte es def_cld.
El parametro split= se conserva solo por compatibilidad y no afecta la
logica del modulo.
========================================================================= */
%include "&fw_root./src/modules/segmentacion/segmentacion_contract.sas";
%include "&fw_root./src/modules/segmentacion/impl/segmentacion_compute.sas";
%include "&fw_root./src/modules/segmentacion/impl/segmentacion_report.sas";

%macro segmentacion_run(input_caslib=PROC, input_table=_scope_input,
    output_caslib=OUT, troncal_id=, split=, scope=, run_id=);

    %global _seg_rc;
    %let _seg_rc = 0;

    %local _seg_target _seg_segvar _seg_byvar _seg_idvar _seg_def_cld
        _seg_train_min _seg_train_max _seg_oot_min _seg_oot_max
        _seg_train_first _seg_oot_last _seg_has_segm _seg_has_id
        _report_path _images_path _tables_path _file_prefix _tbl_prefix
        _scope_abbr _seg_is_custom _seg_has_col _dir_rc _select_cols;

    %put NOTE:======================================================;
    %put NOTE: [segmentacion_run] INICIO;
    %put NOTE: troncal=&troncal_id. scope=&scope.;
    %put NOTE: input=&input_caslib..&input_table.;
    %put NOTE:======================================================;

    %if %length(%superq(split)) > 0 %then
        %put NOTE: [segmentacion_run] split=&split. se ignora por compatibilidad; el modulo opera sobre el input consolidado.;

    %let _seg_target = ;
    %let _seg_segvar = ;
    %let _seg_byvar = ;
    %let _seg_idvar = ;
    %let _seg_def_cld = ;
    %let _seg_train_min = ;
    %let _seg_train_max = ;
    %let _seg_oot_min = ;
    %let _seg_oot_max = ;
    %let _seg_train_first = ;
    %let _seg_oot_last = ;
    %let _seg_is_custom = 0;
    %let _seg_has_segm = 0;
    %let _seg_has_id = 0;

    proc sql noprint;
        select strip(target),
               strip(byvar),
               def_cld,
               strip(var_seg),
               strip(id_var_id),
               train_min_mes,
               train_max_mes,
               oot_min_mes,
               oot_max_mes
          into :_seg_target trimmed,
               :_seg_byvar trimmed,
               :_seg_def_cld trimmed,
               :_seg_segvar trimmed,
               :_seg_idvar trimmed,
               :_seg_train_min trimmed,
               :_seg_train_max trimmed,
               :_seg_oot_min trimmed,
               :_seg_oot_max trimmed
        from casuser.cfg_troncales
        where troncal_id = &troncal_id.;
    quit;

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

    %if %length(%superq(_seg_target)) = 0 or %length(%superq(_seg_byvar)) = 0 or
        %length(%superq(_seg_def_cld)) = 0 or
        %length(%superq(_seg_train_min)) = 0 or %length(%superq(_seg_train_max)) = 0 or
        %length(%superq(_seg_oot_min)) = 0 or %length(%superq(_seg_oot_max)) = 0 %then %do;
        %put ERROR: [segmentacion_run] No se pudo resolver la configuracion minima de la troncal &troncal_id..;
        %let _seg_rc = 1;
        %return;
    %end;

    %let _seg_train_first = &_seg_train_min.;
    %let _seg_oot_last = %sysfunc(min(&_seg_oot_max., &_seg_def_cld.));

    %put NOTE: [segmentacion_run] Variables resueltas:;
    %put NOTE: [segmentacion_run] target=&_seg_target. segvar=&_seg_segvar.;
    %put NOTE: [segmentacion_run] byvar=&_seg_byvar. idvar=&_seg_idvar.;
    %put NOTE: [segmentacion_run] TRAIN=&_seg_train_min.-&_seg_train_max.;
    %put NOTE: [segmentacion_run] OOT=&_seg_oot_min.-&_seg_oot_max. def_cld=&_seg_def_cld.;

    %if %substr(&scope., 1, 3) = seg %then %let _scope_abbr = &scope.;
    %else %let _scope_abbr = base;

    %if &_seg_is_custom. = 1 %then %do;
        %let _report_path = &fw_root./outputs/runs/&run_id./experiments;
        %let _images_path = &fw_root./outputs/runs/&run_id./experiments;
        %let _tables_path = &fw_root./outputs/runs/&run_id./experiments;
        %let _file_prefix = custom_seg_troncal_&troncal_id._&_scope_abbr.;
        %let _tbl_prefix  = cx_seg_t&troncal_id._&_scope_abbr.;
        %put NOTE: [segmentacion_run] Output -> experiments/ (exploratorio);
    %end;
    %else %do;
        %let _report_path = &fw_root./outputs/runs/&run_id./reports/METOD3;
        %let _images_path = &fw_root./outputs/runs/&run_id./images/METOD3;
        %let _tables_path = &fw_root./outputs/runs/&run_id./tables/METOD3;
        %let _file_prefix = seg_troncal_&troncal_id._&_scope_abbr.;
        %let _tbl_prefix  = seg_t&troncal_id._&_scope_abbr.;
        %put NOTE: [segmentacion_run] Output -> reports/METOD3/;
    %end;

    %segmentacion_contract(input_caslib=&input_caslib.,
        input_table=&input_table., target=&_seg_target.,
        byvar=&_seg_byvar., segvar=&_seg_segvar., idvar=&_seg_idvar.,
        def_cld=&_seg_def_cld., train_min_mes=&_seg_train_min.,
        train_max_mes=&_seg_train_max., oot_min_mes=&_seg_oot_min.,
        oot_max_mes=&_seg_oot_max.);

    %if &_seg_rc. ne 0 %then %do;
        %put ERROR: [segmentacion_run] Contract fallido - modulo abortado.;
        %return;
    %end;

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

    %put NOTE: [segmentacion_run] has_segm=&_seg_has_segm. has_id=&_seg_has_id.;

    %let _select_cols = a.&_seg_target. as &_seg_target.,
        a.&_seg_byvar. as &_seg_byvar.;
    %if &_seg_has_segm. = 1 %then
        %let _select_cols = &_select_cols., a.&_seg_segvar. as &_seg_segvar.;
    %if &_seg_has_id. = 1 %then
        %let _select_cols = &_select_cols., a.&_seg_idvar. as &_seg_idvar.;

    proc cas;
        session conn;
        table.dropTable / caslib="casuser" name="_seg_input" quiet=true;
    quit;

    proc fedsql sessref=conn;
        create table casuser._seg_input {options replace=true} as
        select case
                   when a.&_seg_byvar. >= &_seg_train_min.
                    and a.&_seg_byvar. <= &_seg_train_max.
                   then 'TRAIN'
                   else 'OOT'
               end as _seg_period,
               &_select_cols.
        from &input_caslib..&input_table. a
        where a.&_seg_byvar. <= &_seg_def_cld.
          and (
                (a.&_seg_byvar. >= &_seg_train_min. and a.&_seg_byvar. <= &_seg_train_max.)
                or
                (a.&_seg_byvar. >= &_seg_oot_min. and a.&_seg_byvar. <= &_seg_oot_max.)
              );
    quit;

    %_seg_compute(data=casuser._seg_input, target=&_seg_target.,
        segvar=&_seg_segvar., byvar=&_seg_byvar., idvar=&_seg_idvar.,
        min_obs=&seg_min_obs., min_target=&seg_min_target.,
        train_first_mes=&_seg_train_first., oot_last_mes=&_seg_oot_last.,
        has_segm=&_seg_has_segm., has_id=&_seg_has_id.,
        period_var=_seg_period);

    %if &_seg_is_custom. = 0 %then %do;
        %let _dir_rc = %sysfunc(dcreate(METOD3, &fw_root./outputs/runs/&run_id./reports));
        %let _dir_rc = %sysfunc(dcreate(METOD3, &fw_root./outputs/runs/&run_id./images));
        %let _dir_rc = %sysfunc(dcreate(METOD3, &fw_root./outputs/runs/&run_id./tables));
    %end;

    %_seg_report(report_path=&_report_path., images_path=&_images_path.,
        file_prefix=&_file_prefix., data=casuser._seg_input,
        target=&_seg_target., byvar=&_seg_byvar., segvar=&_seg_segvar.,
        has_segm=&_seg_has_segm., has_id=&_seg_has_id.,
        oot_min_mes=&_seg_oot_min., plot_sep=&seg_plot_sep.);

    libname _outlib "&_tables_path.";

    data _outlib.&_tbl_prefix._mtd;
        set casuser._seg_mtd_global;
    run;

    %if &_seg_has_segm. = 1 %then %do;
        data _outlib.&_tbl_prefix._mtds;
            set casuser._seg_mtd_segm;
        run;

        data _outlib.&_tbl_prefix._mtr;
            set casuser._seg_mtd_resumen;
        run;
    %end;

    %if &_seg_has_segm. = 1 %then %do;
        data _outlib.&_tbl_prefix._ks;
            set casuser._seg_ks_results;
        run;
    %end;

    %if &_seg_has_segm. = 1 and &_seg_has_id. = 1 %then %do;
        data _outlib.&_tbl_prefix._mig;
            set casuser._seg_mig_tipos;
        run;
    %end;

    libname _outlib clear;

    %put NOTE: [segmentacion_run] Tablas persistidas en &_tables_path.;

    proc datasets library=work nolist nowarn;
        delete _seg_ks_results _seg_ks_temp _seg_ks_row _seg_kw_test;
    quit;

    proc datasets library=casuser nolist nowarn;
        delete _seg_:;
    quit;

    %put NOTE:======================================================;
    %put NOTE: [segmentacion_run] FIN - &_file_prefix. (mode=&seg_mode.);
    %put NOTE:======================================================;

%mend segmentacion_run;
