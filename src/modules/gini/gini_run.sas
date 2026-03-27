/* =========================================================================
gini_run.sas - Macro publica del modulo Gini (Metodo 4.3)

Alcance:
- Gini del modelo (general y mensual)
- Gini de variables (general y mensual)
- Reporte HTML + Excel + JPEG
- Persistencia de tablas .sas7bdat

Implementacion CAS-first:
- scope_input=1 como flujo principal
- tabla unificada con columna Split derivada en modulo
- compatibilidad transitoria con train_table + oot_table legacy
- sorting solo al final para salidas/reportes
========================================================================= */
%include "&fw_root./src/modules/gini/gini_contract.sas";
%include "&fw_root./src/modules/gini/impl/gini_common.sas";
%include "&fw_root./src/modules/gini/impl/gini_model_compute.sas";
%include "&fw_root./src/modules/gini/impl/gini_variable_compute.sas";
%include "&fw_root./src/modules/gini/impl/gini_plot.sas";
%include "&fw_root./src/modules/gini/impl/gini_report.sas";

%macro _gini_prepare_input_scope(input_caslib=, input_table=, byvar=, target=,
    def_cld=, train_min_mes=, train_max_mes=, oot_min_mes=, oot_max_mes=,
    out_table=_gini_input, split_var=Split);

    proc cas;
        session conn;
        table.dropTable / caslib="casuser" name="&out_table." quiet=true;
        table.dropTable / caslib="casuser" name="_gini_input_stage" quiet=true;
    quit;

    proc fedsql sessref=conn;
        create table casuser.&out_table. {options replace=true} as
        select 'TRAIN' as &split_var. ,
            a.*
        from &input_caslib..&input_table. a
        where a.&byvar. >= &train_min_mes.
          and a.&byvar. <= &train_max_mes.
          and a.&byvar. <= &def_cld.
          and a.&target. is not null;
    quit;

    proc fedsql sessref=conn;
        create table casuser._gini_input_stage {options replace=true} as
        select 'OOT' as &split_var. ,
            a.*
        from &input_caslib..&input_table. a
        where a.&byvar. >= &oot_min_mes.
          and a.&byvar. <= &oot_max_mes.
          and a.&byvar. <= &def_cld.
          and a.&target. is not null;
    quit;

    proc cas;
        session conn;
        table.append /
            source={caslib="casuser", name="_gini_input_stage"},
            target={caslib="casuser", name="&out_table."};
        table.dropTable / caslib="casuser" name="_gini_input_stage" quiet=true;
    quit;

%mend _gini_prepare_input_scope;

%macro _gini_prepare_input_legacy(input_caslib=, train_table=, oot_table=,
    byvar=, target=, def_cld=, out_table=_gini_input, split_var=Split);

    proc cas;
        session conn;
        table.dropTable / caslib="casuser" name="&out_table." quiet=true;
        table.dropTable / caslib="casuser" name="_gini_input_stage" quiet=true;
    quit;

    proc fedsql sessref=conn;
        create table casuser.&out_table. {options replace=true} as
        select 'TRAIN' as &split_var. ,
            a.*
        from &input_caslib..&train_table. a
        where a.&byvar. <= &def_cld.
          and a.&target. is not null;
    quit;

    proc fedsql sessref=conn;
        create table casuser._gini_input_stage {options replace=true} as
        select 'OOT' as &split_var. ,
            a.*
        from &input_caslib..&oot_table. a
        where a.&byvar. <= &def_cld.
          and a.&target. is not null;
    quit;

    proc cas;
        session conn;
        table.append /
            source={caslib="casuser", name="_gini_input_stage"},
            target={caslib="casuser", name="&out_table."};
        table.dropTable / caslib="casuser" name="_gini_input_stage" quiet=true;
    quit;

%mend _gini_prepare_input_legacy;

%macro gini_run(input_caslib=PROC, input_table=_scope_input, train_table=,
    oot_table=, output_caslib=OUT, troncal_id=, scope=, run_id=);

    %global _gini_rc;
    %let _gini_rc=0;

    %local _gini_vars_num _gini_target _gini_score _gini_pd _gini_xb
        _gini_byvar _gini_def_cld _gini_is_custom _scope_abbr _report_path
        _images_path _tables_path _file_prefix _tbl_prefix _seg_num _dir_rc
        _gini_model_low _gini_model_high _gini_var_low _gini_var_high
        _gini_model_type _gini_vars_train _gini_vars_oot _gini_vars_shared
        _gini_has_model_type_col _gini_train_min _gini_train_max _gini_oot_min
        _gini_oot_max _gini_has_input_table _gini_has_train_table
        _gini_has_oot_table _gini_use_legacy;

    %put NOTE:======================================================;
    %put NOTE: [gini_run] INICIO;
    %put NOTE: troncal=&troncal_id. scope=&scope.;
    %put NOTE: input=&input_caslib..&input_table.;
    %put NOTE: legacy train=&input_caslib..&train_table.
        oot=&input_caslib..&oot_table.;
    %put NOTE: mode=&gini_mode. score_source=&gini_score_source.
        with_missing=&gini_with_missing.;
    %put NOTE:======================================================;

    %let _gini_vars_num=;
    %let _gini_target=;
    %let _gini_score=;
    %let _gini_pd=;
    %let _gini_xb=;
    %let _gini_byvar=;
    %let _gini_def_cld=;
    %let _gini_train_min=;
    %let _gini_train_max=;
    %let _gini_oot_min=;
    %let _gini_oot_max=;
    %let _gini_is_custom=0;
    %let _gini_model_type=BHV;
    %let _gini_vars_train=;
    %let _gini_vars_oot=;
    %let _gini_vars_shared=;

    proc sql noprint;
        select strip(target) into :_gini_target trimmed
            from casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(pd) into :_gini_pd trimmed
            from casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(xb) into :_gini_xb trimmed
            from casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(byvar) into :_gini_byvar trimmed
            from casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(put(def_cld, best.)) into :_gini_def_cld trimmed
            from casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(put(train_min_mes, best.)) into :_gini_train_min trimmed
            from casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(put(train_max_mes, best.)) into :_gini_train_max trimmed
            from casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(put(oot_min_mes, best.)) into :_gini_oot_min trimmed
            from casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(put(oot_max_mes, best.)) into :_gini_oot_max trimmed
            from casuser.cfg_troncales where troncal_id=&troncal_id.;
    quit;

    %let _gini_has_model_type_col=0;
    proc sql noprint;
        select count(*) into :_gini_has_model_type_col trimmed
        from dictionary.columns
        where upcase(libname)='CASUSER' and upcase(memname)='CFG_TRONCALES' and
            upcase(name)='MODEL_TYPE';
    quit;

    %if &_gini_has_model_type_col. > 0 %then %do;
        proc sql noprint;
            select strip(model_type) into :_gini_model_type trimmed
                from casuser.cfg_troncales where troncal_id=&troncal_id.;
        quit;
    %end;
    %else %do;
        %put WARNING: [gini_run] cfg_troncales no tiene columna model_type.
            Se usa fallback BHV.;
    %end;

    %if %length(%superq(_gini_model_type))=0 %then %do;
        %let _gini_model_type=BHV;
        %put WARNING: [gini_run] model_type vacio para troncal &troncal_id..
            Se usa fallback BHV.;
    %end;

    %if %upcase(&gini_mode.)=CUSTOM %then %do;
        %let _gini_is_custom=1;
        %if %length(%superq(gini_custom_vars_num)) > 0 %then
            %let _gini_vars_num=&gini_custom_vars_num.;
        %if %length(%superq(gini_custom_target)) > 0 %then
            %let _gini_target=&gini_custom_target.;
        %if %length(%superq(gini_custom_def_cld)) > 0 %then
            %let _gini_def_cld=&gini_custom_def_cld.;
    %end;

    %if %upcase(&gini_score_source.)=CUSTOM and
        %length(%superq(gini_custom_score_var)) > 0 %then %do;
        %let _gini_is_custom=1;
    %end;

    %if %length(%superq(_gini_vars_num))=0 %then %do;
        %if %substr(&scope., 1, 3)=seg %then %do;
            %let _seg_num=%sysfunc(inputn(%substr(&scope., 4), best.));
            proc sql noprint;
                select strip(num_list) into :_gini_vars_num trimmed
                    from casuser.cfg_segmentos where troncal_id=&troncal_id.
                    and seg_id=&_seg_num.;
            quit;
        %end;

        %if %length(%superq(_gini_vars_num))=0 %then %do;
            proc sql noprint;
                select strip(num_unv) into :_gini_vars_num trimmed
                    from casuser.cfg_troncales where troncal_id=&troncal_id.;
            quit;
        %end;
    %end;

    %if %upcase(&gini_score_source.)=PD %then %let _gini_score=&_gini_pd.;
    %else %if %upcase(&gini_score_source.)=XB %then %let _gini_score=&_gini_xb.;
    %else %if %upcase(&gini_score_source.)=CUSTOM %then %do;
        %if %length(%superq(gini_custom_score_var)) > 0 %then
            %let _gini_score=&gini_custom_score_var.;
        %else %do;
            %put WARNING: [gini_run] gini_score_source=CUSTOM pero sin
                gini_custom_score_var. Fallback a AUTO.;
        %end;
    %end;

    %if %length(%superq(_gini_score))=0 %then %do;
        %if %length(%superq(_gini_pd)) > 0 %then %let _gini_score=&_gini_pd.;
        %else %let _gini_score=&_gini_xb.;
    %end;

    %if %upcase(&_gini_model_type.)=BHV %then %do;
        %let _gini_model_low=0.50;
        %let _gini_model_high=0.60;
    %end;
    %else %do;
        %let _gini_model_low=0.40;
        %let _gini_model_high=0.50;
    %end;

    %if %length(%superq(gini_threshold_model_low)) > 0 %then
        %let _gini_model_low=&gini_threshold_model_low.;
    %if %length(%superq(gini_threshold_model_high)) > 0 %then
        %let _gini_model_high=&gini_threshold_model_high.;

    %if %length(%superq(gini_threshold_var_low)) > 0 %then
        %let _gini_var_low=&gini_threshold_var_low.;
    %else %let _gini_var_low=0.05;

    %if %length(%superq(gini_threshold_var_high)) > 0 %then
        %let _gini_var_high=&gini_threshold_var_high.;
    %else %let _gini_var_high=0.15;

    %if %length(%superq(_gini_target))=0 or %length(%superq(_gini_byvar))=0 or
        %length(%superq(_gini_def_cld))=0 %then %do;
        %put ERROR: [gini_run] Config incompleta en cfg_troncales para troncal
            &troncal_id. (target/byvar/def_cld).;
        %let _gini_rc=1;
        %return;
    %end;

    %if %length(%superq(_gini_train_min))=0 or
        %length(%superq(_gini_train_max))=0 or
        %length(%superq(_gini_oot_min))=0 or
        %length(%superq(_gini_oot_max))=0 %then %do;
        %put ERROR: [gini_run] Ventanas TRAIN/OOT incompletas en cfg_troncales
            para troncal &troncal_id..;
        %let _gini_rc=1;
        %return;
    %end;

    %put NOTE: [gini_run] target=&_gini_target. score=&_gini_score.
        byvar=&_gini_byvar. def_cld=&_gini_def_cld.;
    %put NOTE: [gini_run] ventanas TRAIN=&_gini_train_min.-&_gini_train_max.
        OOT=&_gini_oot_min.-&_gini_oot_max..;
    %put NOTE: [gini_run] model_type=&_gini_model_type.;
    %put NOTE: [gini_run] vars_num=&_gini_vars_num.;
    %put NOTE: [gini_run] threshold_model=&_gini_model_low./&_gini_model_high.;
    %put NOTE: [gini_run] threshold_var=&_gini_var_low./&_gini_var_high.;

    %if %substr(&scope., 1, 3)=seg %then %let _scope_abbr=&scope.;
    %else %let _scope_abbr=base;

    %if &_gini_is_custom.=1 %then %do;
        %let _report_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _images_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _tables_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _file_prefix=custom_gini_troncal_&troncal_id._&_scope_abbr.;
        %let _tbl_prefix=cx_gini_t&troncal_id._&_scope_abbr.;
        %put NOTE: [gini_run] Output -> experiments/ (exploratorio);
    %end;
    %else %do;
        %let _report_path=&fw_root./outputs/runs/&run_id./reports/METOD4.3;
        %let _images_path=&fw_root./outputs/runs/&run_id./images/METOD4.3;
        %let _tables_path=&fw_root./outputs/runs/&run_id./tables/METOD4.3;
        %let _file_prefix=gini_troncal_&troncal_id._&_scope_abbr.;
        %let _tbl_prefix=gini_t&troncal_id._&_scope_abbr.;
        %let _dir_rc=%sysfunc(dcreate(METOD4.3,
            &fw_root./outputs/runs/&run_id./reports));
        %let _dir_rc=%sysfunc(dcreate(METOD4.3,
            &fw_root./outputs/runs/&run_id./images));
        %let _dir_rc=%sysfunc(dcreate(METOD4.3,
            &fw_root./outputs/runs/&run_id./tables));
        %put NOTE: [gini_run] Output -> reports/images/tables METOD4.3.;
    %end;

    %let _gini_has_input_table=0;
    %let _gini_has_train_table=0;
    %let _gini_has_oot_table=0;
    %let _gini_use_legacy=0;

    %if %length(%superq(input_table)) > 0 %then %do;
        proc sql noprint;
            select count(*) into :_gini_has_input_table trimmed
            from dictionary.tables
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&input_table.");
        quit;
    %end;

    %if %length(%superq(train_table)) > 0 %then %do;
        proc sql noprint;
            select count(*) into :_gini_has_train_table trimmed
            from dictionary.tables
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&train_table.");
        quit;
    %end;

    %if %length(%superq(oot_table)) > 0 %then %do;
        proc sql noprint;
            select count(*) into :_gini_has_oot_table trimmed
            from dictionary.tables
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&oot_table.");
        quit;
    %end;

    %if &_gini_has_input_table. > 0 %then %do;
        %_gini_prepare_input_scope(input_caslib=&input_caslib.,
            input_table=&input_table., byvar=&_gini_byvar.,
            target=&_gini_target., def_cld=&_gini_def_cld.,
            train_min_mes=&_gini_train_min., train_max_mes=&_gini_train_max.,
            oot_min_mes=&_gini_oot_min., oot_max_mes=&_gini_oot_max.,
            out_table=_gini_input, split_var=Split);
    %end;
    %else %if &_gini_has_train_table. > 0 and &_gini_has_oot_table. > 0 %then
        %do;
        %let _gini_use_legacy=1;
        %put WARNING: [gini_run] input_table no disponible. Se usa ruta legacy
            train/oot para compatibilidad transitoria.;

        %_gini_prepare_input_legacy(input_caslib=&input_caslib.,
            train_table=&train_table., oot_table=&oot_table.,
            byvar=&_gini_byvar., target=&_gini_target.,
            def_cld=&_gini_def_cld., out_table=_gini_input,
            split_var=Split);
    %end;
    %else %do;
        %put ERROR: [gini_run] No se encontro input valido. Esperado
            &input_caslib..&input_table. o train/oot legacy.;
        %let _gini_rc=1;
        %return;
    %end;

    %gini_contract(input_caslib=casuser, input_table=_gini_input,
        target=&_gini_target., score=&_gini_score., byvar=&_gini_byvar.,
        def_cld=&_gini_def_cld., split_var=Split);

    %if &_gini_rc. ne 0 %then %do;
        %put ERROR: [gini_run] Contract fallido - modulo abortado.;
        %return;
    %end;

    %_gini_partition_vars(data=casuser._gini_input, vars_num=&_gini_vars_num.,
        out_train=_gini_vars_train, out_oot=_gini_vars_oot,
        out_shared=_gini_vars_shared);

    %_gini_model_general(data=casuser._gini_input, split_var=Split,
        target=&_gini_target., score=&_gini_score.,
        with_missing=&gini_with_missing., model_low=&_gini_model_low.,
        model_high=&_gini_model_high., out=casuser._gini_model_general);

    %_gini_model_monthly(data=casuser._gini_input, split_var=Split,
        target=&_gini_target., score=&_gini_score., byvar=&_gini_byvar.,
        with_missing=&gini_with_missing., model_low=&_gini_model_low.,
        model_high=&_gini_model_high., trend_delta=&gini_trend_delta.,
        out=casuser._gini_model_monthly);

    %_gini_variables_general(data=casuser._gini_input, split_var=Split,
        target=&_gini_target., vars_num_train=&_gini_vars_train.,
        vars_num_oot=&_gini_vars_oot., with_missing=&gini_with_missing.,
        min_n_valid=&gini_min_n_valid., var_low=&_gini_var_low.,
        var_high=&_gini_var_high., out=casuser._gini_vars_general);

    %_gini_variables_compare(data=casuser._gini_vars_general,
        delta_warn=&gini_delta_warn., out=casuser._gini_vars_compare);

    %_gini_variables_monthly(data=casuser._gini_input, split_var=Split,
        target=&_gini_target., vars_num_train=&_gini_vars_train.,
        vars_num_oot=&_gini_vars_oot., byvar=&_gini_byvar.,
        with_missing=&gini_with_missing., min_n_valid=&gini_min_n_valid.,
        var_low=&_gini_var_low., var_high=&_gini_var_high.,
        out=casuser._gini_vars_detail);

    %_gini_variables_summary(data=casuser._gini_vars_detail,
        var_low=&_gini_var_low., var_high=&_gini_var_high.,
        trend_delta=&gini_trend_delta., out=casuser._gini_vars_summary);

    %_gini_sort_cas(table_name=_gini_model_monthly,
        orderby=%str({"Periodo", "Split"}));
    %_gini_sort_cas(table_name=_gini_vars_general,
        orderby=%str({"Variable", "Split"}));
    %_gini_sort_cas(table_name=_gini_vars_compare,
        orderby=%str({"Variable"}));
    %_gini_sort_cas(table_name=_gini_vars_summary,
        orderby=%str({"Variable", "First_Period", "Split"}));
    %_gini_sort_cas(table_name=_gini_vars_detail,
        orderby=%str({"Variable", "Periodo", "Split"}));

    libname _giniout "&_tables_path.";

    data _giniout.&_tbl_prefix._mdlg;
        set casuser._gini_model_general;
    run;

    data _giniout.&_tbl_prefix._mdlm;
        set casuser._gini_model_monthly;
    run;

    data _giniout.&_tbl_prefix._varg;
        set casuser._gini_vars_general;
    run;

    data _giniout.&_tbl_prefix._vcmp;
        set casuser._gini_vars_compare;
    run;

    data _giniout.&_tbl_prefix._vsum;
        set casuser._gini_vars_summary;
    run;

    data _giniout.&_tbl_prefix._vdet;
        set casuser._gini_vars_detail;
    run;

    %_gini_report(report_path=&_report_path., images_path=&_images_path.,
        file_prefix=&_file_prefix., byvar=&_gini_byvar.,
        model_low=&_gini_model_low., model_high=&_gini_model_high.,
        var_low=&_gini_var_low., var_high=&_gini_var_high.,
        delta_warn=&gini_delta_warn., top_n=&gini_plot_top_n.,
        table_lib=_giniout, table_prefix=&_tbl_prefix.);

    libname _giniout clear;

    proc datasets library=casuser nolist nowarn;
        delete _gini_:;
    quit;

    %put NOTE:======================================================;
    %put NOTE: [gini_run] FIN - &_file_prefix. (mode=&gini_mode.).;
    %put NOTE:======================================================;

%mend gini_run;
