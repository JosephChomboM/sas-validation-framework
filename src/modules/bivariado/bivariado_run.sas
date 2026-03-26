/* =========================================================================
bivariado_run.sas - Macro publica del modulo Bivariado (Metodo 4.3)

API publica compatible:
%bivariado_run(
    input_caslib  = PROC,
    input_table   = _scope_input,
    train_table   = _train_input,
    oot_table     = _oot_input,
    output_caslib = OUT,
    troncal_id    = <id>,
    scope         = base | segNNN,
    run_id        = <run_id>
)

Compatibilidad:
- Si input_table existe, usa flujo unificado scope-input.
- Si input_table no existe pero train_table/oot_table si existen,
  arma una tabla unificada legacy y continua.
========================================================================= */
%include "&fw_root./src/modules/bivariado/bivariado_contract.sas";
%include "&fw_root./src/modules/bivariado/impl/bivariado_compute.sas";
%include "&fw_root./src/modules/bivariado/impl/bivariado_report.sas";

%macro _biv_append_unique(word=, listvar=);

    %local _current_list _found;

    %if %length(%superq(word)) = 0 %then %return;
    %if %length(%superq(listvar)) = 0 %then %return;

    %let _current_list=&&&listvar.;
    %let _found=0;

    %if %length(%superq(_current_list)) = 0 %then %let &listvar.=%superq(word);
    %else %do;
        %let _found=%sysfunc(findw(%superq(_current_list), %superq(word), %str( )));
        %if %sysevalf(%superq(_found)=, boolean) %then %let _found=0;
        %if &_found.=0 %then %let &listvar.=&_current_list. %superq(word);
    %end;

%mend _biv_append_unique;

%macro _biv_filter_existing(input_caslib=, input_table=, raw_list=, outvar=,
    label=);

    %local _idx _var _exists;
    %let &outvar.=;
    %let _idx=1;
    %let _var=%scan(%superq(raw_list), &_idx., %str( ));

    %do %while(%length(%superq(_var)) > 0);
        %let _exists=0;
        proc sql noprint;
            select count(*) into :_exists trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&input_table.")
              and upcase(name)=upcase("&_var.");
        quit;

        %if &_exists. > 0 %then %_biv_append_unique(word=&_var., listvar=&outvar.);
        %else %put WARNING: [bivariado_run] &label.=&_var. no existe en &input_caslib..&input_table. y se omitira.;

        %let _idx=%eval(&_idx. + 1);
        %let _var=%scan(%superq(raw_list), &_idx., %str( ));
    %end;

%mend _biv_filter_existing;

%macro _biv_build_select_sql(var_list=, outvar=);

    %local _idx _var _current_select;
    %let &outvar.=;
    %let _idx=1;
    %let _var=%scan(%superq(var_list), &_idx., %str( ));

    %do %while(%length(%superq(_var)) > 0);
        %let _current_select=&&&outvar.;
        %if %length(%superq(_current_select)) = 0 %then
            %let &outvar.=a.&_var. as &_var.;
        %else %let &outvar.=&_current_select., a.&_var. as &_var.;
        %let _idx=%eval(&_idx. + 1);
        %let _var=%scan(%superq(var_list), &_idx., %str( ));
    %end;

%mend _biv_build_select_sql;

%macro bivariado_run(input_caslib=PROC, input_table=_scope_input,
    train_table=_train_input, oot_table=_oot_input, output_caslib=OUT,
    troncal_id=, scope=, run_id=);

    %global _biv_rc;
    %let _biv_rc=0;

    %local _biv_vars_num _biv_vars_cat _biv_target _biv_dri_num _biv_dri_cat
        _biv_byvar _biv_def_cld _biv_train_min _biv_train_max _biv_oot_min
        _biv_oot_max _report_path _images_path _file_prefix _scope_abbr
        _biv_is_custom _seg_num _input_exists _legacy_train_exists
        _legacy_oot_exists _use_legacy _source_table _source_caslib
        _filter_table _all_vars _select_sql _idx _var;

    %put NOTE:======================================================;
    %put NOTE: [bivariado_run] INICIO;
    %put NOTE: troncal=&troncal_id. scope=&scope.;
    %put NOTE: input=&input_caslib..&input_table. train=&train_table. oot=&oot_table.;
    %put NOTE:======================================================;

    %let _biv_vars_num=;
    %let _biv_vars_cat=;
    %let _biv_target=;
    %let _biv_dri_num=;
    %let _biv_dri_cat=;
    %let _biv_byvar=;
    %let _biv_def_cld=;
    %let _biv_train_min=;
    %let _biv_train_max=;
    %let _biv_oot_min=;
    %let _biv_oot_max=;
    %let _biv_is_custom=0;
    %let _use_legacy=0;

    proc sql noprint;
        select strip(target),
               strip(byvar),
               strip(put(def_cld, best.)),
               strip(put(train_min_mes, best.)),
               strip(put(train_max_mes, best.)),
               strip(put(oot_min_mes, best.)),
               strip(put(oot_max_mes, best.))
          into :_biv_target trimmed,
               :_biv_byvar trimmed,
               :_biv_def_cld trimmed,
               :_biv_train_min trimmed,
               :_biv_train_max trimmed,
               :_biv_oot_min trimmed,
               :_biv_oot_max trimmed
        from casuser.cfg_troncales
        where troncal_id=&troncal_id.;
    quit;

    %if %upcase(&biv_mode.)=CUSTOM %then %do;
        %if %length(%superq(biv_custom_vars_num)) > 0 or
            %length(%superq(biv_custom_vars_cat)) > 0 %then %do;
            %let _biv_vars_num=&biv_custom_vars_num.;
            %let _biv_vars_cat=&biv_custom_vars_cat.;
            %let _biv_is_custom=1;
            %put NOTE: [bivariado_run] Modo CUSTOM activado.;
        %end;
        %else %put WARNING: [bivariado_run] biv_mode=CUSTOM sin variables custom. Fallback a AUTO.;
    %end;

    %if &_biv_is_custom.=0 %then %do;
        %put NOTE: [bivariado_run] Modo AUTO - resolviendo vars desde config.;

        %if %substr(&scope., 1, 3)=seg %then %do;
            %let _seg_num=%sysfunc(inputn(%substr(&scope., 4), best.));
            proc sql noprint;
                select strip(num_list), strip(cat_list), strip(dri_num_list),
                       strip(dri_cat_list)
                  into :_biv_vars_num trimmed,
                       :_biv_vars_cat trimmed,
                       :_biv_dri_num trimmed,
                       :_biv_dri_cat trimmed
                from casuser.cfg_segmentos
                where troncal_id=&troncal_id. and seg_id=&_seg_num.;
            quit;
        %end;

        %if %length(%superq(_biv_vars_num))=0 %then %do;
            proc sql noprint;
                select strip(num_unv) into :_biv_vars_num trimmed
                from casuser.cfg_troncales where troncal_id=&troncal_id.;
            quit;
        %end;

        %if %length(%superq(_biv_vars_cat))=0 %then %do;
            proc sql noprint;
                select strip(cat_unv) into :_biv_vars_cat trimmed
                from casuser.cfg_troncales where troncal_id=&troncal_id.;
            quit;
        %end;

        %if %length(%superq(_biv_dri_num))=0 %then %do;
            proc sql noprint;
                select strip(dri_num_unv) into :_biv_dri_num trimmed
                from casuser.cfg_troncales where troncal_id=&troncal_id.;
            quit;
        %end;

        %if %length(%superq(_biv_dri_cat))=0 %then %do;
            proc sql noprint;
                select strip(dri_cat_unv) into :_biv_dri_cat trimmed
                from casuser.cfg_troncales where troncal_id=&troncal_id.;
            quit;
        %end;
    %end;

    %let _input_exists=0;
    %let _legacy_train_exists=0;
    %let _legacy_oot_exists=0;

    proc sql noprint;
        select count(*) into :_input_exists trimmed
        from dictionary.tables
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&input_table.");

        select count(*) into :_legacy_train_exists trimmed
        from dictionary.tables
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&train_table.");

        select count(*) into :_legacy_oot_exists trimmed
        from dictionary.tables
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&oot_table.");
    quit;

    %if &_input_exists. > 0 %then %do;
        %let _source_caslib=&input_caslib.;
        %let _source_table=&input_table.;
        %let _filter_table=&input_table.;
        %put NOTE: [bivariado_run] Usando flujo unificado sobre &input_caslib..&input_table.;
    %end;
    %else %if &_legacy_train_exists. > 0 and &_legacy_oot_exists. > 0 %then %do;
        %let _use_legacy=1;
        %let _source_caslib=casuser;
        %let _source_table=_biv_scope_legacy;
        %let _filter_table=&train_table.;
        %put NOTE: [bivariado_run] input_table no disponible. Se activa compatibilidad legacy train/oot.;
    %end;
    %else %do;
        %put ERROR: [bivariado_run] No se encontro input_table=&input_table. ni par legacy train/oot valido.;
        %let _biv_rc=1;
        %return;
    %end;

    %_biv_filter_existing(input_caslib=&input_caslib., input_table=&_filter_table.,
        raw_list=&_biv_vars_num., outvar=_biv_vars_num, label=vars_num);
    %_biv_filter_existing(input_caslib=&input_caslib., input_table=&_filter_table.,
        raw_list=&_biv_vars_cat., outvar=_biv_vars_cat, label=vars_cat);
    %_biv_filter_existing(input_caslib=&input_caslib., input_table=&_filter_table.,
        raw_list=&_biv_dri_num., outvar=_biv_dri_num, label=dri_num);
    %_biv_filter_existing(input_caslib=&input_caslib., input_table=&_filter_table.,
        raw_list=&_biv_dri_cat., outvar=_biv_dri_cat, label=dri_cat);

    %put NOTE: [bivariado_run] target=&_biv_target. byvar=&_biv_byvar. def_cld=&_biv_def_cld.;
    %put NOTE: [bivariado_run] vars_num=&_biv_vars_num.;
    %put NOTE: [bivariado_run] vars_cat=&_biv_vars_cat.;
    %put NOTE: [bivariado_run] dri_num=&_biv_dri_num.;
    %put NOTE: [bivariado_run] dri_cat=&_biv_dri_cat.;

    %if %length(%superq(_biv_vars_num))=0 and %length(%superq(_biv_vars_cat))=0 %then %do;
        %put ERROR: [bivariado_run] No quedaron variables principales validas despues del filtrado contra el input.;
        %let _biv_rc=1;
        %return;
    %end;

    %if %substr(&scope., 1, 3)=seg %then %let _scope_abbr=&scope.;
    %else %let _scope_abbr=base;

    %if &_biv_is_custom.=1 %then %do;
        %let _report_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _images_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _file_prefix=custom_biv_troncal_&troncal_id._&_scope_abbr.;
        %put NOTE: [bivariado_run] Output -> experiments/ (exploratorio);
    %end;
    %else %do;
        %let _report_path=&fw_root./outputs/runs/&run_id./reports/METOD4.3;
        %let _images_path=&fw_root./outputs/runs/&run_id./images/METOD4.3;
        %let _file_prefix=biv_troncal_&troncal_id._&_scope_abbr.;
        %put NOTE: [bivariado_run] Output -> reports/METOD4.3/ + images/METOD4.3/;
    %end;

    %if &_use_legacy.=1 %then %do;
        %bivariado_contract(input_caslib=&input_caslib., train_table=&train_table.,
            oot_table=&oot_table., vars_num=&_biv_vars_num.,
            vars_cat=&_biv_vars_cat., target=&_biv_target.,
            byvar=&_biv_byvar., train_min_mes=&_biv_train_min.,
            train_max_mes=&_biv_train_max., oot_min_mes=&_biv_oot_min.,
            oot_max_mes=&_biv_oot_max., def_cld=&_biv_def_cld.);
    %end;
    %else %do;
        %bivariado_contract(input_caslib=&input_caslib., input_table=&input_table.,
            vars_num=&_biv_vars_num., vars_cat=&_biv_vars_cat.,
            target=&_biv_target., byvar=&_biv_byvar.,
            train_min_mes=&_biv_train_min., train_max_mes=&_biv_train_max.,
            oot_min_mes=&_biv_oot_min., oot_max_mes=&_biv_oot_max.,
            def_cld=&_biv_def_cld.);
    %end;

    %if &_biv_rc. ne 0 %then %do;
        %put ERROR: [bivariado_run] Contract fallido - modulo abortado.;
        %return;
    %end;

    %if &_use_legacy.=1 %then %do;
        proc cas;
            session conn;
            table.dropTable / caslib='casuser' name='_biv_scope_legacy' quiet=true;
        quit;

        proc fedsql sessref=conn;
            create table casuser._biv_scope_legacy {options replace=true} as
            select 'TRAIN' as _legacy_period, a.*
            from &input_caslib..&train_table. a
            union all
            select 'OOT' as _legacy_period, a.*
            from &input_caslib..&oot_table. a;
        quit;
    %end;

    %let _all_vars=;
    %_biv_append_unique(word=&_biv_target., listvar=_all_vars);
    %_biv_append_unique(word=&_biv_byvar., listvar=_all_vars);

    %let _idx=1;
    %let _var=%scan(&_biv_vars_num. &_biv_vars_cat. &_biv_dri_num. &_biv_dri_cat., &_idx., %str( ));
    %do %while(%length(%superq(_var)) > 0);
        %_biv_append_unique(word=&_var., listvar=_all_vars);
        %let _idx=%eval(&_idx. + 1);
        %let _var=%scan(&_biv_vars_num. &_biv_vars_cat. &_biv_dri_num. &_biv_dri_cat., &_idx., %str( ));
    %end;

    %_biv_build_select_sql(var_list=&_all_vars., outvar=_select_sql);

    proc cas;
        session conn;
        table.dropTable / caslib='casuser' name='_biv_input' quiet=true;
        table.dropTable / caslib='casuser' name='_biv_train' quiet=true;
    quit;

    proc fedsql sessref=conn;
        create table casuser._biv_input {options replace=true} as
        select case
                   when a.&_biv_byvar. >= &_biv_train_min.
                    and a.&_biv_byvar. <= &_biv_train_max.
                   then 'TRAIN'
                   else 'OOT'
               end as _biv_period,
               &_select_sql.
        from &_source_caslib..&_source_table. a
        where a.&_biv_byvar. <= &_biv_def_cld.
          and a.&_biv_target. is not null
          and (
                (a.&_biv_byvar. >= &_biv_train_min. and a.&_biv_byvar. <= &_biv_train_max.)
                or
                (a.&_biv_byvar. >= &_biv_oot_min. and a.&_biv_byvar. <= &_biv_oot_max.)
              );
    quit;

    proc fedsql sessref=conn;
        create table casuser._biv_train {options replace=true} as
        select *
        from casuser._biv_input
        where _biv_period = 'TRAIN';
    quit;

    %_bivariado_compute(source_data=casuser._biv_input,
        train_data=casuser._biv_train, target=&_biv_target.,
        byvar=&_biv_byvar., vars_num=&_biv_vars_num.,
        vars_cat=&_biv_vars_cat., dri_num=&_biv_dri_num.,
        dri_cat=&_biv_dri_cat., groups=&biv_n_groups.);

    %_bivariado_report(byvar=&_biv_byvar., oot_min_mes=&_biv_oot_min.,
        report_path=&_report_path., images_path=&_images_path.,
        file_prefix=&_file_prefix.);

    proc datasets library=casuser nolist nowarn;
        delete _biv_:;
    quit;

    proc datasets library=work nolist nowarn;
        delete _biv_:;
    quit;

    %put NOTE:======================================================;
    %put NOTE: [bivariado_run] FIN - &_file_prefix. (mode=&biv_mode.);
    %put NOTE:======================================================;

%mend bivariado_run;
