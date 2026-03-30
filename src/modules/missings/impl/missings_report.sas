/* =========================================================================
missings_report.sas - Reportes legacy sobre compute unificado CAS-first

Genera:
- <report_path>/<prefix>.html
- <report_path>/<prefix>.xlsx

El HTML consolidado presenta TRAIN y OOT en secciones separadas.
No persiste tablas en tables/.
========================================================================= */

%macro _miss_prepare_scope_data(input_caslib=, input_table=, split_mode=DERIVED,
    split_var=Split, byvar=, train_min_mes=, train_max_mes=, oot_min_mes=,
    oot_max_mes=, out_table=_miss_input);

    proc datasets library=casuser nolist nowarn;
        delete &out_table.;
    quit;

    %if %upcase(&split_mode.)=DERIVED %then %do;
        proc fedsql sessref=conn;
            create table casuser.&out_table. {options replace=true} as
            select 'TRAIN' as Split, *
            from &input_caslib..&input_table.
            where &byvar. >= &train_min_mes.
              and &byvar. <= &train_max_mes.
            union all
            select 'OOT' as Split, *
            from &input_caslib..&input_table.
            where &byvar. >= &oot_min_mes.
              and &byvar. <= &oot_max_mes.;
        quit;
    %end;
    %else %do;
        proc fedsql sessref=conn;
            create table casuser.&out_table. {options replace=true} as
            select *
            from &input_caslib..&input_table.
            where upcase(&split_var.) in ('TRAIN', 'OOT');
        quit;
    %end;

%mend _miss_prepare_scope_data;

%macro _miss_stage_detail_sections(detail_data=, vars=, out_prefix=,
    ds_list_var=, label_list_var=);

    %local _i _var _nobs _dsid _dsname;

    %let &ds_list_var.=;
    %let &label_list_var.=;

    %let _i=1;
    %let _var=%scan(%superq(vars), &_i., %str( ));
    %do %while(%length(%superq(_var)) > 0);
        %let _dsname=&out_prefix._%sysfunc(putn(&_i., z4.));

        data work.&_dsname.;
            set &detail_data.;
            where upcase(strip(Variable))="%upcase(&_var.)";
        run;

        %let _dsid=%sysfunc(open(work.&_dsname.));
        %if &_dsid. > 0 %then %do;
            %let _nobs=%sysfunc(attrn(&_dsid., nlobs));
            %let _dsid=%sysfunc(close(&_dsid.));
        %end;
        %else %let _nobs=0;

        %if %sysevalf(%superq(_nobs)=, boolean) %then %let _nobs=0;

        %if &_nobs. > 0 %then %do;
            %let &ds_list_var.=&&&ds_list_var. &_dsname.;
            %let &label_list_var.=&&&label_list_var. &_var.;
        %end;
        %else %do;
            proc datasets library=work nolist nowarn;
                delete &_dsname.;
            quit;
        %end;

        %let _i=%eval(&_i. + 1);
        %let _var=%scan(%superq(vars), &_i., %str( ));
    %end;

%mend _miss_stage_detail_sections;

%macro _miss_render_detail_sections(ds_list=, label_list=);

    %local _i _ds _label;

    %let _i=1;
    %let _ds=%scan(%superq(ds_list), &_i., %str( ));
    %let _label=%scan(%superq(label_list), &_i., %str( ));
    %do %while(%length(%superq(_ds)) > 0);
        proc print data=work.&_ds. label noobs;
            var Dummy_Value Type Total_N NMiss Pct_Miss;
            label Dummy_Value="&_label."
                  Total_N='Total';
            format Pct_Miss percent8.2;
        run;

        %let _i=%eval(&_i. + 1);
        %let _ds=%scan(%superq(ds_list), &_i., %str( ));
        %let _label=%scan(%superq(label_list), &_i., %str( ));
    %end;

%mend _miss_render_detail_sections;

%macro _miss_render_summary(summary_data=, threshold=);

    proc report data=work.&summary_data. nowd missing;
        columns Variable Type Total_Pct_Missing;
        define Variable / display;
        define Type / display;
        define Total_Pct_Missing / display format=percent8.2;

        compute Total_Pct_Missing;
            if Total_Pct_Missing >= &threshold. then
                call define(_col_, 'style', 'style={background=red}');
            else call define(_col_, 'style', 'style={background=white}');
        endcomp;
    run;

%mend _miss_render_summary;

%macro _miss_render_split(detail_ds_list_num=, detail_label_list_num=,
    detail_ds_list_cat=, detail_label_list_cat=, summary_data=,
    split_label=, threshold=);

    title "&split_label.: Analisis de Missings";
    title2 "Missing summarize (variable/cases)";
    %_miss_render_detail_sections(ds_list=&detail_ds_list_num.,
        label_list=&detail_label_list_num.);
    %_miss_render_detail_sections(ds_list=&detail_ds_list_cat.,
        label_list=&detail_label_list_cat.);

    title2 "Missing summarize (variables)";
    %_miss_render_summary(summary_data=&summary_data., threshold=&threshold.);

    title;
    title2;

%mend _miss_render_split;

%macro _missings_report(input_caslib=, input_table=, split_mode=DERIVED,
    split_var=Split, byvar=, train_min_mes=, train_max_mes=, oot_min_mes=,
    oot_max_mes=, vars_num=, vars_cat=, threshold=, report_path=,
    file_prefix=);

    %local _dir_rc;

    %put NOTE: [missings_report] Generando reportes...;
    %put NOTE: [missings_report] input=&input_caslib..&input_table.;
    %put NOTE: [missings_report] split_mode=&split_mode.;
    %put NOTE: [missings_report] report_path=&report_path.;
    %put NOTE: [missings_report] file_prefix=&file_prefix.;
    %put NOTE: [missings_report] threshold=&threshold.;

    %if %index(%upcase(&report_path.), METOD4.2) > 0 %then %do;
        %let _dir_rc=%sysfunc(dcreate(METOD4.2, &report_path./../));
    %end;

    %_miss_prepare_scope_data(input_caslib=&input_caslib.,
        input_table=&input_table., split_mode=&split_mode.,
        split_var=&split_var., byvar=&byvar., train_min_mes=&train_min_mes.,
        train_max_mes=&train_max_mes., oot_min_mes=&oot_min_mes.,
        oot_max_mes=&oot_max_mes., out_table=_miss_input);

    %_miss_compute(data=casuser._miss_input, split_var=Split,
        vars_num=&vars_num., vars_cat=&vars_cat., detail_table=_miss_detail,
        summary_table=_miss_summary, var_catalog_table=_miss_var_catalog,
        split_totals_table=_miss_split_totals);

    proc fedsql sessref=conn;
        create table casuser._miss_train_detail_rpt {options replace=true} as
        select Variable, Dummy_Value, Type, Total_N, NMiss, Pct_Miss
        from casuser._miss_detail
        where Split='TRAIN';

        create table casuser._miss_oot_detail_rpt {options replace=true} as
        select Variable, Dummy_Value, Type, Total_N, NMiss, Pct_Miss
        from casuser._miss_detail
        where Split='OOT';

        create table casuser._miss_train_summary_rpt {options replace=true} as
        select Variable, Type, Pct_Miss as Total_Pct_Missing
        from casuser._miss_summary
        where Split='TRAIN';

        create table casuser._miss_oot_summary_rpt {options replace=true} as
        select Variable, Type, Pct_Miss as Total_Pct_Missing
        from casuser._miss_summary
        where Split='OOT';
    quit;

    %_miss_sort_cas(table_name=_miss_train_detail_rpt,
        orderby=%str({"Variable", "Dummy_Value"}));
    %_miss_sort_cas(table_name=_miss_oot_detail_rpt,
        orderby=%str({"Variable", "Dummy_Value"}));
    %_miss_sort_cas(table_name=_miss_train_summary_rpt,
        orderby=%str({"Variable"}));
    %_miss_sort_cas(table_name=_miss_oot_summary_rpt,
        orderby=%str({"Variable"}));

    data work._miss_train_detail_rpt;
        set casuser._miss_train_detail_rpt;
    run;

    data work._miss_oot_detail_rpt;
        set casuser._miss_oot_detail_rpt;
    run;

    data work._miss_train_summary_rpt;
        set casuser._miss_train_summary_rpt;
    run;

    data work._miss_oot_summary_rpt;
        set casuser._miss_oot_summary_rpt;
    run;

    %local _train_num_ds _train_num_lbl _train_cat_ds _train_cat_lbl
        _oot_num_ds _oot_num_lbl _oot_cat_ds _oot_cat_lbl;

    %_miss_stage_detail_sections(detail_data=work._miss_train_detail_rpt,
        vars=&vars_num., out_prefix=_miss_trn_num,
        ds_list_var=_train_num_ds, label_list_var=_train_num_lbl);
    %_miss_stage_detail_sections(detail_data=work._miss_train_detail_rpt,
        vars=&vars_cat., out_prefix=_miss_trn_cat,
        ds_list_var=_train_cat_ds, label_list_var=_train_cat_lbl);
    %_miss_stage_detail_sections(detail_data=work._miss_oot_detail_rpt,
        vars=&vars_num., out_prefix=_miss_oot_num,
        ds_list_var=_oot_num_ds, label_list_var=_oot_num_lbl);
    %_miss_stage_detail_sections(detail_data=work._miss_oot_detail_rpt,
        vars=&vars_cat., out_prefix=_miss_oot_cat,
        ds_list_var=_oot_cat_ds, label_list_var=_oot_cat_lbl);

    ods html5 file="&report_path./&file_prefix..html"
        options(bitmap_mode="inline");
    %_miss_render_split(detail_ds_list_num=&_train_num_ds.,
        detail_label_list_num=&_train_num_lbl.,
        detail_ds_list_cat=&_train_cat_ds.,
        detail_label_list_cat=&_train_cat_lbl.,
        summary_data=_miss_train_summary_rpt, split_label=TRAIN,
        threshold=&threshold.);
    %_miss_render_split(detail_ds_list_num=&_oot_num_ds.,
        detail_label_list_num=&_oot_num_lbl.,
        detail_ds_list_cat=&_oot_cat_ds.,
        detail_label_list_cat=&_oot_cat_lbl.,
        summary_data=_miss_oot_summary_rpt, split_label=OOT,
        threshold=&threshold.);
    ods html5 close;

    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="TRAIN_Missings" sheet_interval="none"
        embedded_titles="yes");
    %_miss_render_split(detail_ds_list_num=&_train_num_ds.,
        detail_label_list_num=&_train_num_lbl.,
        detail_ds_list_cat=&_train_cat_ds.,
        detail_label_list_cat=&_train_cat_lbl.,
        summary_data=_miss_train_summary_rpt, split_label=TRAIN,
        threshold=&threshold.);

    ods excel options(sheet_name="OOT_Missings" sheet_interval="now"
        embedded_titles="yes");
    %_miss_render_split(detail_ds_list_num=&_oot_num_ds.,
        detail_label_list_num=&_oot_num_lbl.,
        detail_ds_list_cat=&_oot_cat_ds.,
        detail_label_list_cat=&_oot_cat_lbl.,
        summary_data=_miss_oot_summary_rpt, split_label=OOT,
        threshold=&threshold.);

    ods excel close;

    proc datasets library=casuser nolist nowarn;
        delete _miss_input _miss_detail _miss_summary _miss_var_catalog
            _miss_split_totals _miss_detail_raw _miss_summary_stage
            _miss_train_detail_rpt _miss_oot_detail_rpt
            _miss_train_summary_rpt _miss_oot_summary_rpt;
    quit;

    proc datasets library=work nolist nowarn;
        delete _miss_train_detail_rpt _miss_oot_detail_rpt
            _miss_train_summary_rpt _miss_oot_summary_rpt
            _miss_trn_num_: _miss_trn_cat_:
            _miss_oot_num_: _miss_oot_cat_:;
    quit;

    %put NOTE: [missings_report] HTML=> &report_path./&file_prefix..html;
    %put NOTE: [missings_report] Excel=> &report_path./&file_prefix..xlsx;

%mend _missings_report;
