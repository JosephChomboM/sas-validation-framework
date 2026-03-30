/* =========================================================================
missings_report.sas - Reporte consolidado de missings

Genera:
- <report_path>/<prefix>.html
- <report_path>/<prefix>.xlsx

Metodologia funcional basada en missings_legacy.sas.
========================================================================= */
%macro _miss_prepare_scope_data(input_caslib=, input_table=, split_mode=DERIVED,
    split_var=Split, byvar=, train_min_mes=, train_max_mes=, oot_min_mes=,
    oot_max_mes=, out_table=_miss_input);

    proc cas;
        session conn;
        table.dropTable / caslib="casuser" name="&out_table." quiet=true;
        table.dropTable / caslib="casuser" name="_miss_input_stage" quiet=true;
    quit;

    %if %upcase(&split_mode.)=DERIVED %then %do;
        proc fedsql sessref=conn;
            create table casuser.&out_table. {options replace=true} as
            select cast('TRAIN' as varchar(16)) as Split, a.*
            from &input_caslib..&input_table. a
            where a.&byvar. >= &train_min_mes.
              and a.&byvar. <= &train_max_mes.;
        quit;

        proc fedsql sessref=conn;
            create table casuser._miss_input_stage {options replace=true} as
            select cast('OOT' as varchar(16)) as Split, a.*
            from &input_caslib..&input_table. a
            where a.&byvar. >= &oot_min_mes.
              and a.&byvar. <= &oot_max_mes.;
        quit;

        proc cas;
            session conn;
            table.append /
                source={caslib="casuser", name="_miss_input_stage"},
                target={caslib="casuser", name="&out_table."};
            table.dropTable / caslib="casuser" name="_miss_input_stage"
                quiet=true;
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

%macro _miss_print_detail(detail_data=, vars=);

    %local _i _var;

    %let _i=1;
    %let _var=%scan(%superq(vars), &_i., %str( ));
    %do %while(%length(%superq(_var)) > 0);
        proc print data=&detail_data.(where=(Variable="&_var.")) noobs label;
            var Dummy_Value Type Total NMiss Pct_Miss;
            label Dummy_Value="&_var."
                  Type="type"
                  Total="total"
                  NMiss="nmiss"
                  Pct_Miss="pct_miss";
            format Pct_Miss percent8.2;
        run;

        %let _i=%eval(&_i. + 1);
        %let _var=%scan(%superq(vars), &_i., %str( ));
    %end;

%mend _miss_print_detail;

%macro _miss_render_split(detail_data=, summary_data=, split_label=,
    vars_num=, vars_cat=);

    title "&split_label.: Analisis de Missings";
    title2 "Missing summarize (variable/cases)";
    %_miss_print_detail(detail_data=&detail_data., vars=&vars_num.);
    %_miss_print_detail(detail_data=&detail_data., vars=&vars_cat.);

    title;
    title2 "Missing summarize (variables)";
    proc print data=&summary_data. style(column)={backgroundcolor=MissSignif.}
        noobs;
        var Variable Type Total_Pct_Missing;
        format Total_Pct_Missing percent8.2;
    run;
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

    %if %index(%upcase(&report_path.), METOD4.2) > 0 %then %do;
        %let _dir_rc=%sysfunc(dcreate(METOD4.2, &report_path./../));
    %end;

    %_miss_prepare_scope_data(input_caslib=&input_caslib.,
        input_table=&input_table., split_mode=&split_mode.,
        split_var=&split_var., byvar=&byvar., train_min_mes=&train_min_mes.,
        train_max_mes=&train_max_mes., oot_min_mes=&oot_min_mes.,
        oot_max_mes=&oot_max_mes., out_table=_miss_input);

    proc fedsql sessref=conn;
        create table casuser._miss_train_input {options replace=true} as
        select *
        from casuser._miss_input
        where upcase(Split)='TRAIN';

        create table casuser._miss_oot_input {options replace=true} as
        select *
        from casuser._miss_input
        where upcase(Split)='OOT';
    quit;

    %_miss_compute_split(data=casuser._miss_train_input, vars_num=&vars_num.,
        vars_cat=&vars_cat., detail_table=_miss_train_detail,
        summary_table=_miss_train_summary);

    %_miss_compute_split(data=casuser._miss_oot_input, vars_num=&vars_num.,
        vars_cat=&vars_cat., detail_table=_miss_oot_detail,
        summary_table=_miss_oot_summary);

    data work._miss_train_detail;
        set casuser._miss_train_detail;
    run;

    data work._miss_oot_detail;
        set casuser._miss_oot_detail;
    run;

    data work._miss_train_summary;
        set casuser._miss_train_summary;
    run;

    data work._miss_oot_summary;
        set casuser._miss_oot_summary;
    run;

    proc format;
        value MissSignif
            -0.0-<&threshold.='white'
            &threshold.-<1='red';
    run;

    ods html5 file="&report_path./&file_prefix..html"
        options(bitmap_mode="inline");
    %_miss_render_split(detail_data=work._miss_train_detail,
        summary_data=work._miss_train_summary, split_label=TRAIN,
        vars_num=&vars_num., vars_cat=&vars_cat.);
    %_miss_render_split(detail_data=work._miss_oot_detail,
        summary_data=work._miss_oot_summary, split_label=OOT,
        vars_num=&vars_num., vars_cat=&vars_cat.);
    ods html5 close;

    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="TRAIN_Missings" sheet_interval="none"
        embedded_titles="yes");
    %_miss_render_split(detail_data=work._miss_train_detail,
        summary_data=work._miss_train_summary, split_label=TRAIN,
        vars_num=&vars_num., vars_cat=&vars_cat.);

    ods excel options(sheet_name="OOT_Missings" sheet_interval="now"
        embedded_titles="yes");
    %_miss_render_split(detail_data=work._miss_oot_detail,
        summary_data=work._miss_oot_summary, split_label=OOT,
        vars_num=&vars_num., vars_cat=&vars_cat.);
    ods excel close;

    proc datasets library=casuser nolist nowarn;
        delete _miss_input _miss_input_stage _miss_train_input _miss_oot_input
            _miss_train_detail _miss_train_summary _miss_oot_detail
            _miss_oot_summary _miss_det_stage _miss_sum_stage;
    quit;

    proc datasets library=work nolist nowarn;
        delete _miss_train_detail _miss_train_summary _miss_oot_detail
            _miss_oot_summary;
    quit;

    %put NOTE: [missings_report] HTML=> &report_path./&file_prefix..html;
    %put NOTE: [missings_report] Excel=> &report_path./&file_prefix..xlsx;

%mend _missings_report;
