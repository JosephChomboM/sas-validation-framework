/* =========================================================================
precision_report.sas - Reporte HTML + Excel + JPEG para Precision

El modulo recibe un scope unificado, deriva TRAIN/OOT en una sola tabla CAS
con columna split y renderiza ambos en un unico reporte.
========================================================================= */

%macro _prec_prepare_scope_data(input_caslib=, input_table=, target=,
    score_var=, monto_var=, segvar=, byvar=, def_cld=, train_min_mes=,
    train_max_mes=, oot_min_mes=, oot_max_mes=, out_table=_prec_input,
    split_var=split);

    %local _keep_vars _keep_vars_sql;
    %let _keep_vars=&target. &score_var. &byvar.;
    %if %length(%superq(monto_var)) > 0 %then
        %let _keep_vars=&_keep_vars. &monto_var.;
    %if %length(%superq(segvar)) > 0 %then
        %let _keep_vars=&_keep_vars. &segvar.;
    %let _keep_vars=%sysfunc(compbl(&_keep_vars.));
    %let _keep_vars_sql=%sysfunc(tranwrd(&_keep_vars.,%str( ),%str(, )));

    proc cas;
        session conn;
        table.dropTable / caslib="casuser" name="&out_table." quiet=true;
        table.dropTable / caslib="casuser" name="_prec_input_stage"
            quiet=true;
    quit;

    proc fedsql sessref=conn;
        create table casuser.&out_table. {options replace=true} as
        select 'TRAIN' as &split_var., &_keep_vars_sql.
        from &input_caslib..&input_table.
        where &byvar. >= &train_min_mes.
          and &byvar. <= &train_max_mes.
          and &byvar. <= &def_cld.;
    quit;

    proc fedsql sessref=conn;
        create table casuser._prec_input_stage {options replace=true} as
        select 'OOT' as &split_var., &_keep_vars_sql.
        from &input_caslib..&input_table.
        where &byvar. >= &oot_min_mes.
          and &byvar. <= &oot_max_mes.
          and &byvar. <= &def_cld.;
    quit;

    proc cas;
        session conn;
        table.append /
            source={caslib="casuser", name="_prec_input_stage"},
            target={caslib="casuser", name="&out_table."};
        table.dropTable / caslib="casuser" name="_prec_input_stage"
            quiet=true;
    quit;

%mend _prec_prepare_scope_data;

%macro _prec_render_total_block(total_table=, plot_table=, title_txt=,
    chart_title=, yaxis_label=, sheet_name=, file_prefix=, image_suffix=);

    %_prec_sort_cas(table_name=&total_table., orderby=%str({"split"}));
    %_prec_sort_cas(table_name=&plot_table.,
        orderby=%str({"split", "metrica"}));

    title "&title_txt.";
    proc print data=casuser.&total_table. noobs label;
        label split="Split"
            target_mean="Promedio Target"
            score_mean="Promedio Score"
            gap="Gap Score-Target"
            abs_gap="Gap Absoluto";
    run;
    title;

    ods graphics / imagename="&file_prefix._&image_suffix." imagefmt=jpeg;
    title "&chart_title.";
    proc sgplot data=casuser.&plot_table.;
        vbarparm category=split response=valor / group=metrica
            groupdisplay=cluster datalabel;
        yaxis label="&yaxis_label.";
        xaxis label="Split";
    run;
    title;

%mend _prec_render_total_block;

%macro _prec_render_seg_block(seg_table=, plot_table=, segvar=, title_txt=,
    chart_title=, yaxis_label=, sheet_name=, file_prefix=, image_suffix=);

    %_prec_sort_cas(table_name=&seg_table.,
        orderby=%str({"split", "&segvar."}));
    %_prec_sort_cas(table_name=&plot_table.,
        orderby=%str({"split", "&segvar.", "metrica"}));

    title "&title_txt.";
    proc print data=casuser.&seg_table. noobs label;
        label split="Split"
            target_mean="Promedio Target"
            score_mean="Promedio Score"
            gap="Gap Score-Target"
            abs_gap="Gap Absoluto";
    run;
    title;

    ods graphics / imagename="&file_prefix._&image_suffix." imagefmt=jpeg;
    title "&chart_title.";
    proc sgpanel data=casuser.&plot_table.;
        panelby split / columns=2 novarname;
        vbarparm category=&segvar. response=valor / group=metrica
            groupdisplay=cluster datalabel;
        colaxis label="Segmento";
        rowaxis label="&yaxis_label.";
    run;
    title;

%mend _prec_render_seg_block;

%macro _precision_report(input_caslib=, input_table=, target=, score_var=,
    monto_var=, segvar=, byvar=, def_cld=0, train_min_mes=, train_max_mes=,
    oot_min_mes=, oot_max_mes=, ponderado=1, report_path=, images_path=,
    file_prefix=);

    %local _has_weight _has_seg;
    %let _has_weight=0;
    %let _has_seg=0;

    %if %length(%superq(monto_var)) > 0 and &ponderado.=1 %then
        %let _has_weight=1;
    %if %length(%superq(segvar)) > 0 %then
        %let _has_seg=1;

    %_prec_prepare_scope_data(input_caslib=&input_caslib.,
        input_table=&input_table., target=&target., score_var=&score_var.,
        monto_var=&monto_var., segvar=&segvar., byvar=&byvar.,
        def_cld=&def_cld., train_min_mes=&train_min_mes.,
        train_max_mes=&train_max_mes., oot_min_mes=&oot_min_mes.,
        oot_max_mes=&oot_max_mes., out_table=_prec_input, split_var=split);

    %_precision_compute(data=casuser._prec_input, alias=_prec,
        target=&target., score_var=&score_var., monto_var=&monto_var.,
        segvar=&segvar., has_weight=&_has_weight., has_seg=&_has_seg.,
        split_var=split, out_caslib=casuser);

    ods graphics on;
    ods listing gpath="&images_path.";
    ods html5 file="&report_path./&file_prefix..html"
        options(bitmap_mode="inline");
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="Total_Precision" sheet_interval="none"
        embedded_titles="yes");

    %_prec_render_total_block(total_table=_prec_total,
        plot_table=_prec_plot_total, title_txt=Precision del Modelo,
        chart_title=Target vs Score por Split, yaxis_label=Promedio,
        sheet_name=Total_Precision, file_prefix=&file_prefix.,
        image_suffix=total);

    %if &_has_seg.=1 %then %do;
        ods excel options(sheet_name="Segmento_Precision" sheet_interval="now"
            embedded_titles="yes");
        %_prec_render_seg_block(seg_table=_prec_seg, plot_table=_prec_plot_seg,
            segvar=&segvar., title_txt=Precision por Segmento,
            chart_title=Target vs Score por Segmento y Split,
            yaxis_label=Promedio, sheet_name=Segmento_Precision,
            file_prefix=&file_prefix., image_suffix=seg);
    %end;

    %if &_has_weight.=1 %then %do;
        ods excel options(sheet_name="Weighted_Precision" sheet_interval="now"
            embedded_titles="yes");
        %_prec_render_total_block(total_table=_prec_total_w,
            plot_table=_prec_plot_total_w,
            title_txt=Precision Ponderada por Monto - &monto_var.,
            chart_title=Target vs Score Ponderado por Split,
            yaxis_label=Promedio Ponderado, sheet_name=Weighted_Precision,
            file_prefix=&file_prefix., image_suffix=totalw);

        %if &_has_seg.=1 %then %do;
            ods excel options(sheet_name="Segmento_Weighted"
                sheet_interval="now" embedded_titles="yes");
            %_prec_render_seg_block(seg_table=_prec_seg_w,
                plot_table=_prec_plot_seg_w, segvar=&segvar.,
                title_txt=Precision Ponderada por Segmento,
                chart_title=Target vs Score Ponderado por Segmento y Split,
                yaxis_label=Promedio Ponderado,
                sheet_name=Segmento_Weighted, file_prefix=&file_prefix.,
                image_suffix=segw);
        %end;
    %end;

    ods excel close;
    ods html5 close;
    ods graphics / reset=all;
    ods graphics off;

    proc datasets library=casuser nolist nowarn;
        delete _prec_:;
    quit;

%mend _precision_report;
