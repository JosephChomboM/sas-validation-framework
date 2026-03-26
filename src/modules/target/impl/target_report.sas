/* =========================================================================
target_report.sas - Reporte consolidado TRAIN + OOT para Target (METOD2.1)
========================================================================= */

%macro _target_rows(data=, where=, outvar=_tgt_rows);
    %local _where_clause;
    %let _where_clause=1=1;
    %if %length(%superq(where)) > 0 %then %let _where_clause=&where.;

    proc sql noprint;
        select count(*) into :&outvar trimmed
        from &data.
        where &_where_clause.;
    quit;

    %if %sysevalf(%superq(&outvar)=, boolean) %then %let &outvar=0;
%mend _target_rows;

%macro _target_note(text=);
    ods text="^S={fontweight=bold color=gray} &text.";
%mend _target_note;

%macro _target_report_rel_diff(split=);
    %local _rows;
    %_target_rows(data=casuser._tgt_rel_diff, where=Split="&split.",
        outvar=_rows);
    %if &_rows.=0 %then %do;
        %_target_note(text=No hay datos de diferencia relativa para &split..);
        %return;
    %end;

    title "Diferencia Relativa - &split.";
    proc report data=casuser._tgt_rel_diff(where=(Split="&split.")) nowd
        missing;
        columns Split N_Months Window_Type Start_Label Start_Value End_Label
            End_Value Relative_Diff Note;
        define Split / display "Dataset";
        define N_Months / display "N Periodos";
        define Window_Type / display "Ventana";
        define Start_Label / display "Referencia Inicial" flow width=22;
        define Start_Value / display "Valor Inicial" format=percent8.4;
        define End_Label / display "Referencia Final" flow width=22;
        define End_Value / display "Valor Final" format=percent8.4;
        define Relative_Diff / display "Diferencia Relativa" format=percent8.2;
        define Note / display "Nota" flow width=42;
    run;
    title;
%mend _target_report_rel_diff;

%macro _target_plot_rd(data=casuser._tgt_rd_monthly, split=, file_prefix=);
    %local _rows _img;
    %_target_rows(data=&data., where=Split="&split.", outvar=_rows);
    %if &_rows.=0 %then %return;

    %let _img=&file_prefix._rd_%lowcase(&split.);

    ods graphics / imagename="&_img." imagefmt=jpeg;
    title "Evolutivo del Target - &split.";
    proc sgplot data=&data.(where=(Split="&split."));
        vline Periodo / response=RD markers
            markerattrs=(symbol=circlefilled color=black)
            lineattrs=(color=crimson);
        yaxis label="RD" min=0 max=1;
        xaxis label="Periodo" type=discrete;
    run;
    title;
    ods graphics / reset=all;
%mend _target_plot_rd;

%macro _target_report_rd(split=, file_prefix=);
    %local _rows;
    %_target_rows(data=casuser._tgt_rd_monthly, where=Split="&split.",
        outvar=_rows);
    %if &_rows.=0 %then %do;
        %_target_note(text=No hay RD mensual para &split..);
        %return;
    %end;

    %_target_report_rel_diff(split=&split.);

    title "RD Mensual - &split.";
    proc report data=casuser._tgt_rd_monthly(where=(Split="&split.")) nowd
        missing;
        columns Split Periodo N_Total N_Valid N_Default RD;
        define Split / display "Dataset";
        define Periodo / display "Periodo" format=6.;
        define N_Total / display "N Total";
        define N_Valid / display "N Target Valido";
        define N_Default / display "N Default";
        define RD / display "RD" format=percent8.4;
    run;
    title;

    %_target_plot_rd(data=casuser._tgt_rd_monthly, split=&split.,
        file_prefix=&file_prefix.);
%mend _target_report_rd;

%macro _target_report_materiality(split=);
    %local _rows;
    %_target_rows(data=casuser._tgt_materiality, where=Split="&split.",
        outvar=_rows);
    %if &_rows.=0 %then %do;
        %_target_note(text=No hay materialidad para &split..);
        %return;
    %end;

    title "Materialidad por Periodo y Target - &split.";
    proc report data=casuser._tgt_materiality(where=(Split="&split.")) nowd
        missing;
        columns Split Periodo Target_Value N_Cuentas;
        define Split / display "Dataset";
        define Periodo / display "Periodo" format=6.;
        define Target_Value / display "Target";
        define N_Cuentas / display "N Cuentas";
    run;
    title;
%mend _target_report_materiality;

%macro _target_plot_bands(data=casuser._tgt_bands, split=, yvar=RD,
    refvar=Global_Avg, lower=Lower_Band, upper=Upper_Band, axismin=Axis_Min,
    axismax=Axis_Max, ylabel=RD, line_color=blue, ref_color=red,
    file_suffix=band, title_txt=Evolutivo del Target, file_prefix=);

    %local _rows _axis_min _axis_max _ref_value _img;
    %let _axis_min=;
    %let _axis_max=;
    %let _ref_value=;

    %_target_rows(data=&data., where=Split="&split.", outvar=_rows);
    %if &_rows.=0 %then %return;

    %if %length(%superq(axismin)) > 0 and %length(%superq(axismax)) > 0 %then
        %do;
        proc sql noprint;
            select min(&axismin.), max(&axismax.)
            into :_axis_min trimmed, :_axis_max trimmed
            from &data.
            where Split="&split.";
        quit;
    %end;

    %if %length(%superq(refvar)) > 0 %then %do;
        proc sql noprint;
            select min(&refvar.) into :_ref_value trimmed
            from &data.
            where Split="&split.";
        quit;
    %end;

    %let _img=&file_prefix._&file_suffix._%lowcase(&split.);

    ods graphics / imagename="&_img." imagefmt=jpeg;
    title "&title_txt. - &split.";
    proc sgplot data=&data.(where=(Split="&split.")) subpixel noautolegend;
        band x=Periodo lower=&lower. upper=&upper. /
            fillattrs=(color=graydd) legendlabel="+/- 2 Desv. Estandar"
            name="band1";
        series x=Periodo y=&yvar. / markers
            lineattrs=(color=&line_color. thickness=2)
            legendlabel="RD" name="serie1";
        %if %length(%superq(_ref_value)) > 0 %then %do;
            refline &_ref_value. / lineattrs=(color=&ref_color. pattern=dash)
                legendlabel="Media TRAIN" name="line1";
        %end;
        %if %length(%superq(_axis_min)) > 0 and
            %length(%superq(_axis_max)) > 0 %then %do;
            yaxis min=&_axis_min. max=&_axis_max. label="&ylabel.";
        %end;
        %else %do;
            yaxis label="&ylabel.";
        %end;
        xaxis label="Periodo" type=discrete;
        %if %length(%superq(_ref_value)) > 0 %then %do;
            keylegend "serie1" "band1" "line1" /
                location=inside position=bottomright;
        %end;
        %else %do;
            keylegend "serie1" "band1" /
                location=inside position=bottomright;
        %end;
    run;
    title;
    ods graphics / reset=all;
%mend _target_plot_bands;

%macro _target_report_bands(split=, target_label=Target, file_prefix=);
    %local _rows;
    %_target_rows(data=casuser._tgt_bands, where=Split="&split.",
        outvar=_rows);
    %if &_rows.=0 %then %do;
        %_target_note(text=No hay bandas de target para &split..);
        %return;
    %end;

    title "Bandas del Target - &split.";
    proc report data=casuser._tgt_bands(where=(Split="&split.")) nowd missing;
        columns Split Periodo N_Total N_Valid N_Default RD Lower_Band
            Upper_Band Global_Avg;
        define Split / display "Dataset";
        define Periodo / display "Periodo" format=6.;
        define N_Total / display "N Total";
        define N_Valid / display "N Target Valido";
        define N_Default / display "N Default";
        define RD / display "Promedio del Target" format=percent8.4;
        define Lower_Band / display "Limite Inferior (- 2 Desv.)"
            format=percent8.4;
        define Upper_Band / display "Limite Superior (+ 2 Desv.)"
            format=percent8.4;
        define Global_Avg / display "Promedio Global TRAIN" format=percent8.4;
    run;
    title;

    %_target_plot_bands(data=casuser._tgt_bands, split=&split., yvar=RD,
        refvar=Global_Avg, lower=Lower_Band, upper=Upper_Band,
        axismin=Axis_Min, axismax=Axis_Max,
        ylabel=Promedio de &target_label., line_color=blue, ref_color=red,
        file_suffix=band, title_txt=Evolutivo del Target,
        file_prefix=&file_prefix.);
%mend _target_report_bands;

%macro _target_report_weight_avg(split=, file_prefix=);
    %local _rows;
    %if not %sysfunc(exist(casuser._tgt_weight_avg)) %then %return;
    %_target_rows(data=casuser._tgt_weight_avg, where=Split="&split.",
        outvar=_rows);
    %if &_rows.=0 %then %do;
        %_target_note(text=No hay target ponderado promedio para &split..);
        %return;
    %end;

    title "Target Ponderado por Monto - Promedio - &split.";
    proc report data=casuser._tgt_weight_avg(where=(Split="&split.")) nowd
        missing;
        columns Split Periodo N_Cuentas Total_Monto RD_Pond_Prom Lower_Band
            Upper_Band Global_Avg;
        define Split / display "Dataset";
        define Periodo / display "Periodo" format=6.;
        define N_Cuentas / display "N Cuentas";
        define Total_Monto / display "Monto Total" format=comma18.2;
        define RD_Pond_Prom / display "RD Ponderado Promedio"
            format=percent8.6;
        define Lower_Band / display "Limite Inferior (- 2 Desv.)"
            format=percent8.6;
        define Upper_Band / display "Limite Superior (+ 2 Desv.)"
            format=percent8.6;
        define Global_Avg / display "Media Ponderada Global TRAIN"
            format=percent8.6;
    run;
    title;

    %_target_plot_bands(data=casuser._tgt_weight_avg, split=&split.,
        yvar=RD_Pond_Prom, refvar=Global_Avg, lower=Lower_Band,
        upper=Upper_Band, axismin=Axis_Min, axismax=Axis_Max,
        ylabel=RD Pond. por Monto, line_color=darkblue, ref_color=red,
        file_suffix=wavg, title_txt=Target Ponderado por Monto,
        file_prefix=&file_prefix.);
%mend _target_report_weight_avg;

%macro _target_report_weight_sum(split=, file_prefix=);
    %local _rows;
    %if not %sysfunc(exist(casuser._tgt_weight_sum)) %then %return;
    %_target_rows(data=casuser._tgt_weight_sum, where=Split="&split.",
        outvar=_rows);
    %if &_rows.=0 %then %do;
        %_target_note(text=No hay target ponderado por suma para &split..);
        %return;
    %end;

    title "Target Ponderado por Suma de Monto - &split.";
    proc report data=casuser._tgt_weight_sum(where=(Split="&split.")) nowd
        missing;
        columns Split Periodo N_Cuentas Sum_Target_Pond Total_Monto Lower_Band
            Upper_Band Global_Sum;
        define Split / display "Dataset";
        define Periodo / display "Periodo" format=6.;
        define N_Cuentas / display "N Cuentas";
        define Sum_Target_Pond / display "RD Ponderado por Suma"
            format=comma18.2;
        define Total_Monto / display "Monto Total" format=comma18.2;
        define Lower_Band / display "Limite Inferior (- 2 Desv.)"
            format=comma18.2;
        define Upper_Band / display "Limite Superior (+ 2 Desv.)"
            format=comma18.2;
        define Global_Sum / display "Media de Sumas Global TRAIN"
            format=comma18.2;
    run;
    title;

    %_target_plot_bands(data=casuser._tgt_weight_sum, split=&split.,
        yvar=Sum_Target_Pond, refvar=Global_Sum, lower=Lower_Band,
        upper=Upper_Band, axismin=, axismax=,
        ylabel=RD Pond. por Suma de Monto, line_color=darkgreen,
        ref_color=red, file_suffix=wsum,
        title_txt=Target Ponderado por Suma de Monto,
        file_prefix=&file_prefix.);
%mend _target_report_weight_sum;

%macro _target_report_weight_ratio(split=, file_prefix=);
    %local _rows;
    %if not %sysfunc(exist(casuser._tgt_weight_ratio)) %then %return;
    %_target_rows(data=casuser._tgt_weight_ratio, where=Split="&split.",
        outvar=_rows);
    %if &_rows.=0 %then %do;
        %_target_note(text=No hay ratio RD/monto para &split..);
        %return;
    %end;

    title "Ratio RD Ponderado sobre Monto Total - &split.";
    proc report data=casuser._tgt_weight_ratio(where=(Split="&split.")) nowd
        missing;
        columns Split Periodo N_Cuentas Sum_Target_Pond Total_Monto
            Ratio_RD_Monto Lower_Band Upper_Band Global_Ratio;
        define Split / display "Dataset";
        define Periodo / display "Periodo" format=6.;
        define N_Cuentas / display "N Cuentas";
        define Sum_Target_Pond / display "RD Pond. Suma" format=comma18.2;
        define Total_Monto / display "Monto Total" format=comma18.2;
        define Ratio_RD_Monto / display "Ratio RD/Monto" format=percent8.6;
        define Lower_Band / display "Limite Inferior (- 2 Desv.)"
            format=percent8.6;
        define Upper_Band / display "Limite Superior (+ 2 Desv.)"
            format=percent8.6;
        define Global_Ratio / display "Media del Ratio Global TRAIN"
            format=percent8.6;
    run;
    title;

    %_target_plot_bands(data=casuser._tgt_weight_ratio, split=&split.,
        yvar=Ratio_RD_Monto, refvar=Global_Ratio, lower=Lower_Band,
        upper=Upper_Band, axismin=Axis_Min, axismax=Axis_Max,
        ylabel=Ratio RD/Monto Total, line_color=darkred, ref_color=blue,
        file_suffix=ratio, title_txt=Ratio RD Ponderado sobre Monto Total,
        file_prefix=&file_prefix.);
%mend _target_report_weight_ratio;

%macro _target_report(input_caslib=, train_table=, oot_table=, byvar=, target=,
    monto_var=, def_cld=, has_monto=0, report_path=, images_path=,
    file_prefix=);

    ods graphics on;
    ods listing gpath="&images_path.";
    ods html5 file="&report_path./&file_prefix..html"
        options(bitmap_mode="inline");
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="DESCRIBE" sheet_interval="none"
        embedded_titles="yes" frozen_headers="yes" autofilter="all");

    title "METOD2.1 - Target Consolidado TRAIN + OOT";
    proc report data=casuser._tgt_rd_monthly nowd missing;
        columns Split N_Total N_Valid N_Default;
        define Split / group "Dataset";
        define N_Total / analysis sum "N Total";
        define N_Valid / analysis sum "N Target Valido";
        define N_Default / analysis sum "N Default";
    run;
    title;

    %_target_report_rd(split=TRAIN, file_prefix=&file_prefix.);
    %_target_report_rd(split=OOT, file_prefix=&file_prefix.);

    ods excel options(sheet_name="MATERIALIDAD" sheet_interval="now"
        embedded_titles="yes" frozen_headers="yes");
    %_target_report_materiality(split=TRAIN);
    %_target_report_materiality(split=OOT);

    ods excel options(sheet_name="BANDAS" sheet_interval="now"
        embedded_titles="yes" frozen_headers="yes");
    %_target_report_bands(split=TRAIN, target_label=&target.,
        file_prefix=&file_prefix.);
    %_target_report_bands(split=OOT, target_label=&target.,
        file_prefix=&file_prefix.);

    %if &has_monto.=1 %then %do;
        ods excel options(sheet_name="WGT_PROM" sheet_interval="now"
            embedded_titles="yes" frozen_headers="yes");
        %_target_report_weight_avg(split=TRAIN, file_prefix=&file_prefix.);
        %_target_report_weight_avg(split=OOT, file_prefix=&file_prefix.);

        ods excel options(sheet_name="WGT_SUMA" sheet_interval="now"
            embedded_titles="yes" frozen_headers="yes");
        %_target_report_weight_sum(split=TRAIN, file_prefix=&file_prefix.);
        %_target_report_weight_sum(split=OOT, file_prefix=&file_prefix.);

        ods excel options(sheet_name="WGT_RATIO" sheet_interval="now"
            embedded_titles="yes" frozen_headers="yes");
        %_target_report_weight_ratio(split=TRAIN, file_prefix=&file_prefix.);
        %_target_report_weight_ratio(split=OOT, file_prefix=&file_prefix.);
    %end;

    ods excel close;
    ods html5 close;
    ods graphics / reset=all;
    ods graphics off;

%mend _target_report;

/* -------------------------------------------------------------------------
Override:
- Reporte combinado TRAIN + OOT
- Tablas unificadas
- Graficos unificados con group=Split
- Materialidad descriptiva directa desde casuser._tgt_input
------------------------------------------------------------------------- */

%macro _target_report_rel_diff();
    %local _rows;
    %if not %sysfunc(exist(casuser._tgt_rel_diff)) %then %return;
    %_target_rows(data=casuser._tgt_rel_diff, outvar=_rows);
    %if &_rows.=0 %then %return;

    title "Diferencia Relativa";
    proc report data=casuser._tgt_rel_diff nowd missing;
        columns Split N_Months Window_Type Start_Label Start_Value End_Label
            End_Value Relative_Diff Note;
        define Split / display "Dataset";
        define N_Months / display "N Periodos";
        define Window_Type / display "Ventana";
        define Start_Label / display "Referencia Inicial" flow width=22;
        define Start_Value / display "Valor Inicial" format=percent8.4;
        define End_Label / display "Referencia Final" flow width=22;
        define End_Value / display "Valor Final" format=percent8.4;
        define Relative_Diff / display "Diferencia Relativa" format=percent8.2;
        define Note / display "Nota" flow width=42;
    run;
    title;
%mend _target_report_rel_diff;

%macro _target_plot_rd(data=casuser._tgt_rd_monthly, yvar=RD, ylabel=RD,
    file_suffix=rd, title_txt=Evolutivo del Target, file_prefix=);

    %local _rows _img;
    %_target_rows(data=&data., outvar=_rows);
    %if &_rows.=0 %then %return;

    %let _img=&file_prefix._&file_suffix.;

    ods graphics / imagename="&_img." imagefmt=jpeg;
    title "&title_txt.";
    proc sgplot data=&data.;
        series x=Periodo y=&yvar. / group=Split markers lineattrs=(thickness=2);
        yaxis label="&ylabel.";
        xaxis label="Periodo" type=discrete;
    run;
    title;
    ods graphics / reset=all;
%mend _target_plot_rd;

%macro _target_report_rd(file_prefix=);
    %local _rows;
    %_target_rows(data=casuser._tgt_rd_monthly, outvar=_rows);
    %if &_rows.=0 %then %return;

    %_target_report_rel_diff();

    title "RD Mensual";
    proc report data=casuser._tgt_rd_monthly nowd missing;
        columns Split Periodo N_Total N_Valid N_Default RD;
        define Split / display "Dataset";
        define Periodo / display "Periodo" format=6.;
        define N_Total / display "N Total";
        define N_Valid / display "N Target Valido";
        define N_Default / display "N Default";
        define RD / display "RD" format=percent8.4;
    run;
    title;

    %_target_plot_rd(data=casuser._tgt_rd_monthly, yvar=RD, ylabel=RD,
        file_suffix=rd, title_txt=Evolutivo del Target,
        file_prefix=&file_prefix.);
%mend _target_report_rd;

%macro _target_report_materiality(byvar=, target=);
    %if not %sysfunc(exist(casuser._tgt_input)) %then %return;

    title "Materialidad por Periodo y Target";
    proc freqtab data=casuser._tgt_input;
        tables Split * &byvar. * &target. / norow nopercent nocum nocol;
    run;
    title;
%mend _target_report_materiality;

%macro _target_plot_bands(data=casuser._tgt_bands, yvar=RD,
    refvar=Global_Avg, lower=Lower_Band, upper=Upper_Band, axismin=Axis_Min,
    axismax=Axis_Max, ylabel=RD, ref_color=red, file_suffix=band,
    title_txt=Evolutivo del Target, file_prefix=);

    %local _rows _axis_min _axis_max _ref_value _img;
    %let _axis_min=;
    %let _axis_max=;
    %let _ref_value=;

    %_target_rows(data=&data., outvar=_rows);
    %if &_rows.=0 %then %return;

    %if %length(%superq(axismin)) > 0 and %length(%superq(axismax)) > 0 %then
        %do;
        proc sql noprint;
            select min(&axismin.), max(&axismax.)
            into :_axis_min trimmed, :_axis_max trimmed
            from &data.;
        quit;
    %end;

    %if %length(%superq(refvar)) > 0 %then %do;
        proc sql noprint;
            select min(&refvar.) into :_ref_value trimmed
            from &data.;
        quit;
    %end;

    %let _img=&file_prefix._&file_suffix.;

    ods graphics / imagename="&_img." imagefmt=jpeg;
    title "&title_txt.";
    proc sgplot data=&data. subpixel noautolegend;
        band x=Periodo lower=&lower. upper=&upper. /
            fillattrs=(color=graydd) transparency=0.45
            legendlabel="+/- 2 Desv. Estandar" name="band1";
        series x=Periodo y=&yvar. / group=Split markers name="serie1";
        %if %length(%superq(_ref_value)) > 0 %then %do;
            refline &_ref_value. / lineattrs=(color=&ref_color. pattern=dash)
                legendlabel="Media TRAIN" name="line1";
        %end;
        %if %length(%superq(_axis_min)) > 0 and
            %length(%superq(_axis_max)) > 0 %then %do;
            yaxis min=&_axis_min. max=&_axis_max. label="&ylabel.";
        %end;
        %else %do;
            yaxis label="&ylabel.";
        %end;
        xaxis label="Periodo" type=discrete;
        %if %length(%superq(_ref_value)) > 0 %then %do;
            keylegend "serie1" "band1" "line1" /
                location=inside position=bottomright;
        %end;
        %else %do;
            keylegend "serie1" "band1" /
                location=inside position=bottomright;
        %end;
    run;
    title;
    ods graphics / reset=all;
%mend _target_plot_bands;

%macro _target_report_bands(target_label=Target, file_prefix=);
    %local _rows;
    %_target_rows(data=casuser._tgt_bands, outvar=_rows);
    %if &_rows.=0 %then %return;

    title "Bandas del Target";
    proc report data=casuser._tgt_bands nowd missing;
        columns Split Periodo N_Total N_Valid N_Default RD Lower_Band
            Upper_Band Global_Avg;
        define Split / display "Dataset";
        define Periodo / display "Periodo" format=6.;
        define N_Total / display "N Total";
        define N_Valid / display "N Target Valido";
        define N_Default / display "N Default";
        define RD / display "Promedio del Target" format=percent8.4;
        define Lower_Band / display "Limite Inferior (- 2 Desv.)"
            format=percent8.4;
        define Upper_Band / display "Limite Superior (+ 2 Desv.)"
            format=percent8.4;
        define Global_Avg / display "Promedio Global TRAIN" format=percent8.4;
    run;
    title;

    %_target_plot_bands(data=casuser._tgt_bands, yvar=RD,
        refvar=Global_Avg, lower=Lower_Band, upper=Upper_Band,
        axismin=Axis_Min, axismax=Axis_Max,
        ylabel=Promedio de &target_label., ref_color=red,
        file_suffix=band, title_txt=Evolutivo del Target,
        file_prefix=&file_prefix.);
%mend _target_report_bands;

%macro _target_report_weight_avg(file_prefix=);
    %local _rows;
    %if not %sysfunc(exist(casuser._tgt_weight_avg)) %then %return;
    %_target_rows(data=casuser._tgt_weight_avg, outvar=_rows);
    %if &_rows.=0 %then %return;

    title "Target Ponderado por Monto - Promedio";
    proc report data=casuser._tgt_weight_avg nowd missing;
        columns Split Periodo N_Cuentas Total_Monto RD_Pond_Prom Lower_Band
            Upper_Band Global_Avg;
        define Split / display "Dataset";
        define Periodo / display "Periodo" format=6.;
        define N_Cuentas / display "N Cuentas";
        define Total_Monto / display "Monto Total" format=comma18.2;
        define RD_Pond_Prom / display "RD Ponderado Promedio"
            format=percent8.6;
        define Lower_Band / display "Limite Inferior (- 2 Desv.)"
            format=percent8.6;
        define Upper_Band / display "Limite Superior (+ 2 Desv.)"
            format=percent8.6;
        define Global_Avg / display "Media Ponderada Global TRAIN"
            format=percent8.6;
    run;
    title;

    %_target_plot_bands(data=casuser._tgt_weight_avg, yvar=RD_Pond_Prom,
        refvar=Global_Avg, lower=Lower_Band, upper=Upper_Band,
        axismin=Axis_Min, axismax=Axis_Max,
        ylabel=RD Pond. por Monto, ref_color=red,
        file_suffix=wavg, title_txt=Target Ponderado por Monto,
        file_prefix=&file_prefix.);
%mend _target_report_weight_avg;

%macro _target_report_weight_sum(file_prefix=);
    %local _rows;
    %if not %sysfunc(exist(casuser._tgt_weight_sum)) %then %return;
    %_target_rows(data=casuser._tgt_weight_sum, outvar=_rows);
    %if &_rows.=0 %then %return;

    title "Target Ponderado por Suma de Monto";
    proc report data=casuser._tgt_weight_sum nowd missing;
        columns Split Periodo N_Cuentas Sum_Target_Pond Total_Monto Lower_Band
            Upper_Band Global_Sum;
        define Split / display "Dataset";
        define Periodo / display "Periodo" format=6.;
        define N_Cuentas / display "N Cuentas";
        define Sum_Target_Pond / display "RD Ponderado por Suma"
            format=comma18.2;
        define Total_Monto / display "Monto Total" format=comma18.2;
        define Lower_Band / display "Limite Inferior (- 2 Desv.)"
            format=comma18.2;
        define Upper_Band / display "Limite Superior (+ 2 Desv.)"
            format=comma18.2;
        define Global_Sum / display "Media de Sumas Global TRAIN"
            format=comma18.2;
    run;
    title;

    %_target_plot_bands(data=casuser._tgt_weight_sum, yvar=Sum_Target_Pond,
        refvar=Global_Sum, lower=Lower_Band, upper=Upper_Band, axismin=,
        axismax=, ylabel=RD Pond. por Suma de Monto, ref_color=red,
        file_suffix=wsum, title_txt=Target Ponderado por Suma de Monto,
        file_prefix=&file_prefix.);
%mend _target_report_weight_sum;

%macro _target_report_weight_ratio(file_prefix=);
    %local _rows;
    %if not %sysfunc(exist(casuser._tgt_weight_ratio)) %then %return;
    %_target_rows(data=casuser._tgt_weight_ratio, outvar=_rows);
    %if &_rows.=0 %then %return;

    title "Ratio RD Ponderado sobre Monto Total";
    proc report data=casuser._tgt_weight_ratio nowd missing;
        columns Split Periodo N_Cuentas Sum_Target_Pond Total_Monto
            Ratio_RD_Monto Lower_Band Upper_Band Global_Ratio;
        define Split / display "Dataset";
        define Periodo / display "Periodo" format=6.;
        define N_Cuentas / display "N Cuentas";
        define Sum_Target_Pond / display "RD Pond. Suma" format=comma18.2;
        define Total_Monto / display "Monto Total" format=comma18.2;
        define Ratio_RD_Monto / display "Ratio RD/Monto" format=percent8.6;
        define Lower_Band / display "Limite Inferior (- 2 Desv.)"
            format=percent8.6;
        define Upper_Band / display "Limite Superior (+ 2 Desv.)"
            format=percent8.6;
        define Global_Ratio / display "Media del Ratio Global TRAIN"
            format=percent8.6;
    run;
    title;

    %_target_plot_bands(data=casuser._tgt_weight_ratio, yvar=Ratio_RD_Monto,
        refvar=Global_Ratio, lower=Lower_Band, upper=Upper_Band,
        axismin=Axis_Min, axismax=Axis_Max, ylabel=Ratio RD/Monto Total,
        ref_color=blue, file_suffix=ratio,
        title_txt=Ratio RD Ponderado sobre Monto Total,
        file_prefix=&file_prefix.);
%mend _target_report_weight_ratio;

%macro _target_report(input_caslib=, train_table=, oot_table=, byvar=, target=,
    monto_var=, def_cld=, has_monto=0, report_path=, images_path=,
    file_prefix=);

    ods graphics on;
    ods listing gpath="&images_path.";
    ods html5 file="&report_path./&file_prefix..html"
        options(bitmap_mode="inline");
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="DESCRIBE" sheet_interval="none"
        embedded_titles="yes" frozen_headers="yes" autofilter="all");

    title "METOD2.1 - Target Consolidado TRAIN + OOT";
    proc report data=casuser._tgt_rd_monthly nowd missing;
        columns Split N_Total N_Valid N_Default;
        define Split / group "Dataset";
        define N_Total / analysis sum "N Total";
        define N_Valid / analysis sum "N Target Valido";
        define N_Default / analysis sum "N Default";
    run;
    title;

    %_target_report_rd(file_prefix=&file_prefix.);

    ods excel options(sheet_name="MATERIALIDAD" sheet_interval="now"
        embedded_titles="yes" frozen_headers="yes");
    %_target_report_materiality(byvar=&byvar., target=&target.);

    ods excel options(sheet_name="BANDAS" sheet_interval="now"
        embedded_titles="yes" frozen_headers="yes");
    %_target_report_bands(target_label=&target., file_prefix=&file_prefix.);

    %if &has_monto.=1 %then %do;
        ods excel options(sheet_name="WGT_PROM" sheet_interval="now"
            embedded_titles="yes" frozen_headers="yes");
        %_target_report_weight_avg(file_prefix=&file_prefix.);

        ods excel options(sheet_name="WGT_SUMA" sheet_interval="now"
            embedded_titles="yes" frozen_headers="yes");
        %_target_report_weight_sum(file_prefix=&file_prefix.);

        ods excel options(sheet_name="WGT_RATIO" sheet_interval="now"
            embedded_titles="yes" frozen_headers="yes");
        %_target_report_weight_ratio(file_prefix=&file_prefix.);
    %end;

    ods excel close;
    ods html5 close;
    ods graphics / reset=all;
    ods graphics off;

%mend _target_report;
